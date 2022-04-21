package com.qonto.streams.movements

import com.qonto.streams.Authorizer.{BankAccount, BankAccountMovement, BankAccountMovementAuthorization, accountsStoreName}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore


class MovementHandler extends Transformer[String, BankAccountMovement, KeyValue[String, BankAccountMovementAuthorization]] {

  private var bankAccountsStore: KeyValueStore[String, BankAccount] = _

  override def init(processorContext: ProcessorContext): Unit = {
    bankAccountsStore = processorContext.getStateStore(accountsStoreName)
  }

  override def transform(iban: String, movement: BankAccountMovement): KeyValue[String, BankAccountMovementAuthorization] = {
    val currentAccountState = bankAccountsStore.get(iban)
    var balance: Long = 0
    var authorized: Boolean = false

    if (currentAccountState == null) {
      if (movement.direction == "credit") {
        balance = movement.amountCents
        authorized = true
      }
    } else {
      var projectedBalance: Long = 0

      movement.direction match {
        case "credit" =>
          projectedBalance = currentAccountState.balance + movement.amountCents
        case "debit" =>
          projectedBalance = currentAccountState.balance - movement.amountCents
      }

      if (projectedBalance < 0) {
        authorized = false
        balance = currentAccountState.balance
      } else {
        balance = projectedBalance
        authorized = true
      }
    }

    bankAccountsStore.put(iban, BankAccount(iban, balance))

    val res: BankAccountMovementAuthorization = BankAccountMovementAuthorization(
      movement.movementId,
      movement.amountCents,
      balance,
      iban,
      authorized
    )

    KeyValue.pair[String, BankAccountMovementAuthorization](iban, res)
  }

  override def close(): Unit = {}
}

class MovementTransformer extends TransformerSupplier[String, BankAccountMovement, KeyValue[String, BankAccountMovementAuthorization]] {
  override def get(): Transformer[String, BankAccountMovement, KeyValue[String, BankAccountMovementAuthorization]] = new MovementHandler
}