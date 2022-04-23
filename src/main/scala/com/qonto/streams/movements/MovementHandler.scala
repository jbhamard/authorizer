package com.qonto.streams.movements

import com.qonto.streams.Authorizer.{BankAccountBalance, BankAccountMovementAuthorization, MovementWithBankAccount, accountsStoreName}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class MovementHandler extends Transformer[String, MovementWithBankAccount, KeyValue[String, BankAccountMovementAuthorization]] {

  private var bankAccountsBalanceStore: KeyValueStore[String, BankAccountBalance] = _

  override def init(processorContext: ProcessorContext): Unit = {
    bankAccountsBalanceStore = processorContext.getStateStore(accountsStoreName)
  }

  override def transform(iban: String, movementWithAccount: MovementWithBankAccount): KeyValue[String, BankAccountMovementAuthorization] = {
    val currentAccountBalanceState = bankAccountsBalanceStore.get(iban)
    var balance: Long = 0
    var authorized: Boolean = false
    var declinedReason: String = null

    val movement = movementWithAccount.movement
    val bankAccount = movementWithAccount.bankAccount

    if (bankAccount == null) {
      return KeyValue.pair[String, BankAccountMovementAuthorization](iban, BankAccountMovementAuthorization(
        movement.movementId,
        movement.amountCents,
        balance,
        iban,
        authorized,
        "account_not_found"
      ))
    }

    var currentBalance: Long = 0
    var projectedBalance: Long = 0

    if (currentAccountBalanceState != null) {
      currentBalance = currentAccountBalanceState.balance
    }

    movement.direction match {
      case "credit" =>
        projectedBalance = currentBalance + movement.amountCents
      case "debit" =>
        projectedBalance = currentBalance - movement.amountCents
    }

    if (projectedBalance < 0) {
      balance = currentBalance
      authorized = false
      declinedReason = "insufficient_funds"
    } else {
      balance = projectedBalance
      authorized = true
    }

    bankAccountsBalanceStore.put(iban, BankAccountBalance(iban, balance))

    val res: BankAccountMovementAuthorization = BankAccountMovementAuthorization(
      movement.movementId,
      movement.amountCents,
      balance,
      iban,
      authorized,
      declinedReason
    )

    KeyValue.pair[String, BankAccountMovementAuthorization](iban, res)
  }

  override def close(): Unit = {}
}

class MovementTransformer extends TransformerSupplier[String, MovementWithBankAccount, KeyValue[String, BankAccountMovementAuthorization]] {
  override def get(): Transformer[String, MovementWithBankAccount, KeyValue[String, BankAccountMovementAuthorization]] = new MovementHandler
}