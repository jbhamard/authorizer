package com.qonto.streams.movements

import com.qonto.streams.Authorizer.{BankAccount, BankAccountBalance, BankAccountMovement, BankAccountMovementAuthorization, MovementWithBankAccount, accountsStoreName}
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
    val movement = movementWithAccount.movement
    val bankAccount = movementWithAccount.bankAccount

    if (bankAccount == null) {
      return KeyValue.pair[String, BankAccountMovementAuthorization](iban, BankAccountMovementAuthorization(
        movement.movementId,
        movement.amountCents,
        0,
        iban,
        false,
        "account_not_found"
      ))
    }

    var currentBalance: Long = 0

    if (currentAccountBalanceState != null) {
      currentBalance = currentAccountBalanceState.balance
    }

    if (bankAccount.closed) {
      return KeyValue.pair[String, BankAccountMovementAuthorization](iban, BankAccountMovementAuthorization(
        movement.movementId,
        movement.amountCents,
        currentBalance,
        iban,
        false,
        "account_closed"
      ))
    }

    movement.direction match {
      case "credit" =>
        return updateStore(handleCredit(movement, bankAccount, currentBalance))
      case "debit" =>
        return updateStore(handleDebit(movement, bankAccount, currentBalance))
    }
  }

  override def close(): Unit = {}

  def handleCredit(movement: BankAccountMovement, bankAccount: BankAccount, currentBalance: Long): BankAccountMovementAuthorization = {
    if (bankAccount.creditBlocked) {
      return BankAccountMovementAuthorization(
        movement.movementId,
        movement.amountCents,
        currentBalance,
        movement.iban,
        false,
        "account_is_credit_blocked"
      )
    } else {
      return BankAccountMovementAuthorization(
        movement.movementId,
        movement.amountCents,
        currentBalance + movement.amountCents,
        movement.iban,
        true,
        null
      )
    }
  }

  def handleDebit(movement: BankAccountMovement, bankAccount: BankAccount, currentBalance: Long): BankAccountMovementAuthorization = {
    if (bankAccount.debitBlocked) {
      return BankAccountMovementAuthorization(
        movement.movementId,
        movement.amountCents,
        currentBalance,
        movement.iban,
        false,
        "account_is_debit_blocked"
      )
    }

    var balance: Long = 0
    var authorized: Boolean = false
    var declinedReason: String = null

    val projectedBalance = currentBalance - movement.amountCents

    if (projectedBalance < 0) {
      balance = currentBalance
      authorized = false
      declinedReason = "insufficient_funds"
    } else {
      balance = projectedBalance
      authorized = true
    }

    return BankAccountMovementAuthorization(
      movement.movementId,
      movement.amountCents,
      balance,
      movement.iban,
      authorized,
      declinedReason
    )
  }

  def updateStore(res: BankAccountMovementAuthorization): KeyValue[String, BankAccountMovementAuthorization] = {
    if (res.authorized) {
      bankAccountsBalanceStore.put(res.iban, BankAccountBalance(res.iban, res.balanceCents))
    }

    return KeyValue.pair[String, BankAccountMovementAuthorization](res.iban, res)
  }
}

class MovementTransformer extends TransformerSupplier[String, MovementWithBankAccount, KeyValue[String, BankAccountMovementAuthorization]] {
  override def get(): Transformer[String, MovementWithBankAccount, KeyValue[String, BankAccountMovementAuthorization]] = new MovementHandler
}