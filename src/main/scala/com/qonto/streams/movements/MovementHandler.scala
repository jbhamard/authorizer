package com.qonto.streams.movements

import com.qonto.streams.KafkaConstants
import com.qonto.streams.domains.Domains
import com.qonto.streams.domains.Domains._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class MovementHandler(accountStoreName: String) extends Transformer[String, MovementWithBankAccount, KeyValue[String, Domains.BankAccountMovementOperation]] {

  private var bankAccountsBalanceStore: KeyValueStore[String, BankAccountBalance] = _

  override def init(processorContext: ProcessorContext): Unit = bankAccountsBalanceStore = processorContext.getStateStore(accountStoreName)

  override def transform(iban: String, movementWithAccount: MovementWithBankAccount): KeyValue[String, Domains.BankAccountMovementOperation] = {
    val balance: Long = Option(bankAccountsBalanceStore.get(iban)).map(_.balance).getOrElse(0)
    val movement: BankAccountMovement = movementWithAccount.movement
    val bankAccount: Option[BankAccount] = Option(movementWithAccount.bankAccount)

    (bankAccount.map(_.closed), movement.direction, bankAccount) match {
      case (None, _, _) =>
        KeyValue.pair[String, Domains.BankAccountMovementOperation](iban, BankAccountMovementOperation(
          movement.movementId,
          movement.amountCents,
          balance,
          iban,
          "account_not_found",
          false
        ))
      case (Some(true), _, _) =>
          KeyValue.pair[String, Domains.BankAccountMovementOperation](iban, BankAccountMovementOperation(
          movement.movementId,
          movement.amountCents,
          balance,
          iban,
          "account_closed",
          false
        ))
      case (Some(false), "credit", Some(account)) => updateStore(handleCredit(movement, account, balance))
      case (Some(false), "debit", Some(account)) => updateStore(handleDebit(movement, account, balance))
      case (_, _, _) => throw new Exception("Invalid movement direction on an opened account")
    }
  }

  override def close(): Unit = {}

  private def handleCredit(movement: BankAccountMovement, bankAccount: BankAccount, currentBalance: Long): Either[BankAccountMovementOperation, BankAccountMovementOperation] = {
    bankAccount.creditBlocked match {
      case true => Left(BankAccountMovementOperation(movement.movementId, movement.amountCents, currentBalance, movement.iban, "account_is_credit_blocked", false))
      case false => Right(BankAccountMovementOperation(movement.movementId, movement.amountCents, currentBalance + movement.amountCents, movement.iban, null, true))
    }
  }

  private def handleDebit(movement: BankAccountMovement, bankAccount: BankAccount, currentBalance: Long): Either[BankAccountMovementOperation, BankAccountMovementOperation] = {
    (bankAccount.debitBlocked, currentBalance - movement.amountCents) match {
      case (true, _) =>
        Left(BankAccountMovementOperation(movement.movementId, movement.amountCents, currentBalance, movement.iban, "account_is_debit_blocked", false))
      case (false, projectedBalance) if projectedBalance < 0 =>
        Left(BankAccountMovementOperation(movement.movementId, movement.amountCents, currentBalance , movement.iban, "insufficient_funds", false))
      case (false, projectedBalance) =>
        Right(BankAccountMovementOperation(movement.movementId, movement.amountCents, projectedBalance, movement.iban, null, true))
    }
  }

  private def updateStore(res: Either[BankAccountMovementOperation, BankAccountMovementOperation
  ]): KeyValue[String, Domains.BankAccountMovementOperation] = {
    res match {
      case Right(authorized) =>
        bankAccountsBalanceStore.put(authorized.iban, BankAccountBalance(authorized.iban, authorized.balanceCents))
        KeyValue.pair[String, Domains.BankAccountMovementOperation](authorized.iban, authorized)
      case Left(declined) =>
        KeyValue.pair[String, Domains.BankAccountMovementOperation](declined.iban, declined)
    }
  }
}

class MovementAuthorizer extends TransformerSupplier[String, MovementWithBankAccount, KeyValue[String, Domains.BankAccountMovementOperation]] {
  override def get(): Transformer[String, MovementWithBankAccount, KeyValue[String, Domains.BankAccountMovementOperation]] = new MovementHandler(KafkaConstants.accountsStoreName)
}