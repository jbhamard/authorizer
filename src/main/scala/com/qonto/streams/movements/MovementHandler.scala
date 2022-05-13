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
    val currentAccountBalanceState: BankAccountBalance = bankAccountsBalanceStore.get(iban)
    val movement: BankAccountMovement = movementWithAccount.movement
    val bankAccount: BankAccount = movementWithAccount.bankAccount

    (bankAccount.closed, movement.direction) match {
      case (true, _) =>
        KeyValue.pair[String, Domains.BankAccountMovementOperation](iban, DeclinedBankAccountMovement(
          movement.movementId,
          movement.amountCents,
          currentAccountBalanceState.balance,
          iban,
          "account_closed"
        ))
      case (false, "credit") => updateStore(handleCredit(movement, bankAccount, currentAccountBalanceState.balance))
      case (false, "debit") => updateStore(handleDebit(movement, bankAccount, currentAccountBalanceState.balance))
      case (_, _) => throw new Exception("Invalid movement direction on an opened account")
    }
  }

  override def close(): Unit = {}

  private def handleCredit(movement: BankAccountMovement, bankAccount: BankAccount, currentBalance: Long): Either[DeclinedBankAccountMovement, AuthorizedBankAccountMovement] = {
    bankAccount.creditBlocked match {
      case true => Left(DeclinedBankAccountMovement(movement.movementId, movement.amountCents, currentBalance, movement.iban, "account_is_credit_blocked"))
      case false => Right(AuthorizedBankAccountMovement(movement.movementId, movement.amountCents, currentBalance + movement.amountCents, movement.iban))
    }
  }

  private def handleDebit(movement: BankAccountMovement, bankAccount: BankAccount, currentBalance: Long): Either[DeclinedBankAccountMovement, AuthorizedBankAccountMovement] = {
    (bankAccount.debitBlocked, currentBalance - movement.amountCents) match {
      case (true, _) =>
        Left(DeclinedBankAccountMovement(movement.movementId, movement.amountCents, currentBalance, movement.iban, "account_is_debit_blocked"))
      case (false, projectedBalance) if projectedBalance < 0 =>
        Left(DeclinedBankAccountMovement(movement.movementId, movement.amountCents, currentBalance + movement.amountCents, movement.iban, "insufficient_funds"))
      case (false, _) =>
        Right(AuthorizedBankAccountMovement(movement.movementId, movement.amountCents, currentBalance, movement.iban))
    }
  }

  private def updateStore(res: Either[DeclinedBankAccountMovement, AuthorizedBankAccountMovement]): KeyValue[String, Domains.BankAccountMovementOperation] = {
    res match {
      case Right(authorized) =>
        bankAccountsBalanceStore.put(authorized.iban, BankAccountBalance(authorized.iban, authorized.balanceCents))
        KeyValue.pair[String, Domains.BankAccountMovementOperation](authorized.iban, authorized)
      case Left(declined) =>
        KeyValue.pair[String, Domains.BankAccountMovementOperation](declined.iban, declined)
    }
  }
}

class MovementTransformer extends TransformerSupplier[String, MovementWithBankAccount, KeyValue[String, Domains.BankAccountMovementOperation]] {
  override def get(): Transformer[String, MovementWithBankAccount, KeyValue[String, Domains.BankAccountMovementOperation]] = new MovementHandler(KafkaConstants.accountsStoreName)
}