package com.qonto.streams.domains

import com.qonto.streams.serde.JsonSerde

object Domains {
  final case class BankAccount(iban: String, creditBlocked: Boolean, debitBlocked: Boolean, closed: Boolean)

  final case class BankAccountMovement(movementId: String, amountCents: Long, iban: String, direction: String)

  final case class MovementWithBankAccount(movement: BankAccountMovement, bankAccount: BankAccount)



  case class BankAccountMovementOperation(movementId: String,
                                           amountCents: Long,
                                           balanceCents: Long,
                                           iban: String,
                                           declinedReason: String,
                                           authorized: Boolean)


  final case class BankAccountBalance(iban: String, balance: Long)

  object BankAccountSerde {
    implicit val bankAccountSerde: JsonSerde[BankAccount] = new JsonSerde[BankAccount]
    implicit val bankAccountMovementsSerde: JsonSerde[BankAccountMovement] = new JsonSerde[BankAccountMovement]
    implicit val bankAccountMovementAuthorizationsSerde: JsonSerde[Domains.BankAccountMovementOperation] = new JsonSerde[Domains.BankAccountMovementOperation]
    implicit val bankAccountBalanceSerde: JsonSerde[BankAccountBalance] = new JsonSerde[BankAccountBalance]
  }


}

