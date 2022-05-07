package com.qonto.streams.domains

import com.qonto.streams.serde.JsonSerde

object Domains {
  case class BankAccount(iban: String, creditBlocked: Boolean, debitBlocked: Boolean, closed: Boolean)
  case class BankAccountMovement(movementId: String, amountCents: Long, iban: String, direction: String)
  case class MovementWithBankAccount(movement: BankAccountMovement, bankAccount: BankAccount)
  case class BankAccountMovementAuthorization(
                                               movementId: String,
                                               amountCents: Long,
                                               balanceCents: Long,
                                               iban: String,
                                               authorized: Boolean,
                                               declinedReason: String
                                             )
  case class BankAccountBalance(iban: String, balance: Long)

  object BankAccountSerde {
    implicit val bankAccountSerde: JsonSerde[BankAccount] = new JsonSerde[BankAccount]
    implicit val bankAccountMovementsSerde: JsonSerde[BankAccountMovement] = new JsonSerde[BankAccountMovement]
    implicit val bankAccountMovementAuthorizationsSerde: JsonSerde[BankAccountMovementAuthorization] = new JsonSerde[BankAccountMovementAuthorization]
    implicit val bankAccountBalanceSerde: JsonSerde[BankAccountBalance] = new JsonSerde[BankAccountBalance]
  }
}

