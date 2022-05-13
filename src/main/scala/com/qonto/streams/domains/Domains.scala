package com.qonto.streams.domains

import com.qonto.streams.serde.JsonSerde

object Domains {
  final case class BankAccount(iban: String, creditBlocked: Boolean, debitBlocked: Boolean, closed: Boolean)

  final case class BankAccountMovement(movementId: String, amountCents: Long, iban: String, direction: String)

  final case class MovementWithBankAccount(movement: BankAccountMovement, bankAccount: BankAccount)

  abstract class BankAccountMovementOperation {
    val movementId: String
    val amountCents: Long
    val balanceCents: Long
    val iban: String
    val declinedReason: Option[String] = None
    val authorized: Boolean
  }

  case class AuthorizedBankAccountMovement(movementId: String,
                                           amountCents: Long,
                                           balanceCents: Long,
                                           iban: String) extends BankAccountMovementOperation {
    override val declinedReason: Option[String] = None
    override val authorized: Boolean = true
  }

  case class DeclinedBankAccountMovement(movementId: String,
                                         amountCents: Long,
                                         balanceCents: Long,
                                         iban: String, reason: String) extends BankAccountMovementOperation {
    override val declinedReason: Option[String] = Some(reason)
    override val authorized: Boolean = false
  }

  final case class BankAccountBalance(iban: String, balance: Long)

  object BankAccountSerde {
    implicit val bankAccountSerde: JsonSerde[BankAccount] = new JsonSerde[BankAccount]
    implicit val bankAccountMovementsSerde: JsonSerde[BankAccountMovement] = new JsonSerde[BankAccountMovement]
    implicit val bankAccountMovementAuthorizationsSerde: JsonSerde[Domains.BankAccountMovementOperation] = new JsonSerde[Domains.BankAccountMovementOperation]
    implicit val bankAccountBalanceSerde: JsonSerde[BankAccountBalance] = new JsonSerde[BankAccountBalance]
  }


}

