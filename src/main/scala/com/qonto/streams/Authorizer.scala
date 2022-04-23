package com.qonto.streams

import com.qonto.streams.movements.MovementTransformer
import com.qonto.streams.serde.JsonSerde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.util.Properties

object Authorizer extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._


  val config: Properties = {
    val p = new Properties()

    // This APPLICATION_ID_CONFIG gives its name to the consumer group
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "authorizer")

    val bootstrapServers = if (args.length > 0) args(0) else "kafka:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  case class BankAccount(iban: String, creditBlocked: Boolean, debitBlocked: Boolean, closed: Boolean)
  implicit val bankAccountSerde = new JsonSerde[BankAccount]

  case class BankAccountMovement(movementId: String, amountCents: Long, iban: String, direction: String)
  implicit val bankAccountMovementsSerde = new JsonSerde[BankAccountMovement]

  case class  MovementWithBankAccount(movement: BankAccountMovement, bankAccount: BankAccount)

  case class BankAccountMovementAuthorization(
    movementId: String,
    amountCents: Long,
    balanceCents: Long,
    iban: String,
    authorized: Boolean,
    declinedReason: String
  )
  implicit val bankAccountMovementAuthorizationsSerde = new JsonSerde[BankAccountMovementAuthorization]

  case class BankAccountBalance(iban: String, balance: Long)
  implicit val bankAccountBalanceSerde = new JsonSerde[BankAccountBalance]

  val builder = new StreamsBuilder()

  val bankAccountsTable: KTable[String, BankAccount] = builder.table[String, BankAccount]("bank-account")

  val bankAccountMovements: KStream[String, BankAccountMovement] =
    builder.stream[String, BankAccountMovement]("bank-account-movements")

  val movementsWithBankAccount: KStream[String, MovementWithBankAccount] =
    bankAccountMovements.leftJoin[BankAccount, MovementWithBankAccount](bankAccountsTable) {(BankAccountMovement, BankAccount) =>
      MovementWithBankAccount(BankAccountMovement ,BankAccount)
    }

  val accountsStoreName = "bank-accounts-balance-store"
  val bankAccountBalanceStore = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore(accountsStoreName),
    stringSerde,
    bankAccountBalanceSerde
  ).withCachingEnabled()

  builder.addStateStore(bankAccountBalanceStore)

  val movementAuthorizationStream: KStream[String, BankAccountMovementAuthorization] = movementsWithBankAccount
    .peek((_, mvt) => println(mvt))
    .transform(new MovementTransformer, accountsStoreName)

  movementAuthorizationStream.to("movements-authorizations")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  // Always (and unconditionally) clean local state prior to starting the processing topology.
  // We opt for this unconditional call here because this will make it easier for you to play around with the example
  // when resetting the application for doing a re-run (via the Application Reset Tool,
  // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
  //
  // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
  // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
  // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
  // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
  // See `ApplicationResetExample.java` for a production-like example.
  streams.cleanUp()

  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}
