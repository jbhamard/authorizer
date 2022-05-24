package com.qonto.streams

import com.qonto.streams.domains.Domains
import com.qonto.streams.domains.Domains._
import com.qonto.streams.movements.MovementAuthorizer
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.util.Properties

object KafkaConstants {
  val bankAccountTableTopic: String = "bank-accounts"
  val movementsTopic = "movements"
  val accountsStoreName = "bank-accounts-balance-store"
  val movementAuthorizations = "movements-authorizations"
}

object Authorizer extends App {

  import com.qonto.streams.domains.Domains.BankAccountSerde._
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



  val builder = new StreamsBuilder()

  val bankAccountsTable: KTable[String, BankAccount] = builder.table[String, BankAccount](KafkaConstants.bankAccountTableTopic)

  val bankAccountMovements: KStream[String, BankAccountMovement] =
    builder.stream[String, BankAccountMovement](KafkaConstants.movementsTopic)

  val movementsWithBankAccount: KStream[String, MovementWithBankAccount] =
    bankAccountMovements.leftJoin[BankAccount, MovementWithBankAccount](bankAccountsTable) { (BankAccountMovement, BankAccount) =>
      MovementWithBankAccount(BankAccountMovement, BankAccount)
    }

  val bankAccountBalanceStore = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore(KafkaConstants.accountsStoreName),
    stringSerde,
    bankAccountBalanceSerde
  ).withCachingEnabled()

  builder.addStateStore(bankAccountBalanceStore)

  val movementAuthorizationStream: KStream[String, Domains.BankAccountMovementOperation] = movementsWithBankAccount
    .transform(new MovementAuthorizer, KafkaConstants.accountsStoreName)

  movementAuthorizationStream.peek((k,v) =>
    println(v)
  ).to(KafkaConstants.movementAuthorizations)

  val built = builder.build()
  println(built.describe())

  val streams: KafkaStreams = new KafkaStreams(built, config)

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