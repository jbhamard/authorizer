# Authorizer

## Overview

This app is a kafka streams processor which handles bank_account movements of funds in order to update the bank_account balance.

If the targeted bank_account balance is insufficient, the `movement` can be declined (`movements-authorizations.authorized = false`)

The `bankAccount` KTable is built from the `bank-account` topic (see topic creation below) :

```json
{
"key":"iban", //String,
"value": {
    "iban": String,
    "creditBlocked": Boolean,
    "debitBlocked": Boolean,
    "closed": Boolean 
}
```

The `movements` kafka messages are consumed from the `bank-account-movements` :

```json
{
"key":"iban", //String,
"value": {
    "movementId": String,
    "amountCents": Long,
    "direction": String,
    "iban": String 
}
```

The processor publishes authorization results in the `movements-authorizations` topic :

```json
{
  "key": "iban",
  //String,
  "value": {
    "movementId": String,
    "amountCents": Long,
    "balanceCents": Long,
    "iban": String,
    "authorized": Boolean,
    "declinedReason": String
  }
```

## Build and run

### Install dependencies

Run `docker-compose up` to pull and install dependencies :

- kafka
- zookeeper

### Create the kafka topics

Run bash into the kafka container : 

`docker-compose exec kafka bash`

Then, in the container, create the required topics :

```bash

/bin/kafka-topics --create --topic bank-accounts --zookeeper zookeeper --partitions 1 --replication-factor 1 --config "cleanup.policy=compact" 
/bin/kafka-topics --create --topic movements --zookeeper zookeeper --partitions 1 --replication-factor 1
/bin/kafka-topics --create --topic movements-authorizations --zookeeper zookeeper --partitions 1 --replication-factor 1

```

## Restart the app

`docker-compose restart authorizer`

The app should now be running.

## Create kafka producers and consumers

In order to use the app, we need to create a producer to `movements` and `bank-accounts` and a consumer of `movements-authorizations` :

In your kafka container, run the producers :

```
# this starts the producer and produces a movement on iban1

/bin/kafka-console-producer --broker-list localhost:9092 --topic movements --property parse.key=true --property key.separator=,
>iban1,{"movementId":"mvt_id","amountCents":100,"direction":"credit","iban":"iban1"}
```

```
# this starts the producer and produces a bankAccount event on iban1

/bin/kafka-console-producer --broker-list localhost:9092 --topic bank-accounts --property parse.key=true --property key.separator=,
>iban1,{"iban":"iban1","creditBlocked":false,"debitBlocked":false,"closed":false}
```

Finally, start the output topic consumer : 

`/bin/kafka-console-consumer --topic movements-authorizations --from-beginning  --bootstrap-server kafka:9092  --property print.key=true`

