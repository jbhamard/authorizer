# authorizer

## Overview

This app is a kafka streams processor which handles bank_account movements of funds in order to update the bank_account balance.

If the targeted bank_account balance is insufficient, the `movement` can be declined (`movements-authorizations.authorized = false`)

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

The processor publishes authorization results in the `movements-authorizations` tropic : 

```json
{
"key":"iban", //String,
"value": {
  "movementId": String,
  "amountCents": Long,
  "balanceCents": Long,
  "iban": String,
  "authorized": Boolean
}
```

## Build and run

### Install dependencies

Run `docker-compose up` to pull and install dependencies :

- kafka
- zookeeper

### Kafka topics

Run bash into the kafka container : 

`docker-compose exec kafka bash`

Then, in the container, create the required topics :

```bash

/bin/kafka-topics --create --topic bank-account-movements --zookeeper zookeeper --partitions 1 --replication-factor 1
/bin/kafka-topics --create --topic movements-authorizations --zookeeper zookeeper --partitions 1 --replication-factor 1

```

## Restart the app

`docker-compose restart authorize`

The app should now be running.

## Create kafka producers and consumers

In order to use the app, we need to create a producer to `bank-account-movements` and a consumer of `movements-authorizations` :

In your kafka container, run :

`/bin/kafka-console-producer --broker-list localhost:9092 --topic bank-account-movements --property parse.key=true --property key.separator=,`

`/bin/kafka-console-consumer --topic movements-authorizations --from-beginning  --bootstrap-server kafka:9092  --property print.key=true`
