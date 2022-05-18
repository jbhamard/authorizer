scalaVersion := "2.13.5"

mainClass in (Compile, run) := Some("com.qonto.streams.Authorizer")

libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.1"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0"
// JSON
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.11"