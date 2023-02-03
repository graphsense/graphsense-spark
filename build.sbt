ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "info.graphsense"
ThisBuild / version      := "1.3.0"

lazy val root = (project in file(".")).
  settings(
    name := "graphsense-ethereum-transformation",
    fork := true,
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    scalacOptions ++= List(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint:_",
      "-Ywarn-adapted-args",
      "-Ywarn-dead-code",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-unused-import",
      "-Ywarn-value-discard"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "com.github.mrpowers" % "spark-fast-tests_2.12" % "1.0.0" % Test,
      "org.rogach" %% "scallop" % "4.1.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.2.3" % Provided,
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0" % Provided,
      "joda-time" % "joda-time" % "2.10.10" % Provided,
      "org.web3j" % "core" % "4.8.7" % Provided,
      "org.web3j" % "abi" % "4.8.7" % Provided),
  )
