name := "akka streams"

version := "0.1-SNAPSHOT"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.6.10"

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-stream" % akkaVersion,
	"ch.qos.logback" % "logback-classic" % "1.2.3",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
)

javacOptions += "-Xlint:deprecation"

scalacOptions += "-deprecation"

