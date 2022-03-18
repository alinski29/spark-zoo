import sbt.Keys.javaOptions

lazy val sparkVersion = "3.2.1"

ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.github.alinski"

lazy val setup = (project in file(".")).settings(
    name := "spark-zoo",
    Test / javaOptions ++= Seq(
      "-Xms512M",
      "-Xmx2048M",
      "-XX:MaxPermSize=2048M",
      "-XX:+CMSClassUnloadingEnabled"
    ),
    Test / fork := true,
    cleanFiles ++= Seq(file("metastore_db"), file("spark-warehouse"))
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.delta" %% "delta-core" % "1.1.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
)

publishMavenStyle := true
