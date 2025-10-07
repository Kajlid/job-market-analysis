import Dependencies._

ThisBuild / scalaVersion     := "2.13.16"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "job-market-analysis",
    libraryDependencies ++= Seq(
      Dependencies.munit % Test,
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql"  % "3.5.1"
    )
  )

fork := true

javaOptions ++= Seq(
  "-Xmx4G", // Set 4GB heap size (adjust as needed)
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"       // this tells the JVM to open the sun.nio.ch package to Spark at runtime (avoid error).
)

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
