// Project
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"   // Spark 3.5.x uses Scala 2.12

lazy val sparkV   = "3.5.1"
lazy val hadoopV  = "3.3.4"            // Align with Spark 3.5.x (Hadoop 3.3.x)
lazy val luceneV  = "9.9.2"
lazy val pdfboxV  = "2.0.30"           // v2.x needed for MemoryUsageSetting

// Keep Spark's JAXB expectations happy
ThisBuild / dependencyOverrides +=
  "jakarta.xml.bind" % "jakarta.xml.bind-api" % "2.3.3"

// Strip any legacy javax JAXB pulled by transitive deps
ThisBuild / libraryDependencies ~= { _.map(_.exclude("javax.xml.bind", "jaxb-api")) }

// ---- root module ----
lazy val root = (project in file("."))
  .settings(
    name := "rag-pipeline",

    // Default main so sbt run doesn't prompt
    Compile / run / mainClass := Some("ragPipeline.DriverMain"),

    // Fork for Spark runs
    Compile / run / fork := true,
    Compile / run / connectInput := true,

    // Dependencies
    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-core" % sparkV,
      "org.apache.spark" %% "spark-sql"  % sparkV,

      // Hadoop client APIs on classpath when running via sbt
      "org.apache.hadoop" % "hadoop-client-api"     % hadoopV,
      "org.apache.hadoop" % "hadoop-client-runtime" % hadoopV % Runtime,

      // Lucene (vector search + analyzers)
      "org.apache.lucene" % "lucene-core"            % luceneV,
      "org.apache.lucene" % "lucene-analysis-common" % luceneV,

      // PDF parsing (v2.x for MemoryUsageSetting/PDFTextStripper)
      "org.apache.pdfbox" % "pdfbox" % pdfboxV,

      // CSV utilities
      "org.apache.commons" % "commons-csv" % "1.10.0",

      // Circe JSON
      "io.circe" %% "circe-core"    % "0.14.7",
      "io.circe" %% "circe-generic" % "0.14.7",
      "io.circe" %% "circe-parser"  % "0.14.7",

      // HTTP client
      "com.softwaremill.sttp.client3" %% "core"           % "3.9.7",
      "com.softwaremill.sttp.client3" %% "okhttp-backend" % "3.9.7",
      "com.softwaremill.sttp.client3" %% "circe"          % "3.9.7",

      // Config
      "com.typesafe" % "config" % "1.4.3",

      // Logging
      "org.slf4j"      % "slf4j-api"       % "1.7.36",
      "ch.qos.logback" % "logback-classic" % "1.5.6" % Runtime,

      // Test
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    )
  )
