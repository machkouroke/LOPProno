ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "LOPPronostic",
    idePackagePrefix := Some("org.lop")
  )

// Add of Apache Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.0"