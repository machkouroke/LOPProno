ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "LOPPronostic",
    idePackagePrefix := Some("org.lop")
  )

// Add of Apache Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"