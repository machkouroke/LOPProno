ThisBuild / version := "0.0.1"

ThisBuild / scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "LOPPronostic",
    idePackagePrefix := Some("org.lop")
  )

// Add of Apache Spark

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.0"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1"
