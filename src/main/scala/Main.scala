package org.lop
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LOPPronostic").getOrCreate()

    // Chargez le fichier CSV en utilisant la méthode "read" de la session Spark
    val df = spark.read.format("csv").option("header", "true").load("chemin/vers/mon_fichier.csv")
  }
}