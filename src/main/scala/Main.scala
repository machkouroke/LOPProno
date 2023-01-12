package org.lop


import utilities.{Loader, Prono}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import scala.util.Properties

object Main {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("LOPPronostic")
          .set("spark.logging.level", "INFO")
          .set("spark.broadcast.blockSize", "90m")


        val spark: SparkSession = SparkSession.builder().config(conf)
          .getOrCreate()
        val numExecutors = spark.sparkContext.defaultParallelism
        val scalaVersion = Properties.versionString
        val sparkVersion = spark.version

        val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val path = "/data/sports-data/all_data.parquet"
        if (!fs.exists(new Path(path))) {
            println("Not found")
            Loader.saveData(path, spark, fs)
        }
        println("Data saved")
        val matchs = spark.read.parquet(path)
        val pronoVersion = "Pronostiqueur 5.0"
        println("Jar version:5.0")
        println(s"Spark version: $sparkVersion")
        println(s"Scala version: $scalaVersion")
        println(s"Nombre d'exécuteurs par défaut : $numExecutors")
        println(s"Estimated size of the dataFrame match = ${SizeEstimator.estimate(matchs) / 1000000} mb")
        println("Data loaded")
        /* Prédiction */
                println(Prono.makeProno(
                    "Aston Villa",
                    "Wigan",
                    matchs,
                    pronoVersion,
                    spark
                ))
        println("Fin du programme")

        println(matchs.count())
//        Prono.fitModel(
//            pronoVersion,
//            matchs,
//            spark
//        )
    }
}

