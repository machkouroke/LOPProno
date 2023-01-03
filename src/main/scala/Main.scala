package org.lop


import utilities.{Joiner, Loader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("LOPPronostic").getOrCreate()
        val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        val datasets: Seq[DataFrame] = Loader.listDatasets(
            spark,
            fs,
            Loader.listFiles(fs.listFiles(new Path("/data/sports-data"), true))
        )

        val joined: DataFrame = Joiner.join(datasets)
        println(s"La taille du datasets est ${joined.count()}")

    }
}