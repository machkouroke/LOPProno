package org.lop


import constant.FileType
import utilities.Loader
import utilities.Transformer.Transformer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("LOPPronostic").set("spark.logging.level", "INFO")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        /* Chargement des données */
        var matchs: DataFrame = Loader
          .listDatasets(
              spark,
              fs,
              Loader.listFiles(fs.listFiles(new Path("/data/sports-data"), true))
          ).join()

        /* Transformation des données */
        matchs = new Transformer(matchs)
          .typeTransform(FileType.colsInteger, FileType.colsFloat)
          .renameColumns()
          .dropNa()
          .data
        matchs.printSchema()
        matchs.show(2)
//        PronoModel.fit(matchs, "FTRindexed")
    }
}

