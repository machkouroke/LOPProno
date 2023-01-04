package org.lop


import constant.FileType
import utilities.Loader
import utilities.Transformer.Transformer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Main {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("LOPPronostic").getOrCreate()
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
          .dropNa()
          .oneHotEncoder()
          .data
        matchs.printSchema()
    }
}

