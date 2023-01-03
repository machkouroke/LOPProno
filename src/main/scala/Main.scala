package org.lop


import utilities.Loader
import constant.FileType
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.lop.utilities.Transformer.Transformer


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
          .getData()

        matchs.printSchema()
    }
}