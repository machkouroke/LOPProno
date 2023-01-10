package org.lop
package utilities

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import constant.FileType
import utilities.Transformer.Transformer

import java.net.URI
import scala.annotation.tailrec

object Loader {
    def saveData(path: String, spark: SparkSession, fs: FileSystem): Unit = {


        /* Chargement des données */
        val matchs: DataFrame = Loader
          .listDatasets(
              spark,
              fs,
              Loader.listFiles(fs.listFiles(new Path("/data/sports-data"), true))
          ).join()

        /* Transformation des données */
        val data = new Transformer(matchs)
          .renameColumns()
          .typeTransform(
              FileType.colsInteger,
              FileType.colsFloat
          )
          .dropNa()
          .data.
          write
          .parquet(path)
    }

    def listFiles(iter: RemoteIterator[LocatedFileStatus]): Seq[URI] = {
        @tailrec
        def go(iter: RemoteIterator[LocatedFileStatus], acc: List[URI]): List[URI] = {
            if (iter.hasNext) {
                val uri = iter.next.getPath.toUri
                go(iter, uri :: acc)
            } else {
                acc
            }
        }

        go(iter, List.empty[java.net.URI])
    }

    def listDatasets(spark: SparkSession, fs: FileSystem, listFile: Seq[URI]): Joiner = {
        new Joiner(listFiles(fs.listFiles(new Path("/data/sports-data"), true)).
          filter(_.toString.endsWith(".csv")).
          map(fileStatus => spark.read.option("header", "true").
            format("csv").
            load(fileStatus.getPath)))
    }

}
