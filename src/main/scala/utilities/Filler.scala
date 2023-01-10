package org.lop
package utilities

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Filler {
    def fillRow(matchs: DataFrame, duel: (String, String), spark: SparkSession): DataFrame = {
        var row = Row.fromSeq(Seq.fill(matchs.schema.size)(null))
        row = Row.fromSeq(
            row
              .toSeq
              .updated(matchs.columns.indexOf("HomeTeam"), duel._1)
              .updated(matchs.columns.indexOf("AwayTeam"), duel._2)
        )
        matchs
          .drop("HomeTeam", "AwayTeam", "FTR")
          .columns
          .foreach(
              col => {
                  row = Row.fromSeq(
                      row
                        .toSeq
                        .updated(
                            matchs.columns.indexOf(col),
                            BestValue.check(duel._1, duel._2, col, matchs)
                        )
                  )
              }
          )
        spark
          .createDataFrame(spark.sparkContext.parallelize(row :: Nil), matchs.schema)
          .drop("FTR")
    }
}
