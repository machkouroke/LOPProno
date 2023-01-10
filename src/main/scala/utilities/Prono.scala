package org.lop
package utilities

import utilities.Models.PronoModel

import org.apache.spark.sql.{DataFrame, SparkSession}

object Prono {
    def fitModel(pronoVersion: String,
                 matchs: DataFrame,
                 spark: SparkSession
                ): Unit = {
        val model = new PronoModel(pronoVersion)
        model.fit(matchs, "FTR")
        model.save(spark)
    }

    def makeProno(homeTeam: String,
                  awayTeam: String,
                  matchsData: DataFrame,
                  pronoVersion: String,
                  spark: SparkSession
                 ): List[Map[String, Float]] = {
        val duels: List[(String, String)] = List(
            Tuple2(
                homeTeam,
                awayTeam
            )
        )
        val model = PronoModel.load(
            pronoVersion,
            spark
        )
        duels.map(
            duel => {
                val df = Filler.fillRow(
                    matchsData,
                    duel,
                    spark
                )
                model.predict(df)
            }
        )

    }
}
