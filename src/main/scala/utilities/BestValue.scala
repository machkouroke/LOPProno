package org.lop
package utilities

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, desc}

object BestValue {
    /**
     * Retroune la valeur la plus réprésentatif pour un un match donné dans la colonne précisée
     *
     * @param homeTeam Nom de l'équipe à domicile
     * @param awayTeam Nom de l'équipe à l'extérieur
     * @param colName  Nom de la colonne à analyser
     * @param data     DataFrame contenant les données
     * @return La valeur la plus représentative
     */
    def check(homeTeam: String, awayTeam: String, colName: String, data: DataFrame, debug: Boolean = false): Any = {
        val colType = data.schema(colName).dataType.typeName
        if (debug) println(s"colType: $colType, colName: $colName")
        val matchData: DataFrame = data
          .where(s"HomeTeam = '$homeTeam' AND AwayTeam = '$awayTeam'")
        if (List("integer", "string").contains(colType)) {
            val answer = matchData
              .groupBy(colName)
              .count()
              .orderBy(desc("count"))
              .first()
              .get(0)
            if (debug) println(s"**Resultat: $answer")
            if (colType == "integer") answer.asInstanceOf[Int] else answer.asInstanceOf[String]

        } else {
            matchData
              .agg(avg(col(colName)))
              .first()
              .getDouble(0)
        }
    }
}
