package org.lop
package utilities.Transformer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}


class Transformer(val data: DataFrame) {
    /**
     * Transforme les colonnes de type Integer en type Integer et les colonnes de type Float en type Float
     *
     * @param colsInteger : List[String] - Liste des colonnes de type Integer
     * @param colsFloat   : List[String] Colonnes de type Float
     * @return Transformer - Retourne l'objet Transformer
     */
    def typeTransform(colsInteger: List[String], colsFloat: List[String]): Transformer = {


        val cols: List[Column] = colsInteger
          .map(x => col(s"`$x`")
            .cast("int")) ++
          colsFloat
            .map(x => col(s"`$x`").cast("double")) ++
          colsString(colsInteger, colsFloat)
            .map(x => col(s"`$x`")
              .cast("string"))

        new Transformer(data.select(cols: _*))
    }


    def dropNa(): Transformer = {
        new Transformer(data.na.drop())
    }

    /**
     *
     * @return
     */


    /**
     * Retourne la liste des colonnes de type String
     *
     * @param colsInteger : List[String] - Liste des colonnes de type Integer
     * @param colsFloat  : List[String] - Liste des colonnes de type Float
     * @return
     */
    private def colsString(colsInteger: List[String], colsFloat: List[String]): List[String] = {
        data
          .columns
          .diff(colsInteger ++ colsFloat)
          .filter(col => !col.equals("Date"))
          .toList
    }

    def renameColumns(): Transformer = {
        new Transformer(data
          .withColumnRenamed("BbMx>2.5", "BbMx>2,5")
          .withColumnRenamed("BbAv>2.5", "BbAv>2,5")
          .withColumnRenamed("BbMx<2.5", "BbMx<2,5")
          .withColumnRenamed("BbAv<2.5", "BbAv<2,5")

        )
    }
}
