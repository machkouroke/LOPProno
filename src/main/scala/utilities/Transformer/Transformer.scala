package org.lop
package utilities.Transformer

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Column, DataFrame}
import constant.FileType.{colsFloat, colsInteger}


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
    def oneHotEncoder(): Transformer = {
        val cols: List[String] = colsString(colsInteger, colsFloat)

        def columnEncoder(data: DataFrame, colName: String): Transformer = {
            val distinctValue = data.select(col(s"`$colName`")).distinct().collect().map(_.getString(0))
            var tempData: DataFrame = data.select(data.columns.map(c => col(s"`$c`")): _*)
            for {value <- distinctValue} {
                tempData = tempData.
                  withColumn(
                      s"${colName}_$value",
                      when(data(colName) === value, 1).otherwise(0)
                  )
            }
            new Transformer(tempData.drop(colName))
        }

        var encodedData = new Transformer(data)
        for {col <- cols} {
            encodedData = columnEncoder(encodedData.data, col)
        }
        new Transformer(encodedData.data)
    }


    /**
     * Retourne la liste des colonnes de type String
     *
     * @param colsInteger
     * @param colsFloat
     * @return
     */
    private def colsString(colsInteger: List[String], colsFloat: List[String]): List[String] = {
        data
          .columns
          .diff(colsInteger ++ colsFloat)
          .filter(col => !col.equals("Date"))
          .toList
    }

}
