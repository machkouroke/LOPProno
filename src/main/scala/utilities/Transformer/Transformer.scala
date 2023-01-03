package org.lop
package utilities.Transformer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

class Transformer(data: DataFrame) {
    /**
     * Transforme les colonnes de type Integer en type Integer et les colonnes de type Float en type Float
     * @param colsInteger: List[String] - Liste des colonnes de type Integer
     * @param colsFloat: List[String] Colonnes de type Float
     * @return Transformer - Retourne l'objet Transformer
     */
    def typeTransform(colsInteger: List[String], colsFloat: List[String]): Transformer = {

        val colsString: List[Column] = data
          .columns
          .diff(colsInteger ++ colsFloat)
          .toList
          .map(x => col(s"`$x`")
            .cast("string"))

        val cols: List[Column] = colsInteger
          .map(x => col(s"`$x`")
            .cast("int")) ++
          colsFloat
            .map(x => col(s"`$x`").cast("double")) ++
          colsString

        new Transformer(data.select(cols: _*))
    }

}
