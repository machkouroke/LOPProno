package org.lop
package utilities

import org.apache.spark.sql.DataFrame

object Joiner {
    def join(datasets: Seq[DataFrame]): DataFrame = {
        datasets.reduce(
            (dataset1, dataset2) => {
                val commonHeaders: Array[String] = dataset1.columns.intersect(dataset2.columns).map(x => s"`$x`")

                dataset1.select(commonHeaders.head, commonHeaders.tail: _*).
                  union(dataset2.select(commonHeaders.head, commonHeaders.tail: _*))
            }
        )
    }
}
