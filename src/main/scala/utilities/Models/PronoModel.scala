package org.lop
package utilities.Models


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io._


@SerialVersionUID(100L)
case class PronoModel(var name: String,
                      var cols: List[String] = List("Div", "HomeTeam", "AwayTeam", "HTR"),
                      var indexerModel: PipelineModel = null,
                      var model: PipelineModel = null,
                      var labels: Array[String] = null
                     ) extends Serializable {


    def fit(data: DataFrame, output: String): Unit = {
        println("Fit model...")

        /* Convertit les colonnes textuelles en index */
        val colsIndexer: List[StringIndexer] = cols
          .map(x => new StringIndexer()
            .setInputCol(x)
            .setOutputCol(s"${x}indexed"))
        indexerModel = new Pipeline()
          .setStages(colsIndexer.toArray)
          .fit(data)
        val X = stringIndexing(data)


        val assembler = new VectorAssembler()
          .setInputCols(X.drop(output).columns)
          .setOutputCol("features")
        val labelIndexer = new StringIndexer()
          .setInputCol(output)
          .setOutputCol("label")

        /* Création des données d'entrainement et de test mais aussi du modèle */
        val Array(train, test) = X.randomSplit(Array(0.8, 0.2))
        val classifier = new LogisticRegression()
          .setMaxIter(1000)
          .setRegParam(0.3)
          .setElasticNetParam(0.8)


        labels = labelIndexer
          .fit(train)
          .labelsArray
          .flatten
        val labelConverter = new IndexToString()
          .setInputCol("prediction")
          .setOutputCol("predictedLabel")
          .setLabels(labels)

        val pipeline = new Pipeline()
          .setStages(Array(assembler, labelIndexer, classifier, labelConverter))
        model = pipeline.fit(train)
        val predictions = model.transform(test)
        predictions.show(1)
        // Évaluer la performance du modèle
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"Accuracy = $accuracy")
    }

    /**
     * Effectue une prédiction à base du nom des équipes
     *
     * @param data un dataframe contenant deux colonnes: HomeTeam et AwayTeam
     * @return Un dictionnaire contenant les probabilités de victoire , nul et défaite
     */
    def predict(data: DataFrame): Map[String, Float] = {
        println("Predicting...")
        model
          .transform(stringIndexing(data))
          .select("probability")
          .first()
          .get(0)
          .asInstanceOf[org.apache.spark.ml.linalg.DenseVector]
          .toArray
          .zipWithIndex
          .map(x => (labels(x._2), x._1.toFloat))
          .toMap

    }


    /**
     * Convertit les colonnes textuelles en index
     *
     * @param data un dataframe contenant les colonnes textuelles
     * @return un dataframe contenant les colonnes indexées
     */
    private def stringIndexing(data: DataFrame): DataFrame = {
        val result: DataFrame = indexerModel.transform(data).drop(cols: _*)
        cols.foldLeft(result)((acc, col) => acc.withColumnRenamed(s"${col}indexed", col))
    }

    override def toString: String = {
        s"""Model: $name
           |Cols: $cols
           |IndexerModel: $indexerModel
           |Model: $model
           |""".stripMargin

    }

    //noinspection ScalaUnusedSymbol
    private def readObject(in: ObjectInputStream): Unit = {
        name = in.readObject().asInstanceOf[String]
        cols = in.readObject().asInstanceOf[List[String]]
        indexerModel = in.readObject().asInstanceOf[PipelineModel]
        model = in.readObject().asInstanceOf[PipelineModel]
        labels = in.readObject().asInstanceOf[Array[String]]
    }

    //noinspection ScalaUnusedSymbol
    private def writeObject(out: ObjectOutputStream): Unit = {
        out.writeObject(name)
        out.writeObject(cols)
        out.writeObject(indexerModel)
        out.writeObject(model)
        out.writeObject(labels)
    }

    def save(spark: SparkSession): Boolean = {
        val localPath = s"/tmp/$name.lop"

        val oos = new ObjectOutputStream(new FileOutputStream(s"/tmp/$name.lop"))
        oos.writeObject(this)
        oos.close()

        val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)


        // Copy the local file to HDFS
        val hdfsPath = s"/models/$name.lop"
        if (fs.exists(new Path(hdfsPath))) {
            fs.delete(new Path(hdfsPath), true)
        }

        fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath))

        // Delete the local file
        val localFile = new File(localPath)
        localFile.delete()
    }
}

object PronoModel {


    def load(name: String, spark: SparkSession): PronoModel = {
        val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val hdfsPath = s"/models/$name.lop"
        val localPath = s"/home/machkour/$name.lop"
        fs.copyToLocalFile(new Path(hdfsPath), new Path(localPath))
        val ois = new ObjectInputStream(new FileInputStream(localPath))
        ois.readObject().asInstanceOf[PronoModel]
    }
}