package org.lop
package utilities.Models


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import upickle.default
import upickle.default._

import java.io.{File, FileOutputStream}

class PronoModel(val spark: SparkSession) {
    private val cols: List[String] = List("Div", "HomeTeam", "AwayTeam", "FTR", "HTR")
    private var indexerModel: PipelineModel = _
    private var model: PipelineModel = _
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    def fit(data: DataFrame, output: String): Unit = {

        val colsIndexer: List[StringIndexer] = cols
          .map(x => new StringIndexer()
            .setInputCol(x)
            .setOutputCol(s"${x}indexed"))
        indexerModel = new Pipeline()
          .setStages(colsIndexer.toArray).fit(data)


        val X = indexerModel.transform(data).drop(cols: _*)


        val assembler = new VectorAssembler()
          .setInputCols(X.drop(output).columns)
          .setOutputCol("features")
        val labelIndexer = new StringIndexer()
          .setInputCol(output)
          .setOutputCol("label")
        val Array(train, test) = X.randomSplit(Array(0.8, 0.2))
        val classifier = new LogisticRegression()
          .setMaxIter(10)
          .setRegParam(0.3)
          .setElasticNetParam(0.8)


        //        // Convertir les prédictions en valeurs de texte
        //        val labelConverter = new IndexToString()
        //          .setInputCol("prediction")
        //          .setOutputCol("predictedLabel")
        //          .setLabels(labelIndexer.fit(train).labels)

        val pipeline = new Pipeline()
          .setStages(Array(assembler, labelIndexer, classifier))
        model = pipeline.fit(train)
        val predictions = model.transform(test)
        // Évaluer la performance du modèle
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"Accuracy = $accuracy")
    }

    def save(path: String): Boolean = {
        implicit val pronoModelRW: default.ReadWriter[PronoModel] = upickle.default.macroRW[PronoModel]

        // Serialize the object to a JSON string
        val json = write(this)


        // Write the JSON string to a local file
        val localPath = s"/tmp/$path.json"
        val localOutputStream = new FileOutputStream(localPath)
        localOutputStream.write(json.getBytes("UTF-8"))
        localOutputStream.close()

        // Copy the local file to HDFS
        val hdfsPath = s"/models/$path.json"
        fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath))

        // Delete the local file
        val localFile = new File(localPath)
        localFile.delete()
    }
}

object PronoModel {

    def apply(spark: SparkSession): PronoModel = new PronoModel(spark)

    def load(path: String, spark: SparkSession): PronoModel = {
        implicit val pronoModelRW: default.ReadWriter[PronoModel] = upickle.default.macroRW[PronoModel]
        val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val hdfsPath = s"/models/$path.json"
        val hdfsInputStream = fs.open(new Path(hdfsPath))
        val json = scala.io.Source.fromInputStream(hdfsInputStream).mkString
        hdfsInputStream.close()
        read[PronoModel](json)
    }
}