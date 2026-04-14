package com.tipdm.analyse.ForAsk.ask_model

import com.tipdm.analyse.ForAsk.ask_preprocess._
import com.tipdm.util.CommonUtil._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame

/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 13:46
  * //@Email: fansy1990@foxmail.com
  * 随机森林 测试类
  */
object RandomForestModelTest {

  def create_model(data: DataFrame) = {
    val labelIndexer = new StringIndexer()
      .setInputCol(label)
      .setOutputCol(indexedLabel)
      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol(scaled_features)
      .setOutputCol(indexedFeatures)
      .setMaxCategories(4)
      .fit(data)
    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol(indexedLabel)
      .setFeaturesCol(indexedFeatures)
      .setNumTrees(10)
      .setPredictionCol(predict_column)

    // Convert indexed label back to original label.
    val labelConverter = new IndexToString()
      .setInputCol(predict_column)
      .setOutputCol(predictedLabel)
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(data)

    model
  }

  def evaluate_model(data: DataFrame, model: PipelineModel) = {

    val predictions = model.transform(data)
    // Select example rows to display.
    predictions.select(predict_column, label, scaled_features, userid).show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(label)
      .setPredictionCol(predict_column)
    // .setMetricName("accuracy")

    val metrics = new MulticlassMetrics(predictions.select(predict_column, label).rdd.map(t => (t.getDouble(0), t.getDouble(1))))
    val accuracy = evaluator.evaluate(predictions)
    (accuracy, metrics)
  }

  def evaluate_model2(data: DataFrame, model: PipelineModel) = {
    val predictions = model.transform(data)
    predictions.select(predictedLabel, label, scaled_features).show(5)
    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(indexedLabel)
      .setPredictionCol(predict_column)
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    val metrics = new MulticlassMetrics(predictions.select(predict_column, indexedLabel).rdd.map(t => (t.getDouble(0), t.getDouble(1))))
    (accuracy, metrics)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)

    val features_label_data = SplitFeatureWithLabel.split(features_data)
    features_label_data.show(3)
    val filtered_data = UnionData.unionData(FilterData.filterData(features_label_data))
    val assembled_data = AssembleFeatureWithLabel.assemble(filtered_data)
    assembled_data.show(2)
    val scaled_data = ScaleData.scale(assembled_data)
    scaled_data.show(2, false)

    println("scaled_data size :" + scaled_data.count())

    val Array(train, test) = scaled_data.randomSplit(Array(0.8, 0.2))
    test.show(100, false)

    val model = create_model(train)
    val (accuracy, metrics) = evaluate_model2(test, model)
    println("Test Error = " + (1.0 - accuracy))
    println(metrics.confusionMatrix)
    println("f:" + metrics.weightedFMeasure)
    println("precision:" + metrics.weightedPrecision)
    println("recall:" + metrics.weightedRecall)
    println("test.count:" + test.count())
    println("test.1.count:" + test.filter(label + " = 1").count)

  }
}
