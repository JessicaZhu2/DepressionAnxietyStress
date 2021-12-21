// Databricks notebook source
import org.apache.spark.sql.Column
import scala.io.Source
import java.io.File
import org.apache.spark.sql.functions._ 
import sqlContext.implicits._ 
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.DataFrame
import spark.implicits._

// COMMAND ----------

// File location and type
val file_location = "/autumn_2021/jzhu2/project/data.csv"

// CSV options
val delimiter = "\t"

val dataDF = spark.read.option("inferSchema", "true").option("header", "true").option("sep", delimiter).csv(file_location)

// COMMAND ----------

// Save as parquet file 
//dataDF.write.parquet("dbfs:/autumn_2021/jzhu2/project/data1.parquet")

// Read in the parquet file created above
// Parquet files are self-describing so the schema is preserved
// The result of loading a Parquet file is also a DataFrame
val data = spark.read.parquet("dbfs:/autumn_2021/jzhu2/project/data1.parquet")


// COMMAND ----------

// ------------------------------------------------ DATA CLEANING : Extact interested columns  -----------------------------------------------------------------------------------------------------
val tipiCols = data.columns.filter(_.startsWith("TIPI")).toList
val dassQuestionCols = data.columns.filter(_.startsWith("Q")).toList
var basicInfo = List("education", "gender", "engnat", "age", "hand", "religion", "orientation", "race", "married", "familysize", "major", "country", "source", "introelapse", "testelapse", "surveyelapse", "urban", "uniquenetworklocation", "screensize", "voted")



val selectedCols = tipiCols ::: dassQuestionCols ::: basicInfo
val df = data.select(selectedCols.map(data(_)) : _*)

// COMMAND ----------

// ------------------------------------------------ DATA CLEANING : CALCULATE PERSONALITY COLUMNS  -----------------------------------------------------------------------------------------------------
/*
The Ten Item Personality Inventory is a test in order to assess 5 personality traits of participants:  
1. Extraversion  
2. Agreeableness  
3. Conscientiousness  
4. Emotional Stability  
5. Openness  

Recode the reverse-scored columns (i.e., recode a 7 with a 1, a 6 with a 2, a 5 with a 3, etc.). The reverse scored items are 2, 4, 6, 8, & 10.
Extraversion: 1, 6R; Agreeableness: 2R, 7; Conscientiousness; 3, 8R; Emotional Stability: 4R, 9; Openness to Experiences: 5, 10R.
*/ 

val reverseTipi = df.withColumn("TIPI6", abs(df("TIPI6") - 8))
                    .withColumn("TIPI2", abs(df("TIPI2") - 8) )
                    .withColumn("TIPI8", abs(df("TIPI8") - 8) )
                    .withColumn("TIPI4", abs(df("TIPI4") - 8) )
                    .withColumn("TIPI10", abs(df("TIPI10") - 8) )

def calculatePersonality(personalityCol: Array[Column]) = {
  var calculatedPersonality = personalityCol.foldLeft(lit(0)){(x, y) => x+y}/personalityCol.length
  calculatedPersonality
}
// adding extraversion score
val extraCols = Array(col("TIPI1"), col("TIPI6"))
val extraversion = calculatePersonality(extraCols)

// adding aggreableness score
val agreeCols = Array(col("TIPI2"), col("TIPI7"))
val agreeableness=  calculatePersonality(agreeCols)

// adding conscientiousness score
val conscCols = Array(col("TIPI3"), col("TIPI8"))
val conscientiousness =  calculatePersonality(conscCols)

// adding emotional stability score
val emotionCols = Array(col("TIPI4"), col("TIPI9"))
val emotionalStability  =  calculatePersonality(emotionCols)

// adding openess to experiences score
val openCols = Array(col("TIPI1"), col("TIPI6"))
val openness =  calculatePersonality(openCols)


val tipicols = df.columns.filter(_.startsWith("TIPI")).toList

var personality_DF = reverseTipi.withColumn("Extraversion", extraversion)
                                .withColumn("Conscientiousness", conscientiousness)
                                .withColumn("Agreeableness", agreeableness)
                                .withColumn("Emotional_Stability", emotionalStability)
                                .withColumn("Openness_to_Experiences", openness)
                                .drop(tipicols : _*)


// COMMAND ----------

// ------------------------------------------------ COPIED FROM VICTOR'S NOTEBOOK - DASS Scores -----------------------------------------------------------------------------------------------------

// Calculate DASS scores A = Score, I = Position of question in survey, E = Miliseconds took to answer the question
val depQuestionSet = List("Q3", "Q5", "Q10", "Q13", "Q16", "Q17", "Q21", "Q24", "Q26", "Q31", "Q34", "Q37", "Q38", "Q42").map(_ + "A")
val anxQuestionSet = List("Q2", "Q4", "Q7", "Q9", "Q15", "Q19", "Q20", "Q23", "Q25", "Q28", "Q30", "Q36", "Q40", "Q41").map(_ + "A")
val strQuestionSet = List("Q1", "Q6", "Q8", "Q11", "Q12", "Q14", "Q18", "Q22", "Q27", "Q29", "Q32", "Q33", "Q35", "Q39").map(_ + "A")

val mapCategoryToQSet = Map(
  "depression" -> depQuestionSet,
  "anxiety" -> anxQuestionSet,
  "stress" -> strQuestionSet
)

// COMMAND ----------

// ------------------------------------------------ COPIED FROM VICTOR'S NOTEBOOK - DASS Scores -----------------------------------------------------------------------------------------------------

var initialData = personality_DF

var normalizedDataset = initialData
//Normalize scores from range (1, 4) -> (0, 3)
mapCategoryToQSet.foreach((qSet) => {
  qSet._2.foreach((question) => {
    normalizedDataset = normalizedDataset.withColumn(question, col(question) - 1)
  })
})

//Calculate the Dass Scores 
mapCategoryToQSet.foreach((qSet) => {
  normalizedDataset = normalizedDataset.withColumn(qSet._1 + "_score", qSet._2.map(col(_)).reduce(_ + _))
})

val calculateDepressionCategory = udf((s: Integer) => {
  val category = s match {
    case score if 0 until 10 contains score => "Normal"
    case score if 10 until 14 contains score => "Mild"
    case score if 14 until 21 contains score => "Moderate"
    case score if 21 until 28 contains score => "Severe"
    case _ => "Extremely Severe"
  }
 category  
})

val calculateAnxietyCategory = udf((s: Integer) => {
  val category = s match {
    case score if 0 until 8 contains score => "Normal"
    case score if 8 until 10 contains score => "Mild"
    case score if 10 until 15 contains score => "Moderate"
    case score if 15 until 20 contains score => "Severe"
    case _ => "Extremely Severe"
  }
 category  
})

val calculateStressCategory = udf((s: Integer) => {
  val category = s match {
    case score if 0 until 15 contains score => "Normal"
    case score if 15 until 19 contains score => "Mild"
    case score if 19 until 26 contains score => "Moderate"
    case score if 26 until 33 contains score => "Severe"
    case _ => "Extremely Severe"
  }
 category  
})


normalizedDataset = normalizedDataset.withColumn("depression_severity", calculateDepressionCategory(col("depression_score")))
normalizedDataset = normalizedDataset.withColumn("anxiety_severity", calculateAnxietyCategory(col("anxiety_score")))
normalizedDataset = normalizedDataset.withColumn("stress_severity", calculateStressCategory(col("stress_score")))

// COMMAND ----------

// DBTITLE 1,ML Model
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types.Metadata

// Baseline
println("BASELINE PERFORMANCE:")
normalizedDataset.agg(mean("stress_score").alias("mean"), stddev("stress_score").alias("rmse")).show


val data = normalizedDataset.select("Q1A", "Q2A", "Q3A", "surveyelapse", "Agreeableness", "Conscientiousness", "Emotional_Stability", "Extraversion", "Openness_to_Experiences", "education", "urban", "country", "gender", "engnat", "age", "screensize", "uniquenetworklocation", "hand", "religion", "orientation", "race", "voted", "married", "familysize", "source", "introelapse", "testelapse", "stress_score").withColumnRenamed("stress_score", "label")

val splits = data.randomSplit(Array(0.8, 0.2))
val (trainingData, testData) = (splits(0), splits(1))


val countryIndexer = new StringIndexer()
  .setInputCol("country")
  .setOutputCol("country_index")
  .setHandleInvalid("keep")
  .fit(data)

val assembler = new VectorAssembler()
  .setInputCols(Array("Q1A", "Q2A", "Q3A", "country_index", "source", "introelapse", "testelapse", "surveyelapse", "Agreeableness", "Conscientiousness", "Emotional_Stability", "Extraversion", "Openness_to_Experiences", "education", "urban", "gender", "engnat", "age", "screensize", "uniquenetworklocation", "hand", "religion", "orientation", "race", "voted", "married", "familysize"))
  .setOutputCol("features")


val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxBins(150)
      .setNumTrees(14)
      .setMaxDepth(14)


val pipeline = new Pipeline().setStages(Array(countryIndexer, assembler, rf))
val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)
val predictionsTrain = model.transform(trainingData)


val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

println("ML PERFORMANCE:")
val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

val rmseTrain = evaluator.evaluate(predictionsTrain)
    println(s"Root Mean Squared Error (RMSE) on train data = $rmseTrain")

println("==================================================")
println

val featureMetadata = predictions.schema("features").metadata
val attrs = featureMetadata.getMetadata("ml_attr").getMetadata("attrs")
val f: (Metadata) => (Long,String) = (m => (m.getLong("idx"), m.getString("name")))
val nominalFeatures= attrs.getMetadataArray("nominal").map(f)
val numericFeatures = attrs.getMetadataArray("numeric").map(f)
val features = (numericFeatures ++ nominalFeatures).sortBy(_._1)


// COMMAND ----------

// ------------------------------------------------ DATA CLEANING : DROP QUESTIONS COLUMNS FROM DATAFRAME -----------------------------------------------------------------------------------------------------
val questionCols = normalizedDataset.columns.filter(_.startsWith("Q")).toList

var personality_DASS_DF = normalizedDataset.drop(questionCols : _*)
personality_DASS_DF.printSchema()


// COMMAND ----------

// ---------------------------------------------------------------------------- DATA CLEANING SECTION : MAPPING DEMOGRAPHIC NUMERICAL ID TO ACTUAL VALUE ------------------------------------------------------------------

// data transformation function to join the mapping df to the main df
def join_Demographics(main_col: String, other_df: DataFrame, other_col: String)(main_df: DataFrame): DataFrame = {
  val joined = main_df.join(other_df, col(main_col) === col(other_col), "left").drop(col(main_col)).drop(col(other_col))
  joined
}

// education mapping df
var data = Seq((1, "< High school"), (2, "High school"), (3, "University"), (3, "Graduate"))
var rdd = spark.sparkContext.parallelize(data)
var educationDF = rdd.toDF("education_id","education_type")

// gender mapping df
data = Seq((1, "Male"), (2, "Female"), (3, "Other"))
rdd = spark.sparkContext.parallelize(data)
var genderDF = rdd.toDF("gender_id","gender_type")

// engnat mapping df
data = Seq((1, "Yes"), (2, "No"))
rdd = spark.sparkContext.parallelize(data)
var engnatDF = rdd.toDF("engnat_id","engnat_type")

// orientation mapping df
data = Seq((1,"Heterosexual"), (2,"Bisexual"), (3,"Homosexual"), (4,"Asexual"), (5,"Other"))
rdd = spark.sparkContext.parallelize(data)
var orientationDF = rdd.toDF("orientation_id","orientation_type")

// religion mapping df
data = Seq((1,"Agnostic"), (2,"Atheist"), (3,"Buddhist"), (4, "Christian (Catholic)"), (5, "Christian (Mormon)"), (6, "Christian (Protestant)"), (7, "Christian (Other)"), (8, "Hindu"), (9, "Jewish"), (10, "Muslim"), (11, "Sikh"), (12, "Other"))
rdd = spark.sparkContext.parallelize(data)
var religionDf = rdd.toDF("religion_id","religion_type")

// race mapping df
data = Seq((10,"Asian"), (20,"Arab"), (30,"Black"), (40,"Indigenous Australian"), (50,"Native American"), (60,"White"), (70,"Other"))
rdd = spark.sparkContext.parallelize(data)
var raceDF = rdd.toDF("race_id","race_type")

// married mapping df
data = Seq((1,"Never married"), (2,"Currently married"), (3,"Previously married"))
rdd = spark.sparkContext.parallelize(data)
var marriedDf = rdd.toDF("married_id","married_type")


// dataframe with the actual values
val mainDF = personality_DASS_DF.transform(join_Demographics("education", educationDF,  "education_id"))
                .transform(join_Demographics("gender", genderDF,  "gender_id"))
                .transform(join_Demographics("engnat", engnatDF,  "engnat_id"))
                .transform(join_Demographics("orientation", orientationDF,  "orientation_id"))
                .transform(join_Demographics("religion", religionDf, "religion_id"))
                .transform(join_Demographics("race", raceDF, "race_id"))
                .transform(join_Demographics("married", marriedDf, "married_id"))

mainDF.cache()
mainDF.show(10)

// COMMAND ----------

// ---------------------------------------------------------------------------- DATA VISUALIZATION SECTION : HELPER FUNCTION FOR DATA VISUALIZATION ----------------------------------------------------------------

def orderDF_DASSavg(mainDF: DataFrame, main_col: String, order: Array[String]) = {
  var DASS_avg_group_col = mainDF.groupBy(main_col).avg()
  val orderDF = order.toSeq.toDF
  val orderDFWithId = orderDF.withColumn("id", monotonically_increasing_id)
  val joined = DASS_avg_group_col.join(orderDFWithId, col(main_col) === col("value"), "inner")
  val sortedDF = joined.orderBy("id")
  sortedDF
}

def orderDF_DASScount(mainDF: DataFrame, main_col: String, order: Array[String]) = {
  var DASS_count_group_col = personality_DASS_DF.groupBy(main_col).count()
  val orderDF = order.toSeq.toDF
  val orderDFWithId = orderDF.withColumn("id", monotonically_increasing_id)
  val joined = DASS_count_group_col.join(orderDFWithId, col(main_col) === col("value"), "inner")
  val sortedDF = joined.orderBy("id")
  sortedDF
}

val calculateAgeCategory = udf((s: Integer) => {
  val category = s match {
    case score if 0 until 18 contains score => "Below 18"
    case score if 18 until 24 contains score => "18-24"
    case score if 25 until 34 contains score => "25-34"
    case _ => "35 and above"
  }
 category  
})

// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : DISTRIBUTION OF DASS SCORES --------------------------------------------------------------------------

val order = Array("Normal", "Mild", "Moderate", "Severe", "Extremely Severe")
var dep_dist = orderDF_DASScount(mainDF, "depression_severity", order)
var anx_dist = orderDF_DASScount(mainDF, "anxiety_severity", order)
var stress_dist = orderDF_DASScount(mainDF, "stress_severity", order)

display(dep_dist)

// COMMAND ----------

// ---------------------------------------------------------------------------- VISUALIZATION SECTION : Depression vs Personailty Grouped Bar/Line Charts -----------------------------------------------------------------

val order = Array("Normal", "Mild", "Moderate", "Severe", "Extremely Severe")
var dep_DF = orderDF_DASSavg(mainDF, "depression_severity", order)
var anx_DF = orderDF_DASSavg(mainDF, "anxiety_severity", order)
var stress_DF = orderDF_DASSavg(mainDF, "stress_severity", order)

dep_DF.cache()
anx_DF.cache()
stress_DF.cache()

display(stress_DF)

// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : Gender and DASS scores chart --------------------------------------------------------------------------

val order = Array("Male", "Female", "Other")
var genderDf = orderDF_DASSavg(mainDF, "gender_type", order)
display(genderDf)

// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : English Native Language and DASS scores chart --------------------------------------------------------------------------

val order =Array("Yes", "No")
var engnatDF = orderDF_DASSavg(mainDF, "engnat_type", order)
display(engnatDF)

// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : Orientation and DASS scores chart --------------------------------------------------------------------------

val order =Array("Heterosexual", "Bisexual", "Homosexual", "Asexual", "Other")
var orientationDF = orderDF_DASSavg(mainDF, "orientation_type", order)
display(orientationDF)

// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : RELIGION and DASS scores chart --------------------------------------------------------------------------

val order =Array("Agnostic","Atheist","Buddhist","Christian (Catholic)","Christian (Mormon)","Christian (Protestant)","Christian (Other)","Hindu","Jewish","Muslim","Sikh","Other")
var religionDF = orderDF_DASSavg(mainDF, "religion_type", order)
display(religionDF)

// COMMAND ----------



// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : Race and DASS scores chart --------------------------------------------------------------------------

val order =Array("Asian", "Arab", "Black", "Indigenous Australian", "Native American", "White", "Other")
var raceDF = orderDF_DASSavg(mainDF, "race_type", order)
display(raceDF)

// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : Married and DASS scores chart --------------------------------------------------------------------------

val order = Array("Never married", "Currently married", "Previously married")
var marriedDF = orderDF_DASSavg(mainDF, "married_type", order)
display(marriedDF)

// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : Age group and DASS scores chart --------------------------------------------------------------------------
var mainDF_withAgeGroup = mainDF.withColumn("age_group", calculateAgeCategory(col("age")))

val order = Array("Below 18", "18-24", "25-34", "35 and above")
var ageDF = orderDF_DASSavg(mainDF_withAgeGroup, "age_group", order)
display(ageDF)



// COMMAND ----------

// ----------------------------------------------------------------------------- VISUALIZATION SECTION : Education and DASS scores chart --------------------------------------------------------------------------

val order = Array(null, "< High school", "High school", "University", "Graduate")
var educationDF = orderDF_DASSavg(mainDF, "education_type", order)
display(educationDF)
