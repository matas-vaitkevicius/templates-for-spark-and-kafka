import pandas as pd
import numpy as np

from pandas import to_datetime
from cmd import Cmd

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.stat import Correlation
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format, udf

from typing import cast

from datetime import datetime

from pyspark.sql.types import IntegerType

desired_width=200

pd.set_option('display.width', desired_width)

np.set_printoptions(linewidth=desired_width)

pd.set_option('display.max_columns',10)

spark = (SparkSession.builder
         .appName("Read CSV")
         .master("local[*]")
         .getOrCreate())

def dataAnalysis():
    dataFrame = spark.read.csv(
        "H:/downloads/Code (1)/Starting workspaces/Chapter 03/kc_house_data.csv",
        header=True,
        inferSchema=True)
    dataFrameWithDroppedColumns = dataFrame.drop("id","date","zipcode","lat","long")
    vector_col = "corr_features"
    assembler = VectorAssembler(inputCols=dataFrameWithDroppedColumns.columns, outputCol=vector_col)
    df_vector = assembler.transform(dataFrameWithDroppedColumns).select(vector_col)

    # get correlation matrix
    matrix = Correlation.corr(df_vector, vector_col)
    print(dataFrameWithDroppedColumns.columns)
    print(matrix.collect()[0]["pearson({})".format(vector_col)].values)

def linearRegression():
    dataFrame = spark.read.csv(
        "H:/downloads/Code (1)/Starting workspaces/Chapter 03/kc_house_data.csv",
        header=True,
        inferSchema=True)

    #dataFrame.printSchema()
    #print(dataFrame.count())
    #genderToInt = udf(lambda x: 1 if x == 'M' else 0)
    #gymDataFrameWithGenderInt = gymDataFrame.withColumn("GenderAsInt", genderToInt("Gender").cast(IntegerType()))
    #gymDataFrameWithGenderInt.show()
    replaceEmptyRenovationYearWithBuiltYear = udf(lambda built, renovated: built if renovated == 0 else renovated)
    dataFrameWithRenovationYear0ReplacedWithBuiltYear = dataFrame.withColumn(
        "yr_renovated_replaced",
        replaceEmptyRenovationYearWithBuiltYear("yr_built","yr_renovated").cast(IntegerType()))

    modelInputData = dataFrameWithRenovationYear0ReplacedWithBuiltYear.withColumnRenamed("price","label")
    modelInputSplits = modelInputData.randomSplit([0.85,0.15])
    trainingData = modelInputSplits[0]
    holdoutData = modelInputSplits[1]

    zipIndexer = StringIndexer(inputCol="zipcode", outputCol="zipcodeIndex")
    vectorEncoder = OneHotEncoder(inputCol="zipcodeIndex", outputCol="zipcodeVector")
    vectorAssembler = VectorAssembler(outputCol="features", inputCols=["bedrooms",
                                                          "bathrooms",
                                                          "sqft_living",
                                                          "sqft_lot",
                                                          "floors",
                                                          "waterfront",
                                                          "view",
                                                          "condition",
                                                          "grade",
                                                          "sqft_above",
                                                          "sqft_basement",
                                                          "yr_built",
                                                          "yr_renovated_replaced",
                                                          "sqft_living15",
                                                          "sqft_lot15",
                                                          "zipcodeVector"
                                                          ])

    lr = LinearRegression()
    paramGrid = (((ParamGridBuilder()
                 .addGrid(lr.elasticNetParam, [0.7,0.9,0.9999]))
                 .addGrid(lr.regParam, [0.7,0.9,0.99,0.999]))
                 .build())
    trainingValidationSetup = (TrainValidationSplit()
                          .setEstimator(lr)
                          .setEstimatorParamMaps(paramGrid)
                          .setEvaluator(RegressionEvaluator(metricName="r2"))
                          .setTrainRatio(0.2))

    pipeline = Pipeline(stages=[zipIndexer, vectorEncoder, vectorAssembler, trainingValidationSetup])
    pipelineModel = pipeline.fit(trainingData)

    holdoutModel = pipelineModel.transform(holdoutData).drop("prediction")

    stageCount = len(pipelineModel.stages)-1
    lastStage = cast(trainingValidationSetup,pipelineModel.stages[stageCount])
    bestModel = cast(LinearRegressionModel, lastStage.bestModel)
    print(f"Coefficients: {bestModel.coefficients}")
    print(f" Intercept: {bestModel.intercept},"
          f" RegParam: {bestModel.getRegParam()},"
          f" ElasticNetParam: {bestModel.getElasticNetParam()},"
          f" FitIntercept: {bestModel.getFitIntercept()}")
    print(f"R2 {bestModel.summary.r2}")
    print(f"RMSE {bestModel.summary.rootMeanSquaredError}")
    test = bestModel.evaluate(holdoutModel)
    print(f"Test R2 {test.r2}")
    print(f"Test RMSE {test.rootMeanSquaredError}")

def biglogDataAgregationSQL():
    biglogDataFrame = spark.read.csv("H:/downloads/biglog.txt", header=True, inferSchema=True)
    #biglogDataFrame.createOrReplaceTempView("logs")
    biglogMonths = biglogDataFrame.select("level",
                                          date_format("datetime","yyyy-MMMM").name("Month"))

    grouped = (biglogMonths.groupBy("level","Month").count().orderBy("count", ascending=False)
               .withColumn("MonthAsDate", to_date("Month","yyyy-MMMM")))

    grouped.show(grouped.count())

    pivotOnMonth = grouped.groupBy("level").pivot("MonthAsDate")


def bigLogDataAgreggationRDD():
    biglogDataFrame = spark.read.csv("H:/downloads/biglog.txt", header=True, inferSchema=True)
    biglogDataFrame.printSchema()
    biglogDataFrame.show(10)
    #biglogDataFrame.createOrReplaceTempView("logs")
    biglogMonths = biglogDataFrame.rdd.map((lambda x: (x["level"], x["datetime"].strftime('%Y-%B'))))
    print(biglogMonths.keys().take(10))
    grouped = biglogMonths.groupByKey()
    #.map(lambda x: (x, to_datetime(x[1])))
    #print(grouped.take(10))


def studentDataAgregation():
    studentDataFrame = spark.read.csv(
        "H:/downloads/Code/Delivered Folder/Starting Workspace/Project/src/main/resources/exams/students.csv",
        header=True,
        inferSchema=True)

    # print(df.withColumn("Sum", F.udf(lambda x,y: x+" "+y)("subject","year")).take(5))
    # print(df.select("student_id", "subject", "year").where("subject='Modern Art' AND year > 2007").take(10))
    studentDataFrame.createOrReplaceTempView("students")

    hasPassed = spark.udf.register("hasPassed", lambda x: x > 50)
    withHasPassed = studentDataFrame.withColumn("has Passed", hasPassed("score"))

    withHasPassed.show(100)

    #tempView = spark.sql(
    #    "SELECT subject, year, exam_center_id, MAX(score) as MAX, MIN(score) as MIN FROM students GROUP BY subject, year, exam_center_id")
    #withDiff = tempView.withColumn("Diff", tempView.MAX - tempView.MIN)

    #withDiff.orderBy("Diff", ascending=False).show(tempView.count())





#dataAnalysis()
linearRegression()

#bigLogDataAgreggationRDD()
#biglogDataAgregationSQL()
#studentDataAgregation()

#text = input("prompt")