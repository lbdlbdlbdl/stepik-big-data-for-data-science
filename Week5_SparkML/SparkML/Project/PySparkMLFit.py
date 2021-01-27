import io
import sys
import numpy as np

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.regression import LinearRegression, GBTRegressor, RandomForestRegressor, DecisionTreeRegressor

# Используйте как путь куда сохранить модель
MODEL_PATH = 'spark_ml_model'


def get_paramGrids_dict(pipeline):
    pipeline_stages = pipeline.getStages()
    
    lr = LinearRegression(featuresCol='features', labelCol='ctr', predictionCol='prediction')
    gbt = GBTRegressor(featuresCol='features', labelCol='ctr', predictionCol='prediction')
    rf = RandomForestRegressor(featuresCol='features', labelCol='ctr', predictionCol='prediction')
    dt = DecisionTreeRegressor(featuresCol='features', labelCol='ctr', predictionCol='prediction', maxDepth=5)    
    
    lr_paramGrid = ParamGridBuilder()\
        .baseOn({pipeline.stages: pipeline_stages + [lr]}) \
        .addGrid(lr.regParam, [0.4])\
        .addGrid(lr.maxIter, [40])\
        .addGrid(lr.elasticNetParam, [0.8])\
        .build()
    gbt_paramGrid = ParamGridBuilder()\
        .baseOn({pipeline.stages: pipeline_stages + [gbt]}) \
        .addGrid(gbt.maxDepth, [5, 6, 7])\
        .addGrid(gbt.maxBins, [32, 33])\
        .addGrid(gbt.maxIter, [4, 5, 6, 7])\
        .build()
    rf_ParamGrid = ParamGridBuilder()\
        .baseOn({pipeline.stages: pipeline_stages + [rf]}) \
        .addGrid(rf.maxDepth, [2, 3, 4, 5])\
        .addGrid(rf.numTrees, [19, 20, 21])\
        .addGrid(rf.featureSubsetStrategy, ['onethird', '0.5', 'sqrt'])\
        .build()
    dt_paramGrid = ParamGridBuilder()\
        .baseOn({pipeline.stages: pipeline_stages + [dt]}) \
        .addGrid(dt.maxDepth, [4, 5, 6])\
        .addGrid(dt.minInfoGain, [0.0, 0.1, 0.2])\
        .addGrid(dt.maxBins, [28, 32, 33])\
        .build()
    
    return {'lr': lr_paramGrid, 'gbt': gbt_paramGrid, 'rf': rf_ParamGrid, 'dt': dt_paramGrid }


def process(spark, train_data, test_data):
    #train_data - путь к файлу с данными для обучения модели
    train_df =  spark.read.parquet("train.parquet")
    #test_data - путь к файлу с данными для оценки качества модели
    test_df =  spark.read.parquet("test.parquet")

    features = VectorAssembler(inputCols=train_df.columns[:-1], outputCol="features")
    pipeline = Pipeline(stages=[features]) 
    # define models and paramGrids 
    paramGrids_dict = get_paramGrids_dict(pipeline)
    grids = paramGrids_dict['lr'] + paramGrids_dict['gbt'] + paramGrids_dict['rf'] + paramGrids_dict['dt']
    
    evaluator = RegressionEvaluator(labelCol="ctr", predictionCol="prediction", metricName="rmse")
    # define tvs to find the best set of hyperparameters and best model
    tvs = TrainValidationSplit(estimator=pipeline,
                               estimatorParamMaps=grids,
                               evaluator=evaluator,
                               trainRatio=0.8)
    # train model and run tvs
    model = tvs.fit(train_df)
    p_model = model.bestModel
    print("Model: %s" % p_model.stages[1])
    print("Train data RMSE: %f" % round(np.min(model.validationMetrics), 4))
    
    # save model 
    p_model.write().overwrite().save(MODEL_PATH)
    
    # evaluate model on test data 
    prediction = p_model.transform(test_df)
    test_rmse = evaluator.evaluate(prediction)
    print("Test data RMSE: %f" % round(test_rmse, 4))

def main(argv):
    train_data = argv[0]
    print("Input path to train data: " + train_data)
    test_data = argv[1]
    print("Input path to test data: " + test_data)
    spark = _spark_session()
    process(spark, train_data, test_data)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Train and test data are require.")
    else:
        main(arg)
