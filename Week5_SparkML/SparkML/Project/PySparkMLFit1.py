import io
import sys
import operator

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor

MODEL_PATH = 'spark_ml_model'
SPLIT_RATIO = 0.8


def get_model_conf():
    dt = DecisionTreeRegressor(labelCol='ctr')
    rf = RandomForestRegressor(labelCol='ctr')
    gbt = GBTRegressor(labelCol='ctr')
    model_conf = {
        'dt': {'model': dt,
               'params': ParamGridBuilder()
                  .addGrid(dt.maxDepth, [2, 5, 10])
                  .addGrid(dt.maxBins, [10, 20, 40])
                  .addGrid(dt.minInfoGain, [0.1, 0.3, 0.7])
                  .build(),
               'best': None,
               'score': None},
        'rf': {'model': rf,
               'params': ParamGridBuilder()
                  .addGrid(rf.maxDepth, [2, 5, 10])
                  .addGrid(rf.maxBins, [10, 20, 40])
                  .addGrid(rf.minInfoGain, [0.1, 0.3, 0.7])
                  .build(),
               'best': None,
               'score': None},
        'gbt': {'model': gbt,
                'params': ParamGridBuilder()
                  .addGrid(gbt.maxDepth, [2, 5, 10])
                  .addGrid(gbt.maxBins, [10, 20, 40])
                  .addGrid(gbt.maxIter, [5, 15])
                  .build(),
                'best': None,
                'score': None}
    }
    return model_conf


def fit_tvs(model_key, model_conf, features, evaluator, df_train, df_test):
    tvs = TrainValidationSplit(estimator=Pipeline(stages=[features, model_conf[model_key]['model']]),
                               estimatorParamMaps=model_conf[model_key]['params'],
                               evaluator=evaluator,
                               trainRatio=SPLIT_RATIO)
    fitted_models = tvs.fit(df_train)
    best_model = fitted_models.bestModel
    score = evaluator.evaluate(best_model.transform(df_test))
    model_conf[model_key]['best'] = best_model
    model_conf[model_key]['score'] = score



def process(spark, train_data, test_data):
    model_conf = get_model_conf()
    df_train = spark.read.parquet(train_data)
    df_test = spark.read.parquet(test_data)

    features = VectorAssembler(inputCols = df_train.columns[1:-1], outputCol='features')
    evaluator = RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse')

    for key in model_conf.keys():
        fit_tvs(key, model_conf, features, evaluator, df_train, df_test)
        log_results(key, model_conf)

    key = get_best_key(model_conf)
    print(f"Best model type = {key} with score = {model_conf[key]['score']}")
    best_model = model_conf[key]['best']
    best_model.write().overwrite().save(MODEL_PATH)
    print('Best model was saved')


def get_best_key(model_conf):
    md = {k: v['score'] for k, v in model_conf.items()}
    return min(md.items(), key=operator.itemgetter(1))[0]


def log_results(model_key, model_conf):
    j_obj = model_conf[model_key]['best'].stages[-1]._java_obj
    print(f'\nModel type = {model_key}')
    print(f"RMSE = {model_conf[model_key]['score']}")
    print(f'maxDepth = {j_obj.getMaxDepth()}')
    print(f'maxBins = {j_obj.getMaxBins()}')



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
