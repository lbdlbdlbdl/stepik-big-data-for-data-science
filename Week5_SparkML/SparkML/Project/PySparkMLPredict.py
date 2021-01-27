import io
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

# Используйте как путь откуда загрузить модель
MODEL_PATH = 'spark_ml_model'


def process(spark, input_file, output_file):
    #input_file - путь к файлу с данными для которых нужно предсказать ctr
    #output_file - путь по которому нужно сохранить файл с результатами [ads_id, prediction]
    
    input_df = spark.read.parquet(input_file)

    # load model
    model = PipelineModel.load(MODEL_PATH)
    # run model on test data and get presictions
    predictions = model.transform(input_df)
    
    # save predictions to cvs 
    data = predictions.select('ad_id', 'prediction')
    data.coalesce(1).write.csv(output_file, header = True)
    spark.stop()

def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    output_file = argv[1]
    print("Output path to file: " + output_file)
    spark = _spark_session()
    process(spark, input_path, output_file)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLPredict').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)
