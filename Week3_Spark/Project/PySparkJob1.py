import io
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def train_split(df, target_folder):
    splits = df.randomSplit([0.75, 0.25])
    splits[0].write.parquet(target_folder + "\\train\\")
    splits[1].write.parquet(target_folder + "\\test\\")
    
    
# выбираем нужные поля из старого df и вычисляем необходимые
def get_aggr_df(df):
    count_cond_lambda = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    
    ndf = df.select(['ad_id', 
                     'target_audience_count',  
                     'has_video', 
                     'event', 
                     'date',
                     (col('ad_cost_type') == 'CPM').cast('integer').alias('is_cpm'), 
                     (col('ad_cost_type') == 'CPC').cast('integer').alias('is_cpc'), 
                     'ad_cost']) \
            .groupBy('ad_id', 'target_audience_count', 'has_video', 'is_cpm', 'is_cpc', 'ad_cost') \
            .agg(count_cond_lambda(F.col('event') == 'view').alias('views'), \
                 count_cond_lambda(F.col('event') == 'click').alias('clicks'), \
                 F.countDistinct(F.when(F.col('event') == 'view', col('date'))).alias('day_count')) \
            .select([ 
                     'ad_id', 
                     'target_audience_count',  
                     'has_video', 
                     'is_cpm',
                     'is_cpc',
                     'ad_cost',
                     'day_count',
                     F.expr("clicks / views").alias('CTR')]) 
    return ndf
    
    
def process(spark, input_file, target_path):
    # читаем файл в DataFrame сущность
    df = spark.read.parquet(input_file)    
    # делим df на тренирующую и тестовую выборки
    train_split(get_aggr_df(df), target_path) 
    


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    target_path = argv[1]
    print("Target path: " + target_path)
    spark = _spark_session()
    process(spark, input_path, target_path)
    input()


def _spark_session():
    return SparkSession.builder.appName('PySparkJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)
