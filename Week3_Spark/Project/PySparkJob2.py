import io
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
from pyspark.sql import functions as F
from pyspark.sql.functions import min, max, col


def train_split(df, target_folder):
    splits = df.randomSplit([0.75, 0.25])
    splits[0].write.parquet(target_folder + "\\train\\")
    splits[1].write.parquet(target_folder + "\\test\\")
    
    
# подсчет ctr и количества дней показа объявления
def get_ctr_df(df):
    count_cond_lambda = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    
    # Вариант без SQL кода:
    ctr_df = df.groupBy('ad_id') \
               .agg(count_cond_lambda(F.col('event') == 'view').alias('views'), \
                    count_cond_lambda(F.col('event') == 'click').alias('clicks'), \
                    F.countDistinct(F.when(F.col('event') == 'view', col('date'))).alias('day_count')) \
               .select('ad_id', 'day_count', F.expr("clicks / views").alias('CTR')) # селектим только CTR, чтобы в итоговой выборке не дропать clicks и views
    return ctr_df

def process(spark, input_file, target_path):
    # читаем файл в DataFrame сущность
    df = spark.read.parquet(input_file)
    # выбираем нужные поля из старого df и вычисляем необходимые
    ndf = df.select(['ad_id', 
                     'target_audience_count',  
                     'has_video', 
                     (col('ad_cost_type') == 'CPM').cast('integer').alias('is_cpm'), 
                     (col('ad_cost_type') == 'CPC').cast('integer').alias('is_cpc'), 
                     'ad_cost']) \
            .join(get_ctr_df(df), 'ad_id', 'left')
    # делим df на тренирующую и тестовую выборки
    train_split(ndf, target_path) 


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