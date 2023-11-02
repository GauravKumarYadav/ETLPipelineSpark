from src.DataClass.DataClasses import BatchData
from pyspark.sql import SparkSession

def read_batch(dataclass):
    '''
    Read batch data from a directory
    '''
    batch_data = (
        spark
        .read
        .format('json')
        .schema(dataclass.schema)
        .option('header',True)
        .option('mode', 'PERMISSIVE')
        .load(dataclass.path)
    )
    batch_data = batch_data.na.drop()
    batch_data.show()


if __name__ == "__main__":
    global spark
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    data = BatchData()
    read_batch(data)