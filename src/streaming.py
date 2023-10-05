from pyspark import SparkConf
from src.DataClass.streamingDataClass import StreamingDataClass1
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def read_streaming(dataclass : StreamingDataClass1):
    '''
    Read streaming data from a directory
    '''
    spark = SparkSession.builder.appName('Streaming').getOrCreate()
    streaming_data = (
        spark
        .readStream
        .format('csv')
        .option('header',True)
        .option('maxFilesPerTrigger',1)
        .schema(dataclass.schema)
        .load(dataclass.streaming_path)
    )
    
    