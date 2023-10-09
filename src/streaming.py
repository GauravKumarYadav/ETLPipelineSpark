from src.DataClass.DataClasses import StreamingData
from pyspark.sql import SparkSession
import os

def read_streaming(dataclass ):
    '''
    Read streaming data from a directory
    '''
    spark = SparkSession.builder.appName('Streaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    streaming_data = (
        spark
        .readStream
        .format('json')
        .schema(dataclass.schema)
        .option('header',True)
        .option('maxFilesPerTrigger',1)
        .option("multiline","true")
        .load(dataclass.path)
    )
    return streaming_data

def write_streaming_console(streaming_data):
    '''
    Write streaming data to console
    '''
    spark = SparkSession.builder.appName('Streaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    query = (
        streaming_data
        .writeStream
        .format('console')
        .outputMode('append')
        .trigger(processingTime='10 seconds')
        .option('checkpointLocation','file://'+os.path.join(os.getcwd(),"Data","checkpoint"))
        .start()
    )

    query.awaitTermination()

def write_streaming_table(streaming_data,table_name:str):
    '''
    Write streaming data to table
    '''
    spark = SparkSession.builder.appName('Streaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    query = (
        streaming_data
        .writeStream
        .format('delta')
        .outputMode('append')
        .trigger(processingTime='10 seconds')
        .option('checkpointLocation','file://'+os.path.join(os.getcwd(),"Data","checkpoint"))
        .table(table_name)
    )

    query.awaitTermination()

if __name__ == "__main__":
    dataclass = StreamingData()
    stream_data = read_streaming(dataclass)
    write_streaming_console(stream_data)

