from src.DataClass.DataClasses import StreamingData
from pyspark.sql import SparkSession
import os

from src.utils.utils import checkpointLocation

def read_streaming(dataclass):
    '''
    Read streaming data from a directory
    '''
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
    query = (
        streaming_data
        .writeStream
        .format('console')
        .outputMode('append')
        .trigger(processingTime='10 seconds')
        .option('checkpointLocation',checkpointLocation())
        .start()
    )
    query.awaitTermination()


def write_streaming_table(streaming_data,table_name:str,layer:str = "bronze",path:str = None):
    '''
    Write streaming data to table
    '''
    if path is None:
        temp_path =  os.path.join(os.getcwd(),"Data","temp",layer,table_name)
        os.makedirs(temp_path,exist_ok=True)
    else:
        temp_path = path
        os.makedirs(temp_path,exist_ok=True)
    
    temp_path = 'file://'+ temp_path
    query = (
        streaming_data
        .writeStream
        .outputMode('append')
        .format('parquet')
        .trigger(processingTime='10 seconds')
        .option('checkpointLocation',checkpointLocation())
        .option('path',temp_path)
        .start()
    )
    query.awaitTermination()

