from src.utils.streaming import read_streaming, write_streaming_console, write_streaming_table
from src.logger import logger
from src.Dataclass.DataClasses import StreamingData

if __name__ == "__main__":
    global spark
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    dataclass = StreamingData()
    stream_data = read_streaming(dataclass)
    write_streaming_table(stream_data,"test.streaming")
    print("running after the function call")