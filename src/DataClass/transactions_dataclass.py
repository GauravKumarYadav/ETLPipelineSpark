from dataclasses import dataclass , field
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os 

@dataclass(frozen=True)
class transactions_historical:
    path: str = "file://"+os.path.join(os.getcwd(),'Data','stream',"*")
    schema : StructType = field(default_factory = lambda : StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True)
    ]))
    