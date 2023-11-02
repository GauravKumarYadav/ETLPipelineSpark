DROP TABLE IF EXISTS streaming;
CREATE EXTERNAL TABLE IF NOT EXISTS streaming(
    first_name String,
    last_name String,
    email String,
    age int
)
STORED AS PARQUET
LOCATION 'file:///Users/gaurav/Desktop/ETLPipelineSpark/Data/temp/bronze/test.streaming';