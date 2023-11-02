from pyspark.sql import DataFrame
import os
import sys 

def Converter(df : DataFrame,config,write_flag=False):
    if write_flag:    
        for source_col,modified_cols in config.items():
            if source_col in df.columns:
                df = df.withColumnRenamed(source_col,modified_cols)
            else:
                raise ValueError(f"Column '{source_col}' is not present in the DataFrame.")
    else:
        for source_col,modified_cols in config.items():
            if modified_cols in df.columns:
                df = df.withColumnRenamed(modified_cols,source_col)
            else:
                raise ValueError(f"Column '{source_col}' is not present in the DataFrame.")

    return df

def checkpointLocation(path:str = None):
    if path is None:
        path =  os.path.join(get_root_dir(),"Data","checkpoint")
        os.makedirs(path,exist_ok=True)
    else:
        path = path
        os.makedirs(path,exist_ok=True)
    return 'file://' + path

def get_root_dir() -> str:
    current_file = os.path.abspath('__file__')
    while True:
        current_dir = os.path.dirname(current_file)
        if os.path.exists(os.path.join(current_dir, "README.md")):
            return current_dir
        current_file = current_dir
