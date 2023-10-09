from pyspark.sql import DataFrame

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
