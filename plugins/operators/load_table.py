from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd


class loadTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 spark,
                 df,
                 directory,
                 table_name,
                 *args, **kwargs):
        super(loadTableOperator, self).__init__(*args, **kwargs)
        self.df = df
        self.spark = spark
        self.directory = directory
        self.table_name = table_name

    def execute(self, context):
        sdf = self.spark.createDataFrame(self.df)
        sdf.write.mode('overwrite').parquet(self.directory + self.table_name)
