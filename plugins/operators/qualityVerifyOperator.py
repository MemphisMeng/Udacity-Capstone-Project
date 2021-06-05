from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 spark,
                 directory,
                 table
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.spark = spark
        self.directory = directory
        self.table = table

    def execute(self, context):
        self.log.info(f"Starting data quality validation on table : {table}")
        df = self.spark.read.parquet(self.directory + self.table).toPandas()

        if df.shape[0] < 1 or df.shape[1] < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = df.iloc[0, 0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {num_records} records")    