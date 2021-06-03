from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd


class pandas2SparkOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 df,
                 spark,
                 *args, **kwargs):
        super(pandas2SparkOperator, self).__init__(*args, **kwargs)
        self.df = df
        self.spark = spark

    def execute(self, context):
        table = self.client.create_table(
            TableName=self.table_name,
            KeySchema=self.key_schema,
            AttributeDefinitions=self.attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        for _, row in self.df.iterrows():
            chunk = dict(zip(self.columns, row))
            table.put_item(Item=chunk)
