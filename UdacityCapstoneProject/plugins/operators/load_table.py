from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd


class loadTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 key_schema,
                 attribute_definitions,
                 table_name,
                 df,
                 client,
                 columns,
                 *args, **kwargs):
        super(loadTableOperator, self).__init__(*args, **kwargs)
        self.client = client
        self.key_schema = key_schema
        self.attribute_definitions = attribute_definitions
        self.table_name = table_name
        self.df = df
        self.columns = columns

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
