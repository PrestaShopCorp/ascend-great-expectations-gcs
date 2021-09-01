import os
import time
import pandas as pd
from pyspark.sql import SparkSession
from ascend_great_expectations_gcs.validator import GEValidator

os.environ["DATABRICKS_RUNTIME_VERSION"] = "ASCEND"


class TestValidator:
    def __init__(self, credentials: str = None):
        if credentials is None:
            raise ValueError("Credentials not found.")
        self.credentials = credentials

   
    def make_validator(self) -> GEValidator:
        validator = GEValidator(name="demo", credentials=self.credentials,
                                credentials_file_name="/tmp/google_credentials.json")
        return validator

    def make_df(self):
        spark = SparkSession.builder.appName("demo").getOrCreate()
        df = pd.DataFrame(
            {'language': ["Java", "Python", "Scala"], 'users_count': [20000, 100000, 3000]})
        return spark.createDataFrame(df)

    def make_expectations(self):
        def register_expectations(validator):
            validator.expect_column_values_to_not_be_null("language")
        return register_expectations

    def test_run_checkpoint(self, slack_webhook= ""):
        validator = self.make_validator()

        expectations = self.make_expectations()
        validator.add_expectations(expectations)

        df = self.make_df()
        validator.run_checkpoint(df, slack_webhook=slack_webhook)

    def test_run(self):
        validator = self.make_validator()

        # expectations = self.make_expectations()
        # validator.add_expectations(expectations)

        # df = self.make_df()
        # validator.run(df)

    def test_build_data_docs(self):
        validator = self.make_validator()
        validator.build_data_docs()
