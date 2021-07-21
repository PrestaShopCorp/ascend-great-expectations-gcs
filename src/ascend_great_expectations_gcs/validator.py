from great_expectations.validator.validator import Validator
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite, ExpectationConfiguration
from great_expectations.data_context import DataContext, BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from pyspark.sql import DataFrame
from typing import Callable, List
import uuid
import os


class GEValidator:
    def __init__(self, name: str, gcp_project: str, bucket: str, credentials: str = None):
        self._name = name
        self._context = self._create_data_context(gcp_project, bucket)
        self._suite = self._create_expectation_suite(self._name)
        if credentials is not None:
            self._authenticate(credentials)

    def _authenticate(self, credentials: str, file_name="/tmp/google_credentials.json"):
        with open(file_name, "w") as file:
            print(credentials, file=file)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_name

    def _create_data_context_config(self,
                                    gcp_project: str,
                                    bucket: str,
                                    expectations_prefix="expectations",
                                    validations_prefix="validations",
                                    data_docs_prefix="data_docs"
                                    ) -> DataContextConfig:

        config = DataContextConfig(
            datasources={
                "df": {
                    "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SparkDFExecutionEngine",
                        "module_name": "great_expectations.execution_engine"
                    },
                    "data_connectors": {
                        "df": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["run_id"],
                        }
                    }
                }
            },
            stores={
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "project": gcp_project,
                        "bucket": bucket,
                        "prefix": expectations_prefix,
                    },
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "project": gcp_project,
                        "bucket": bucket,
                        "prefix": validations_prefix,
                    },
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore",
                },
            },
            expectations_store_name="expectations_store",
            validations_store_name="validations_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            data_docs_sites={
                "site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "project": gcp_project,
                        "bucket": bucket,
                        "prefix": data_docs_prefix,
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                    },
                }
            },
            validation_operators={
                "action_list_operator": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "store_evaluation_params",
                            "action": {"class_name": "StoreEvaluationParametersAction"},
                        }
                    ],
                }
            },
            anonymous_usage_statistics={"enabled": False}
        )
        return config

    def _create_data_context(self, gcp_project: str, bucket: str) -> DataContext:
        config = self._create_data_context_config(gcp_project, bucket)
        context = BaseDataContext(project_config=config)
        return context

    def _create_expectation_suite(self, name: str) -> ExpectationSuite:
        suite = self._context.create_expectation_suite(
            expectation_suite_name=name, overwrite_existing=True)
        return suite

    def _create_batch_request(self, df: DataFrame, run_id: str) -> RuntimeBatchRequest:
        batch_request = RuntimeBatchRequest(
            datasource_name="df",
            data_connector_name="df",
            data_asset_name=self._name,
            runtime_parameters={
                "batch_data": df
            },
            batch_identifiers={"run_id": run_id},
        )
        return batch_request

    def add_expectations(self, f: Callable):
        self._register_expectations = f
        return self

    def _get_validator(self, runtime_batch_request: RuntimeBatchRequest) -> Validator:
        return self._context.get_validator(
            expectation_suite=self._suite,
            batch_request=runtime_batch_request,
        )

    def run(self, df: DataFrame, run_id: str = None):
        if run_id is None:
            run_id = str(uuid.uuid1())

        batch_request = self._create_batch_request(df, run_id)
        validator = self._get_validator(batch_request)

        if self._register_expectations is not None:
            self._register_expectations(validator)

        validator.save_expectation_suite(discard_failed_expectations=False)
        self._context.run_validation_operator(
            "action_list_operator", assets_to_validate=[validator], run_id=run_id)

    def build_data_docs(self):
        self._context.build_data_docs()
