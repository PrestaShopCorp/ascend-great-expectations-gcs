import os
import datetime
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig

class Validator:
    def __init__(self,name: str ="name", gcp_project:str ="project",bucket: str="my_bucket", credentials: str="{{}}",asset_name:str="asset_name",temp_table_name:str="ge_temp"):
        self._name = name
        self._gcp_project = gcp_project
        self._temp_table_name = temp_table_name
        self._asset_name = asset_name
        self._context = self._create_data_context(bucket)
        self._authenticate(credentials)

    def _authenticate(self, credentials: str, file_name="/tmp/google_credentials.json"):
        with open(file_name, "w") as file:
            print(credentials, file=file)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_name

    def _create_data_context_config(self,
                                    bucket: str,
                                    expectations_prefix="expectations",
                                    validations_prefix="validations",
                                    data_docs_prefix="data_docs"
                                    ) -> DataContextConfig:
        
        data_context_config = DataContextConfig(
            stores={
                "expectations_GCS_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "project": self._gcp_project,
                        "bucket": bucket,
                        "prefix": expectations_prefix,
                    },
                },
                "validations_GCS_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "project": self._gcp_project,
                        "bucket": bucket,
                        "prefix": validations_prefix,
                    },
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore",
                },
            },
            expectations_store_name="expectations_GCS_store",
            validations_store_name="validations_GCS_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            data_docs_sites={
                "gcs_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleGCSStoreBackend",
                        "bucket": bucket,
                        "prefix": data_docs_prefix,
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                        "show_cta_footer": True,
                        "validation_results_limit": 100,
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
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ],
                }
            },
            anonymous_usage_statistics={"enabled": False}
        )
        return data_context_config

    def _get_datasource_config(self):
        datasource_config = {
            "name": "df",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine",
                "module_name": "great_expectations.execution_engine"
            },
            "data_connectors": {
                "df": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["run_id","pipeline_stage"],
                }
            }
        }
        return datasource_config

    def _create_data_context(self, bucket: str):
        config = self._create_data_context_config(bucket)
        context = BaseDataContext(project_config=config)
        datasource = self._get_datasource_config()
        context.add_datasource(**datasource)
        return context

    def _create_batch_request(self, df, run_id):
            batch_request = RuntimeBatchRequest(
                datasource_name="df",
                data_connector_name="df",
                data_asset_name=self._asset_name,
                runtime_parameters={
                    "batch_data": df
                },
                batch_identifiers= {"pipeline_stage": "staging", "run_id": run_id},
                batch_spec_passthrough={
                    "bigquery_temp_table": self._temp_table_name
                }
            )
            return batch_request

    def add_expectations(self, f):
        self._register_expectations = f
        return self

    def _get_validator(self,suite,runtime_batch_request):
        return self._context.get_validator(
            batch_request=runtime_batch_request,
            expectation_suite=suite
        )
    
    def run(self, df, run_id=None):
        if run_id is None:
            run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

        self._suite = self._context.create_expectation_suite(
            expectation_suite_name=self._name, overwrite_existing=True
        )
        self._runtime_batch_request = self._create_batch_request(df,run_id)
        validator = self._get_validator(self._suite,self._runtime_batch_request)

        if self._register_expectations is not None:
            self._register_expectations(validator)

    def build_data_docs(self):
        self._context.build_data_docs()
