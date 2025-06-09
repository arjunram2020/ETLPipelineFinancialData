from ruamel import yaml
import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context.data_context import EphemeralDataContext

import pandas as pd

context = gx.get_context()
datasource_config = {
    "name": "my_snowflake_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "snowflake://<ARJUNR9206>:<College11!!!!!>@<NN16547>/<ARJUN_TEST>/<PUBLIC>?warehouse=<COMPUTE_WH>&role=<ACCOUNTADMIN>",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
    },
}
context.test_yaml(datasource_config)
context.add_datasource(**datasource_config)


batch_request = BatchRequest(
    datasource_name="my_snowflake_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name=f"{sfSchema.lower()}.NYSE_SHORTENED",  # this is the name of the table you want to retrieve
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite")
