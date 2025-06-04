import great_expectations as gx
from great_expectations import expectations as gxe
context = gx.get_context()
datasource_name = "my_datasource"
my_connection_string = "snowflake://<arjunr9206>:<College11!!!!!>@<mizrudp-nn16547>/<arjun_test>/<information_schema>?warehouse=<public>&role=<accountadmin>&application=great_expectations_oss"
data_source = context.data_sources.add_snowflake (name=datasource_name, connection_string=my_connection_string)
preset_expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="symbol", min_value=1, max_value=6
)
