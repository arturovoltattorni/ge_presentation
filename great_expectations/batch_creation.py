import great_expectations as ge
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest

context = ge.get_context()

batch_request = RuntimeBatchRequest(
    datasource_name="presentation_datasource",
    data_connector_name="presentation_data_connector",
    data_asset_name="presentation_asset",
    runtime_parameters={"path": "gs://ge_data_ge-presentation/ge_data/yellow_tripdata_sample_2019-01.csv"},  # Add your GCS path here.
    batch_identifiers={"identifier_name": "yellow_tripdata"},
)

context.create_expectation_suite(
    expectation_suite_name="presentation_suite", overwrite_existing=True
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="presentation_suite"
)

print(validator.head())

validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint_name = "presentation_checkpoint"
checkpoint_config = f"""
name: {checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-presentation"
validations:
  - batch_request:
      datasource_name: presentation_datasource
      data_connector_name: presentation_data_connector
      data_asset_name: presentation_asset
    expectation_suite_name: presentation_suite
"""

context.add_checkpoint(**yaml.load(checkpoint_config))

checkpoint_result = context.run_checkpoint(
    checkpoint_name=checkpoint_name,
)
