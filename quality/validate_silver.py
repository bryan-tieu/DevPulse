import os
import sys

import great_expectations as gx
import pandas as pd
from great_expectations.exceptions import DataContextError

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
EXPECTED_COLUMNS = [
    "event_id",
    "event_type",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "public",
    "created_at",
]


def read_hour(date: str, hour: int) -> pd.DataFrame:
    df = pd.read_parquet(f"gs://{SILVER_BUCKET}/events/event_date={date}/event_hour={hour}/")

    return df


# bootstrap only — after first registration the stored JSON is authoritative;
# if you edit expectations here, delete the stored suite and re-run to regenerate
def build_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="devpulse_expectation_suite")

    expectation_1 = gx.expectations.ExpectTableColumnsToMatchSet(column_set=EXPECTED_COLUMNS)
    expectation_2 = gx.expectations.ExpectColumnValuesToNotBeNull(column="event_type")
    expectation_3 = gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id")

    # Spark casts malformed timestamps to NULL and rerouted to _hive_default_partition.
    # This happened before we get to validating.
    # Counting is what actually does the check at this phase.
    expectation_4 = gx.expectations.ExpectColumnValuesToNotBeNull(column="created_at")

    # mostly = 0.9999. Thousands of id's. 0.01% of 100000 is 10 which means it catches
    # anomalies not a slow work day
    expectation_5 = gx.expectations.ExpectColumnValuesToNotBeNull(column="actor_id", mostly=0.9999)
    expectation_6 = gx.expectations.ExpectColumnValuesToNotBeNull(column="repo_id", mostly=0.9999)
    expectation_7 = gx.expectations.ExpectColumnValuesToBeUnique(column="event_id")
    # (hand-set from one holiday data point + margin; derive from backfill
    # distribution later).
    expectation_8 = gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=50000, max_value=900000
    )

    suite.add_expectation(expectation=expectation_1)
    suite.add_expectation(expectation=expectation_2)
    suite.add_expectation(expectation=expectation_3)
    suite.add_expectation(expectation=expectation_4)
    suite.add_expectation(expectation=expectation_5)
    suite.add_expectation(expectation=expectation_6)
    suite.add_expectation(expectation=expectation_7)
    suite.add_expectation(expectation=expectation_8)

    return suite


def main(date: str, hour: int) -> int:
    df = read_hour(date, hour)
    print(f"Read {len(df)} rows")

    context = gx.get_context(mode="file")

    try:
        source = context.data_sources.get("silver_parquet")
    except KeyError:
        print("silver_parquet doesn't exist")
        source = context.data_sources.add_pandas(name="silver_parquet")

    try:
        asset = source.get_asset(name="silver_dataframe_asset")
    except KeyError:
        print("silver_dataframe_asset doesn't exist")
        asset = source.add_dataframe_asset(name="silver_dataframe_asset")

    try:
        batch_definition = asset.get_batch_definition(name="silver_batch_definition")
    except KeyError:
        print("silver_batch_definition doesn't exist")
        batch_definition = asset.add_batch_definition_whole_dataframe("silver_batch_definition")

    try:
        suite = context.suites.get("devpulse_expectation_suite")
    except DataContextError:
        print("devpulse_expectation_suite doesn't exist")
        suite = build_suite()
        context.suites.add(suite)

    try:
        validation_definition = context.validation_definitions.get("silver_validation_definition")
    except DataContextError:
        print("silver_validation_definition doesn't exist")
        validation_definition = context.validation_definitions.add_or_update(
            gx.ValidationDefinition(
                name="silver_validation_definition", data=batch_definition, suite=suite
            )
        )

    result = validation_definition.run(batch_parameters={"dataframe": df})
    print(f"Validation {'PASSED' if result.success else 'FAILED'}")
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], int(sys.argv[2])))
