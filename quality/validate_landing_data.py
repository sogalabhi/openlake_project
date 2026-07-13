import os
import sys
import pandas as pd
import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeOfType,
    ExpectColumnValuesToBeBetween
)

def run_validation():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, "data", "online_retail_II.csv")

    if not os.path.exists(csv_path):
        print(f"Error: Raw CSV not found at {csv_path}")
        sys.exit(1)

    print(f"Loading data for validation from: {csv_path}")
    df_raw = pd.read_csv(csv_path)

    context = gx.get_context(mode="ephemeral")
    
    ds = context.data_sources.add_pandas("raw_retail_datasource")
    asset = ds.add_dataframe_asset("retail_csv_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("raw_batch_definition")

    suite = context.suites.add(gx.ExpectationSuite(name="retail_raw_expectations"))
    
    print("\nRegistering expectation assertions...")
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="Invoice"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="StockCode"))
    suite.add_expectation(ExpectColumnValuesToBeOfType(column="Quantity", type_="int64"))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="Price", min_value=0.0, mostly=0.999))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="Customer ID", mostly=0.7))

    validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="retail_landing_validation",
            data=batch_def,
            suite=suite
        )
    )

    print("\nRunning expectation suite tests...")
    result = validation.run(batch_parameters={"dataframe": df_raw})

    print("\n================== DATA QUALITY REPORT ==================")
    all_passed = result.success
    
    for run_result in result.results:
        test_name = run_result.expectation.expectation_type
        column = run_result.expectation.column
        passed = run_result.success
        status = "PASSED" if passed else "FAILED"
        print(f"[{status}] {test_name} on column: '{column}'")
    print("=========================================================")

    if all_passed:
        print("Success: All data quality checks passed!")
        sys.exit(0)
    else:
        print("Error: One or more data quality checks failed.")
        sys.exit(1)

if __name__ == "__main__":
    run_validation()
