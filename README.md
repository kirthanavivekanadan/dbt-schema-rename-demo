# DBT + Snowflake: Metadata-Driven Schema Rename Demo

This project demonstrates how to dynamically handle column renames in DBT models using metadata-driven macros and a Snowflake metadata table.

---

## Project Overview

- **Simulated data** for customers, products, and orders generated via Python and loaded into Snowflake using internal stages.
- DBT **staging models** (`stg_customers`, `stg_orders`, `stg_products`) built on raw Snowflake tables.
- DBT **core models** (`dim_customers`, `fact_orders`, `dim_products`) created from staging models.
- A DBT **macro** `rename_and_rebuild` that:
  - Reads rename instructions from a metadata table (`raw.schema_change_metadata`),
  - Dynamically recreates views with renamed columns,
  - Updates metadata flags to track processed schema changes.

---

## Sample Data Generation (Python)

``python
customers_data = {
    "customer_id": [1, 2, 3],
    "first_name": ["Alice", "Bob", "Charlie"],
    "last_name": ["Smith", "Jones", "Brown"],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "signup_date": ["2021-01-01", "2021-02-15", "2021-03-22"]
} ``

# Similar data for products and orders...

# Export to CSV files and upload to Snowflake internal stages


## Metadata Table Example
This table tracks schema changes:
INSERT INTO raw.schema_change_metadata (
    existing_model_name,
    existing_column_name,
    new_column_name,
    existing_data_type,
    new_data_type,
    update_flag
)
VALUES (
    'STG_CUSTOMERS', 
    'email', 
    'email_address', 
    'STRING', 
    'STRING', 
    'ACTIVE'
);

##Macro Usage
Run the macro to apply the rename and rebuild views:
dbt run-operation rename_and_rebuild --args '{"model_name": "raw.dim_customers", "source_table": "raw.customer"}'


##How It Works
- The macro queries the metadata table for any columns marked ACTIVE to rename.

- It then describes the source table to get the current columns.

- For each column, if a rename is needed, it applies AS new_column_name.

- The macro recreates the DBT view with updated column names.

- Finally, the metadata can be updated to mark changes as handled.


##Future Enhancements
- Handling column type changes

- Detecting added or dropped columns

- Automating metadata updates after macro execution
