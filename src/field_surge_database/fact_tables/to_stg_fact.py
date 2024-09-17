from field_surge_database.fact_tables.stg_customer import Customer
from field_surge_database.fp_stg_records import records_to_stg as staging_records

def fp_stg_to_stg_fact(table_name: str):
    
    Customer().record_json(records=staging_records(table_name=table_name, api_data=None).get_all_records())
