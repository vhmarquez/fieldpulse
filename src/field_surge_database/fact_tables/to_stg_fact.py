from field_surge_database.fact_tables.stg_customer import Customer
from field_surge_database.fact_tables.stg_invoices import Invoice
from field_surge_database.fp_stg_records import records_to_stg as staging_records

def upsert(record_type: str):

    records = staging_records(table_name=record_type, api_data=None).get_all_records_json()
    if record_type == 'customers':
        Customer().record_json(records=records)

    elif record_type == 'invoices':
        Invoice().record_json(records=records)

def delete(record_type: str):

    if record_type == 'customers':
        Customer().delete()
    elif record_type == 'invoices':
        Invoice().delete()