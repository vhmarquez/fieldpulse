from field_surge_database.fact_tables.stg_customers import Customer
from field_surge_database.fact_tables.stg_invoices import Invoice
from field_surge_database.fact_tables.stg_jobs import Job
from field_surge_database.fact_tables.stg_payments import Payment
from field_surge_database.fact_tables.stg_purchase_orders import PurchaseOrder
from field_surge_database.fact_tables.stg_vendors import Vendor
from field_surge_database.fp_stg_records import records_to_stg as staging_records

def upsert(record_type: str):
    record_type: str = record_type.replace('-', '_')
    records = staging_records(table_name=record_type, api_data=None).get_all_records_json()

    if record_type == 'customers':
        Customer().record_json(records=records)

    elif record_type == 'invoices':
        Invoice().record_json(records=records)

    elif record_type == 'jobs':
        Job().record_json(records=records)

    elif record_type == 'payments':
        Payment().record_json(records=records) 

    elif record_type == 'purchase_orders':
        PurchaseOrder().record_json(records=records)
    
    elif record_type == 'vendors':
        Vendor().record_json(records=records)

def delete(record_type: str):
    record_type: str = record_type.replace('-', '_')

    if record_type == 'customers':
        Customer().delete()
    elif record_type == 'invoices':
        Invoice().delete()
    elif record_type == 'jobs':
        Job().delete()
    elif record_type == 'payments':
        Payment().delete()
    elif record_type == 'purchase_orders':
        Payment().delete()
    elif record_type == 'vendors':
        Vendor().delete()