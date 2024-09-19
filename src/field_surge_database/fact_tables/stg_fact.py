from field_surge_database.fact_tables.stg_fact_customers import Customer
from field_surge_database.fact_tables.stg_fact_invoices import Invoice
from field_surge_database.fact_tables.stg_fact_jobs import Job
from field_surge_database.fact_tables.stg_fact_payments import Payment
from field_surge_database.fact_tables.stg_fact_purchase_orders import PurchaseOrder
from field_surge_database.fact_tables.stg_fact_vendors import Vendor
from field_surge_database.fp_stg_records import records_to_stg as staging_records

def upsert(record_type: str):
    record_type: str = record_type.replace('-', '_')
    records = staging_records(table_name=record_type, api_data=None).get_all_records_json()

    match record_type:
        case 'customers':
            Customer().upsert(records=records)
        case 'invoices':
            Invoice().upsert(records=records)
        case 'jobs':
            Job().upsert(records=records)
        case 'payments':
            Payment().upsert(records=records) 
        case 'purchase_orders':
            PurchaseOrder().upsert(records=records)
        case 'vendors':
            Vendor().upsert(records=records)
        case default:
            print(f'{record_type} is not a valid value.')        

def delete(record_type: str):
    record_type: str = record_type.replace('-', '_')

    match record_type:
        case 'customers':
            Customer().delete()
        case 'invoices':
            Invoice().delete()
        case 'jobs':
            Job().delete()
        case 'payments':
            Payment().delete()
        case 'purchase_orders':
            PurchaseOrder().delete()
        case 'vendors':
            Vendor().delete()
        case default:
            print(f'{record_type} is not a valid value.')