import json

import src.field_pulse_api.get_records as api_records
from src.field_sure_database.fact_tables.stg_fact_customers import Customer
from src.field_sure_database.fact_tables.stg_fact_invoices import Invoice
from src.field_sure_database.fact_tables.stg_fact_jobs import Job
from src.field_sure_database.fact_tables.stg_fact_payments import Payment
from src.field_sure_database.fact_tables.stg_fact_purchase_orders import PurchaseOrder
from src.field_sure_database.fact_tables.stg_fact_vendors import Vendor
from src.field_sure_database.staging.fp_stg_records import fp_stg

def db(record_type: str, limit: int, max_pages: int, sort_by: str, sort_dir: str, upsert: bool, delete_staging: bool, delete_fact: bool) -> None:
    """
    Query's FieldPulse API and returns a JSON file, depending on parameters, 
    it will DELETE the '_fp_stg_' and '_stg_fact_' tables, before UPSERT'ing to them.

    :param (string) record_type: Allowed Values: 'customers', 'invoices', 'jobs', 'payments', 'purchase_orders', 'vendors'
    :param (integer) limit: Maximum 100 items allowed
    :param (integer) max_pages: Maximum number of pages to fetch
    :param (string) sort_by: Column to sort by, either 'created_at', or 'updated_at'
    :param (string) sort_dir: Direction to sort by, either 'desc' or 'asc'
    :param (bool) upsert: Whether to UPSERT the retrieved FieldPulse records
    :param (bool) delete_staging: Whether to wipe the staging table clean (for testing purposes)
    :param (bool) delete_fact: Whether to wipe the fact table clean (for testing purposes)
    """

    # Retrive data from FieldPulse API
    record_data: json = api_records.GetRecords().api_request(
        record_type=record_type.replace('_', '-'),
        limit=limit, 
        max_pages=max_pages,
        sort_by=sort_by, 
        sort_dir=sort_dir,
        print=False
    )

    # Create Instance of the Record Class
    table = fp_stg(table_name=record_type, api_data=record_data)

    if delete_staging == True:
        # FP Staging Table
        table.delete()

    if delete_fact == True:
        # Fact Tables
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

    if upsert == True:
        # FP Staging Table
        table.upsert()    

        # Fact Tables
        match record_type:
            case 'customers':
                Customer().upsert(records=table.get_all_records_json())
            case 'invoices':
                Invoice().upsert(records=table.get_all_records_json())
            case 'jobs':
                Job().upsert(records=table.get_all_records_json())
            case 'payments':
                Payment().upsert(records=table.get_all_records_json()) 
            case 'purchase_orders':
                PurchaseOrder().upsert(records=table.get_all_records_json())
            case 'vendors':
                Vendor().upsert(records=table.get_all_records_json())
            case default:
                print(f'{record_type} is not a valid value.')        

    
