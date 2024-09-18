import json

import field_pulse_api.get_records as api_records
import field_surge_database.fp_stg_records as db_records

def upsert(record_type: str, limit: int, max_pages: int) -> None:
    """
    Query's FieldPulse API and returns a JSON file, and UPSERTS to Database

    :param (string) record_type: Allowed values: customers, invoices, jobs, payments, purchase-orders, vendors
    :param (integer) limit: Maximum 100 items allowed
    :param (integer) max_pages: Maximum number of pages to fetch
    """
    table_name: str = record_type.replace('-', '_')

    record_data: json = api_records.GetRecords().api_request(
        record_type=record_type, 
        limit=limit, 
        max_pages=max_pages,
        sort_by='updated_at', 
        sort_dir='desc',
        print=False
    )

    db_records.records_to_stg(
        table_name=table_name,
        api_data=record_data
    ).upsert()

def delete(record_type: str) -> None:
    record_type: str = record_type.replace('-', '_')

    db_records.records_to_stg(table_name=record_type, api_data=None).delete()