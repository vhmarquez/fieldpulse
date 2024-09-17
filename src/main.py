from field_surge_database import fp_to_db
from field_surge_database.fact_tables import to_stg_fact

def main():
    # Get Data from FieldPulse
    # fp_to_db.delete(record_type='customers')
    # fp_to_db.delete(record_type='invoices')
    # fp_to_db.delete(record_type='jobs')
    # fp_to_db.delete(record_type='payments')
    # fp_to_db.delete(record_type='purchase-orders')
    # fp_to_db.delete(record_type='vendors')

    # Create FieldPulse Staging Tables
    # fp_to_db.upsert(record_type='customers', limit=10, max_pages=2)
    # fp_to_db.upsert(record_type='invoices', limit=10, max_pages=2)
    # fp_to_db.upsert(record_type='jobs', limit=10, max_pages=2)
    # fp_to_db.upsert(record_type='payments', limit=10, max_pages=2)
    # fp_to_db.upsert(record_type='purchase-orders', limit=10, max_pages=2)
    # fp_to_db.upsert(record_type='vendors', limit=10, max_pages=2)

    # Create Staging for Fact tables
    to_stg_fact.fp_stg_to_stg_fact(table_name='customers')

if __name__ == '__main__':
    main()