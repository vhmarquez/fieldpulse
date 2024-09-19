from field_surge_database import fp_to_db
from field_surge_database.fact_tables import to_stg_fact

def main():

    # ================================================
    # FP Staging Tables
    # ================================================

    # Delete data from _fp_stg tables
    # ================================================
    
    # fp_to_db.delete(record_type='customers')
    # fp_to_db.delete(record_type='invoices')
    # fp_to_db.delete(record_type='jobs')
    # fp_to_db.delete(record_type='payments')
    # fp_to_db.delete(record_type='purchase-orders')
    # fp_to_db.delete(record_type='vendors')

    
    # Create _fp_stg tables
    # ================================================

    # fp_to_db.upsert(record_type='customers', limit=100, max_pages=1)
    # fp_to_db.upsert(record_type='invoices', limit=100, max_pages=1)
    # fp_to_db.upsert(record_type='jobs', limit=100, max_pages=1)
    # fp_to_db.upsert(record_type='payments', limit=100, max_pages=1)
    # fp_to_db.upsert(record_type='purchase-orders', limit=100, max_pages=1)
    # fp_to_db.upsert(record_type='vendors', limit=100, max_pages=1)

    # ================================================
    # Fact Tables
    # ================================================

    # Delete data from _stg_factTables
    # ================================================
    
    # Delete data from _stg_fact tables
    # to_stg_fact.delete(record_type='customers')
    # to_stg_fact.delete(record_type='invoices')
    # # to_stg_fact.delete(record_type='jobs')
    # to_stg_fact.delete(record_type='payments')
    # to_stg_fact.delete(record_type='purchase-orders')
    # to_stg_fact.delete(record_type='vendors')

    # Create Fact Tables
    # ================================================

    # Create Staging for Fact tables
    # to_stg_fact.upsert(record_type='customers')
    # to_stg_fact.upsert(record_type='invoices')
    # to_stg_fact.upsert(record_type='jobs')
    to_stg_fact.upsert(record_type='payments')
    # to_stg_fact.upsert(record_type='purchase-orders')
    # to_stg_fact.upsert(record_type='vendors')

if __name__ == '__main__':
    main()