from field_surge_database.staging import fp_stg
from field_surge_database.fact_tables import stg_fact

def main():

    # ================================================
    # FP Staging Tables
    # ================================================

    # Delete data from _fp_stg tables
    # ================================================
    
    # fp_stg.delete(record_type='customers')
    # fp_stg.delete(record_type='invoices')
    # fp_stg.delete(record_type='jobs')
    # fp_stg.delete(record_type='payments')
    # fp_stg.delete(record_type='purchase-orders')
    # fp_stg.delete(record_type='vendors')

    
    # Create _fp_stg tables
    # ================================================

    # fp_stg.upsert(record_type='customers', limit=100, max_pages=1)
    # fp_stg.upsert(record_type='invoices', limit=100, max_pages=1)
    # fp_stg.upsert(record_type='jobs', limit=100, max_pages=1)
    # fp_stg.upsert(record_type='payments', limit=100, max_pages=1)
    # fp_stg.upsert(record_type='purchase-orders', limit=100, max_pages=1)
    fp_stg.upsert(record_type='vendors', limit=100, max_pages=1)

    # ================================================
    # Fact Tables
    # ================================================

    # Delete data from _stg_factTables
    # ================================================
    
    # Delete data from _stg_fact tables
    # stg_fact.delete(record_type='customers')
    # stg_fact.delete(record_type='invoices')
    # stg_fact.delete(record_type='jobs')
    # stg_fact.delete(record_type='payments')
    # stg_fact.delete(record_type='purchase-orders')
    # stg_fact.delete(record_type='vendors')

    # Create Fact Tables
    # ================================================

    # Create Staging for Fact tables
    # stg_fact.upsert(record_type='customers')
    # stg_fact.upsert(record_type='invoices')
    # stg_fact.upsert(record_type='jobs')
    # stg_fact.upsert(record_type='payments')
    # stg_fact.upsert(record_type='purchase-orders')
    # stg_fact.upsert(record_type='vendors')

if __name__ == '__main__':
    main()