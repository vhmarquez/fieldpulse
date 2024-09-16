from field_surge_database import fp_to_db

def main():

    # fp_to_db.delete(record_type='customers')
    # fp_to_db.delete(record_type='invoices')
    # fp_to_db.delete(record_type='jobs')
    # fp_to_db.delete(record_type='payments')
    # fp_to_db.delete(record_type='purchase-orders')
    # fp_to_db.delete(record_type='vendors')

    fp_to_db.upsert(record_type='customers', limit=10, max_pages=2)
    fp_to_db.upsert(record_type='invoices', limit=10, max_pages=2)
    fp_to_db.upsert(record_type='jobs', limit=10, max_pages=2)
    fp_to_db.upsert(record_type='payments', limit=10, max_pages=2)
    fp_to_db.upsert(record_type='purchase-orders', limit=10, max_pages=2)
    fp_to_db.upsert(record_type='vendors', limit=10, max_pages=2)

if __name__ == '__main__':
    main()