from field_sure_database.upsert import db

def main():

    db(record_type='customers', limit=5, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='invoices', limit=5, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='jobs', limit=5, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='payments', limit=5, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='purchase_orders', limit=5, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='vendors', limit=5, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)

if __name__ == '__main__':
    main()