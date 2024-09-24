from src.field_sure_database.upsert import db
from src.field_sure_database.connect import FieldSurgeDatabase

def main():

    db(record_type='customers', limit=50, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='invoices', limit=50, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='jobs', limit=50, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='payments', limit=50, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='purchase_orders', limit=50, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)
    db(record_type='vendors', limit=50, max_pages=1, sort_by='updated_at', sort_dir='desc', upsert=True, delete_staging=False, delete_fact=False)

if __name__ == '__main__':
    main()