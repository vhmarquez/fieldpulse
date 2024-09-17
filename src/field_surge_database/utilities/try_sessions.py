from sqlalchemy import update
from sqlalchemy.orm import sessionmaker

from ..connect import FieldSurgeDatabase

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Session = sessionmaker(bind=db)

def try_session(session_type: str, session_object: object, **kwargs):
    """
    Attempts to create a session, commits the session if successful, or rollsback

    :param (string) session_type: Accepts, 'get', 'add', 'execute', or 'delete'
    :param (object) session_object: The object to pass to the session
    :param (string) record_id: **kwargs, the record id that you're trying to fetch, used with session type 'get'
    :param (string) session_list: **kwargs, the list of records that you're trying to update, used with session type 'execute'
    """
    with Session() as session:
        try:
            if session_type == 'get':
                retrieved_record = session.get(session_object, kwargs['record_id'])
                
                if retrieved_record != None:
                    return retrieved_record
                else:
                    return None
            
            if session_type == 'add':
                session.add(session_object)
                session.commit()
                record_id: str = str(session_object.remote_id)
                print(f'Record: {record_id} created')

            if session_type == 'execute':
                session.execute(
                    update(session_object), 
                    kwargs['session_list']
                )
                session.commit()
                
                for list_item in kwargs['session_list']:
                    list_item_id: str = str(list_item['id'])
                    print(f'Record: {list_item_id} was updated')

            if session_type == 'delete':
                session.query(session_object).delete()
                session.commit()
                table_name: str = str(session_object().__tablename__)
                print(f'Table: {table_name} deleted')

        except Exception:
            session.rollback()
            print(f'{session_type} session, failed.')
            print(session_object, kwargs)

        finally:
            session.close()