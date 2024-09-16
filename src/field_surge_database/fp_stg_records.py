from datetime import datetime
from dateutil import parser
import json
from typing import Optional

from sqlalchemy import DateTime, String, Integer, update
from sqlalchemy.orm import Mapped, mapped_column, declarative_base, sessionmaker

from .connect import FieldSurgeDatabase

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()
Session = sessionmaker(bind=db)

def records_to_stg(table_name: str, api_data: json):
    """
    Query's FieldPulse API and returns a JSON file

    :param (string) table_name: Table suffix '_fp_stg_{table_name}
    :param (json) api_data: JSON data from field pulse to send to database
    """

    class Records(Base):
        __tablename__ = f'_fp_stg_{table_name}'

        remote_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
        remote_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
        remote_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
        remote_deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
        raw_json: Mapped[Optional[str]] = mapped_column(String, nullable=True)
        historical_id: Mapped[int] = mapped_column(Integer, nullable=True)
        local_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
        local_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

        def __repr__(self) -> str:
            return f"""{self.__tablename__}(
                remote_id={self.remote_id}
                remote_create_at={self.remote_created_at}
                remote_updated_at={self.remote_updated_at}
                remote_deleted_at={self.remote_deleted_at}
                raw_json={self.raw_json}
                historical_id={self.historical_id}
                local_created_at={self.local_created_at}
                local_updated_at={self.local_updated_at}
            )"""
        
        def delete() -> None:
            Base.metadata.create_all(db)

            try_session(
                session_type='delete',
                session_object=Records
            )


        def upsert(data: json = api_data) -> None:
            Base.metadata.create_all(db)

            session_update_list = []

            for data in data:
                remote_id: int = data['id']
                remote_created_at: datetime = date_normalization(data=data, data_key='created_at')
                remote_updated_at: datetime = date_normalization(data=data, data_key='updated_at')
                remote_deleted_at: datetime = date_normalization(data=data, data_key='deleted_at')
                raw_json: str = str(data)
                historical_id: int = set_historical_id(data=data, data_key='historical_id')
                local_record: object = try_session(session_type='get', session_object=Records, record_id=remote_id)

                if local_record != None:
                    local_updated_at: str = str(local_record.remote_updated_at)

                    if local_updated_at != remote_updated_at:
                        session_update_list.append({
                            "remote_id": remote_id, 
                            "remote_created_at": remote_created_at, 
                            "remote_updated_at": remote_updated_at, 
                            "remote_deleted_at": remote_deleted_at, 
                            "raw_json": str(raw_json), 
                            "historical_id": historical_id, 
                            "local_update_at": local_updated_at
                        })
                        
                else:
                    try_session(
                        session_type='add',
                        session_object=Records(
                            remote_id = remote_id,
                            remote_created_at = remote_created_at,
                            remote_updated_at = remote_updated_at,
                            remote_deleted_at = remote_deleted_at,
                            raw_json = str(raw_json),
                            historical_id = historical_id,
                            local_created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            local_updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                    )

            try_session(
                session_type='execute',
                session_object=Records,
                session_list=session_update_list
            )

    return Records

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

def date_normalization(data: object, data_key: str):
    """
    Normalizes dates

    :param (object) data: The data object which you're iterating through
    :param (string) data_key: The key of the data object which you're trying to normalize
    """
    if data[data_key] != None:
        return parser.parse(str(data[data_key])).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return None
    
def set_historical_id(data: object, data_key: str):
    """
    Checks if the data object has a data key, in which case, returns that data key, otherwise returns None

    :param (object) data: The data object which you're iterating through
    :param (string) data_key: The key of the data object which you're trying to return
    """
    if data_key in data: 
        return data[data_key] 
    else: 
        return None
                
if __name__ == '__main__':
    records_to_stg(tablename='customers')