import json
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, String, Integer
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from src.field_sure_database.connect import FieldSurgeDatabase
from src.field_sure_database.utilities import try_session, date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

def fp_stg(table_name: str, api_data: json):
    """
    Query's FieldPulse API and returns a JSON file

    :param (string) table_name: Table suffix '_fp_stg_{table_name}
    :param (json) api_data: JSON data from field pulse to send to database
    """

    class Records(Base):
        __tablename__ = f'_fp_stg_{table_name}'
        __table_args__ = {'extend_existing': True}

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

        def get_all_records_json() -> list:
            Base.metadata.create_all(db)

            json_records: list = []
            records: object = try_session(session_type='get_all', session_object=Records)
            for record in records:
                json_records.append(json.loads(record.raw_json))

            return json_records

        def upsert(data: json = api_data) -> None:
            Base.metadata.create_all(db)

            session_update_list: list = []

            for record in data:
                remote_id: int = record['id']
                remote_created_at: datetime = date_normalization(data=record, data_key='created_at')
                remote_updated_at: datetime = date_normalization(data=record, data_key='updated_at')
                remote_deleted_at: datetime = date_normalization(data=record, data_key='deleted_at')
                raw_json: json = json.dumps(record)
                if table_name == 'payments':
                    historical_id: int = set_historical_id(data=record, data_key='invoice', child_key='import_id')
                else:
                    historical_id: int = set_historical_id(data=record, data_key='import_id')

                local_record: object = try_session(session_type='get', session_object=Records, record_id=remote_id, composite_key=None)

                if local_record != None:
                    local_updated_at: str = str(local_record.remote_updated_at)

                    if local_updated_at < remote_updated_at:
                        session_update_list.append({
                            "remote_id": remote_id, 
                            "remote_created_at": remote_created_at, 
                            "remote_updated_at": remote_updated_at, 
                            "remote_deleted_at": remote_deleted_at, 
                            "raw_json": raw_json, 
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
                            raw_json = raw_json,
                            historical_id = historical_id,
                            local_created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            local_updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                    )

            if session_update_list != []:
                try_session(
                    session_type='execute',
                    session_object=Records,
                    session_list=session_update_list
                )

    return Records
    
def set_historical_id(data: object, data_key: str, child_key: str = ''):
    """
    Checks if the data object has a data key, in which case, returns that data key, otherwise returns None

    :param (object) data: The data object which you're iterating through
    :param (string) data_key: The key of the data object which you're trying to return
    :param (string) [Optional] child_key: The child key of the data_key
    """
    if data_key in data:
        if child_key:
            print(data[data_key]['import_id'])
        else:
            return data[data_key]
    else: 
        return None