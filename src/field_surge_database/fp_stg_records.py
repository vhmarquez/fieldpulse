from datetime import datetime
import json
from typing import Optional

from sqlalchemy import DateTime, String, Integer, update
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from .connect import FieldSurgeDatabase
from .utilities import try_session, date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

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