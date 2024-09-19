from datetime import datetime
import json
from typing import Optional

from sqlalchemy import DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from field_surge_database.connect import FieldSurgeDatabase
from field_surge_database.utilities.try_sessions import try_session
from field_surge_database.utilities.date_normalization import date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

class Job(Base):
    __tablename__ = "_stg_fact_job"

    job_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    site_id: Mapped[int] = mapped_column(Integer, autoincrement=False)
    job_type_id: Mapped[int] = mapped_column(Integer, autoincrement=False)
    trade_type_id: Mapped[int] = mapped_column(Integer, autoincrement=False)
    job_source_type_id: Mapped[int] = mapped_column(Integer, autoincrement=False)
    job_status_type_id: Mapped[int] = mapped_column(Integer, autoincrement=False)
    job_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    job_received_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    job_scheduled_start_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    job_scheduled_end_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    job_original_scheduled_start_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"""Job(
        job_id={self.job_id}
        site_id={self.site_id}
        job_type_id={self.job_type_id}
        trade_type_id={self.trade_type_id}
        job_source_type_id={self.job_source_type_id}
        job_status_type_id={self.job_status_type_id}
        job_created_ts={self.job_created_ts}
        job_received_ts={self.job_received_ts}
        job_scheduled_start_ts={self.job_scheduled_start_ts}
        job_scheduled_end_ts={self.job_scheduled_end_ts}
        job_original_scheduled_start_ts={self.job_original_scheduled_start_ts}
        )"""
    
    def delete(self):
        Base.metadata.create_all(db)

        try_session(
            session_type='delete',
            session_object=Job
        )

    def record_json(self, records: list):
        Base.metadata.create_all(db)

        session_update_list: list = []
        for record in records:

            job_id: int = record['import_id']
            site_id: int = None
            job_type_id: int = None
            trade_type_id: int = None
            job_source_type_id: int = None
            job_status_type_id: int = record['status_id']
            job_created_ts: datetime = date_normalization(data=record, data_key='created_at')
            job_received_ts: datetime = None
            job_scheduled_start_ts: datetime = date_normalization(data=record, data_key='start_time')
            job_scheduled_end_ts: datetime = date_normalization(data=record, data_key='end_time')
            job_original_scheduled_start_ts: datetime = None

            record_object: object = Job(
                job_id = job_id,
                site_id = site_id,
                job_type_id = job_type_id,
                trade_type_id = trade_type_id,
                job_source_type_id = job_source_type_id,
                job_status_type_id = job_status_type_id,
                job_created_ts = job_created_ts,
                job_received_ts = job_received_ts,
                job_scheduled_start_ts = job_scheduled_start_ts,
                job_scheduled_end_ts = job_scheduled_end_ts,
                job_original_scheduled_start_ts = job_original_scheduled_start_ts
            )

            check_local_record = try_session(session_type='get', session_object=Job, record_id=job_id, composite_key=None)
            if check_local_record:
                session_update_list.append({
                    "job_id": job_id,
                    "site_id": site_id,
                    "job_type_id": job_type_id,
                    "trade_type_id": trade_type_id,
                    "job_source_type_id": job_source_type_id,
                    "job_status_type_id": job_status_type_id,
                    "job_created_ts": job_created_ts,
                    "job_received_ts": job_received_ts,
                    "job_scheduled_start_ts": job_scheduled_start_ts,
                    "job_scheduled_end_ts": job_scheduled_end_ts,
                    "job_original_scheduled_start_ts": job_original_scheduled_start_ts
                })
            else:
                try_session(session_type='add', session_object=record_object)

        if session_update_list != []:
            try_session(
                session_type='execute', 
                session_object=Job, 
                session_list=session_update_list
            )