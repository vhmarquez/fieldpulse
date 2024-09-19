from datetime import datetime
from dateutil import parser
import json
from typing import Optional

from sqlalchemy import DateTime, String, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from field_surge_database.connect import FieldSurgeDatabase
from field_surge_database.utilities.try_sessions import try_session
from field_surge_database.utilities.date_normalization import date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

class Vendor(Base):
    __tablename__ = "_stg_vendor"

    vendor_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    street_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    zip: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    vendor_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    phone_2: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    fax: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(String(510), nullable=True)
    is_active: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    vendor_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"""Vendor(
        vendor_id={self.id}
        name={self.name}
        company_name={self.company_name}
        street_address={self.street_address}
        city={self.city}
        state={self.state}
        zip={self.zip}
        vendor_type_id={self.vendor_type_id}
        phone={self.phone}
        phone_2={self.phone_2}
        fax={self.fax}
        email={self.email}
        notes={self.notes}
        is_active={self.is_active}
        vendor_created_ts={self.vendor_created_ts}
        )"""
    
    def delete(self):
        Base.metadata.create_all(db)

        try_session(
            session_type='delete',
            session_object=Vendor
        )

    def upsert(self, records: list):
        Base.metadata.create_all(db)

        session_update_list: list = []
        for record in records:

            vendor_id: int = record['id']
            name: str = None
            company_name: str = record['name']
            street_address: str = None
            city: str = None
            state: str = None
            zip: str = None
            vendor_type_id: int = None
            phone: str = None
            phone_2: str = None
            fax: str = None
            email: str = record['email']
            notes: str = None
            is_active: bool = None
            vendor_created_ts: datetime = date_normalization(data=record, data_key='created_at')

            record_object: object = Vendor(
                vendor_id = vendor_id,
                name = name,
                company_name = company_name,
                street_address = street_address,
                city = city,
                state = state,
                zip = zip,
                vendor_type_id = vendor_type_id,
                phone = phone,
                phone_2 = phone_2,
                fax = fax,
                email = email,
                notes = notes,
                is_active = is_active,
                vendor_created_ts = vendor_created_ts
            )

            check_local_record = try_session(session_type='get', session_object=Vendor, record_id=vendor_id, composite_key=None)
            if check_local_record:
                session_update_list.append({
                    "vendor_id": vendor_id,
                    "name": name,
                    "company_name": company_name,
                    "street_address": street_address,
                    "city": city,
                    "state": state,
                    "zip": zip,
                    "vendor_type_id": vendor_type_id,
                    "phone": phone,
                    "phone_2": phone_2,
                    "fax": fax,
                    "email": email,
                    "notes": notes,
                    "is_active": is_active,
                    "vendor_created_ts": vendor_created_ts
                })
            else:
                try_session(session_type='add', session_object=record_object)

        if session_update_list != []:
            try_session(
                session_type='execute', 
                session_object=Vendor, 
                session_list=session_update_list
            )