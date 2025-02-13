from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, String, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from src.field_sure_database.connect import FieldSurgeDatabase
from src.field_sure_database.utilities.try_sessions import try_session
from src.field_sure_database.utilities.date_normalization import date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

class Customer(Base):
    __tablename__ = "_stg_customer"

    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False, nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    street_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    zip: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    customer_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    phone_2: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    fax: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(String(2040), nullable=True)
    is_active: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    custom_properties: Mapped[Optional[str]] = mapped_column(String(8000), nullable=True)
    phone_sms_enabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    phone_2_sms_enabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    def __repr__(self) -> str:
        return f"""Customer(
        customer_id={self.customer_id}
        name={self.name}
        company_name={self.company_name}
        street_address={self.street_address}
        city={self.city}
        state={self.state}
        zip={self.zip}
        customer_type_id={self.customer_type_id}
        phone={self.phone}
        phone_2={self.phone_2}
        fax={self.fax}
        email={self.email}
        notes={self.notes}
        is_active={self.is_active}
        created_ts={self.created_ts}
        custom_properties={self.custom_properties}
        phone_sms_enabled={self.phone_sms_enabled}
        phone_2_sms_enabled={self.phone_2_sms_enabled}
        )"""
    
    def delete(self):
        Base.metadata.create_all(db)

        try_session(
            session_type='delete',
            session_object=Customer
        )

    def upsert(self, records: list):
        Base.metadata.create_all(db)

        session_update_list: list = []
        for record in records:

            customer_id: int = record["import_id"]
            name: str = f'{record["first_name"]} {record["middle_name"]} {record["last_name"]}' if record["middle_name"] else f'{record["first_name"]} {record["last_name"]}'
            company_name: str = record["company_name"]
            street_address: str = f'{record["address_1"]} {record["address_2"]}' if record["address_2"] else f'{record["address_1"]}'
            city: str = record["city"]
            state: str = record["state"]
            zip: str = record["zip_code"]
            customer_type_id: int = None
            phone: str = record["phone"]
            phone_2: str = record["alt_phone"]
            fax: str = record["fax"]
            email: str = record["email"]
            notes: str = record["notes"]
            is_active: bool = None
            created_ts: datetime = date_normalization(data=record, data_key='created_at')
            custom_properties: str = None
            phone_sms_enabled: bool = record["is_phone_notification_subscribed"]
            phone_2_sms_enabled: bool = None

            record_object: object = Customer(
                customer_id = customer_id,
                name = name,
                company_name = company_name,
                street_address = street_address,
                city = city,
                state = state,
                zip = zip,
                customer_type_id = None,
                phone = phone,
                phone_2 = phone_2,
                fax = fax,
                email = email,
                notes = notes,
                is_active = None,
                created_ts = created_ts,
                custom_properties = None,
                phone_sms_enabled = phone_sms_enabled,
                phone_2_sms_enabled = None
            )

            check_local_record = try_session(session_type='get', session_object=Customer, record_id=customer_id, composite_key=None)
            if check_local_record:
                session_update_list.append({
                    "customer_id": customer_id,
                    "name": name,
                    "company_name": company_name,
                    "street_address": street_address,
                    "city": city,
                    "state": state,
                    "zip": zip,
                    "customer_type_id": customer_type_id,
                    "phone": phone,
                    "phone_2": phone_2,
                    "fax": fax,
                    "email": email,
                    "notes": notes,
                    "is_active": is_active,
                    "created_ts": created_ts,
                    "custom_properties": custom_properties,
                    "phone_sms_enabled": phone_sms_enabled,
                    "phone_2_sms_enabled": phone_2_sms_enabled
                })
            else:
                try_session(session_type='add', session_object=record_object)

        if session_update_list != []:
            try_session(
                session_type='execute', 
                session_object=Customer, 
                session_list=session_update_list
            )