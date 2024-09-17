from datetime import datetime
import json
from typing import Optional

from sqlalchemy import DateTime, String, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from ..connect import FieldSurgeDatabase
from ..utilities import try_session, date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

class Customer(Base):
    __tablename__ = "_stg_customer"

    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    street_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    zip: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    # customer_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    phone_2: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    fax: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(String(2040), nullable=True)
    # is_active: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # custom_properties: Mapped[Optional[str]] = mapped_column(String(8000), nullable=True)
    phone_sms_enabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    # phone_2_sms_enabled: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    def __repr__(self) -> str:
        return f"""Customer(
        customer_id={self.id}
        name={self.name}
        company_name={self.company_name}
        street_address={self.street_address}
        city={self.city}
        state={self.state}
        zip={self.zip}
        phone={self.phone}
        phone_2={self.phone_2}
        fax={self.fax}
        email={self.email}
        notes={self.notes}
        phone_sms_enabled={self.phone_sms_enabled}
        created_ts={self.created_ts}
        )"""
    
        # is_active={self.is_active}
        # customer_type_id={self.customer_type_id}
        # custom_properties={self.custom_properties}
        # phone_2_sms_enabled={self.phone_2_sms_enabled}

    def get_customer_data():
        Base.metadata.create_all(db)
        return try_session(session_type='get', session_object=Customer)
    
    