from datetime import datetime
import json
from typing import Optional

from sqlalchemy import DateTime, String, Integer, Boolean, DECIMAL
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from field_surge_database.connect import FieldSurgeDatabase
from field_surge_database.utilities.try_sessions import try_session
from field_surge_database.utilities.date_normalization import date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

class Payment(Base):
    __tablename__ = "_stg_fact_payment"

    payment_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    customer_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    payment_type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    amount: Mapped[Optional[float]] = mapped_column(DECIMAL(18,4), nullable=True)
    payment_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_notes: Mapped[Optional[str]] = mapped_column(String(510), nullable=True)
    reference: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    confirmation: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    payment_manager: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    processed_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_canceled_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    amount_allocated: Mapped[Optional[float]] = mapped_column(DECIMAL(18,4), nullable=True)
    invoice_id: Mapped[int] = mapped_column(Integer, nullable=True)
    payment_item_notes: Mapped[Optional[str]] = mapped_column(String(510), nullable=True)
    payment_item_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_item_id: Mapped[int] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        return f"""Payment(
        payment_id={self.payment_id}
        customer_id={self.customer_id}
        payment_type_id={self.payment_type_id}
        amount={self.amount}
        payment_created_ts={self.payment_created_ts}
        payment_ts={self.payment_ts}
        payment_notes={self.payment_notes}
        reference={self.reference}
        confirmation={self.confirmation}
        payment_manager={self.payment_manager}
        processed_ts={self.processed_ts}
        payment_canceled_ts={self.payment_canceled_ts}
        amount_allocated={self.amount_allocated}
        invoice_id={self.invoice_id}
        payment_item_notes={self.payment_item_notes}
        payment_item_created_ts={self.payment_item_created_ts}
        payment_item_id={self.payment_item_id}
        )"""
    
    def delete(self):
        Base.metadata.create_all(db)

        try_session(
            session_type='delete',
            session_object=Payment
        )

    def record_json(self, records: list):
        Base.metadata.create_all(db)

        session_update_list: list = []
        for record in records:

            payment_id: int = record['id']
            customer_id: int = record['customer_id']
            payment_type_id: int = None
            amount: float = record['amount']
            payment_created_ts: datetime = record['created_at']
            payment_ts: datetime = record['payment_date']
            payment_notes: str = record['notes']
            reference: str = None
            confirmation: str = None
            payment_manager: str = None
            processed_ts: datetime = None
            payment_canceled_ts: datetime = None
            amount_allocated: float = None
            invoice_id: int = record['invoice']['import_id']

            for line_item in record['invoice']['line_items']:
                payment_item_notes: str = line_item['line_description']
                payment_item_created_ts: datetime = line_item['created_at']
                payment_item_id: int = line_item['id']

                record_object: object = Payment(
                    payment_id = payment_id,
                    customer_id = customer_id,
                    payment_type_id = payment_type_id,
                    amount = amount,
                    payment_created_ts = payment_created_ts,
                    payment_ts = payment_ts,
                    payment_notes = payment_notes,
                    reference = reference,
                    confirmation = confirmation,
                    payment_manager = payment_manager,
                    processed_ts = processed_ts,
                    payment_canceled_ts = payment_canceled_ts,
                    amount_allocated = amount_allocated,
                    invoice_id = invoice_id,
                    payment_item_notes = payment_item_notes,
                    payment_item_created_ts = payment_item_created_ts,
                    payment_item_id = payment_item_id
                )

                check_local_record = try_session(session_type='get', session_object=Payment, record_id=payment_id, composite_key=None)
                if check_local_record:
                    session_update_list.append({
                        "payment_id": payment_id,
                        "customer_id": customer_id,
                        "payment_type_id": payment_type_id,
                        "amount": amount,
                        "payment_created_ts": payment_created_ts,
                        "payment_ts": payment_ts,
                        "payment_notes": payment_notes,
                        "reference": reference,
                        "confirmation": confirmation,
                        "payment_manager": payment_manager,
                        "processed_ts": processed_ts,
                        "payment_canceled_ts": payment_canceled_ts,
                        "amount_allocated": amount_allocated,
                        "invoice_id": invoice_id,
                        "payment_item_notes": payment_item_notes,
                        "payment_item_created_ts": payment_item_created_ts,
                        "payment_item_id": payment_item_id
                    })
                else:
                    try_session(session_type='add', session_object=record_object)

            if session_update_list != []:
                try_session(
                    session_type='execute', 
                    session_object=Payment, 
                    session_list=session_update_list
                )