from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, String, Integer, Boolean, DECIMAL
from sqlalchemy.orm import Mapped, mapped_column, declarative_base

from field_sure_database.connect import FieldSurgeDatabase
from field_sure_database.utilities.try_sessions import try_session
from field_sure_database.utilities.date_normalization import date_normalization

db = FieldSurgeDatabase().connect().execution_options(isolation_level='AUTOCOMMIT')
Base = declarative_base()

class Invoice(Base):
    __tablename__ = "_stg_fact_invoice"

    invoice_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    invoice_item_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    job_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    customer_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    invoice_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    invoice_date_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(String(510), nullable=True)
    reference: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    invoice_status_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    invoice_item_type_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    quantity: Mapped[Optional[float]] = mapped_column(DECIMAL(18,4), nullable=True)
    unit_price: Mapped[Optional[float]] = mapped_column(DECIMAL(18,4), nullable=True)
    invoice_item_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    invoice_item_notes: Mapped[Optional[str]] = mapped_column(String(510), nullable=True)
    extended_price: Mapped[Optional[float]] = mapped_column(DECIMAL(37,8), nullable=True)

    def __repr__(self) -> str:
        return f"""Invoice(
        invoice_id={self.invoice_id}
        invoice_item_id={self.invoice_item_id}
        job_id={self.job_id}
        customer_id={self.customer_id}
        invoice_created_ts={self.invoice_created_ts}
        invoice_date_ts={self.invoice_date_ts}
        notes={self.notes}
        reference={self.reference}
        invoice_status_type_id={self.invoice_status_type_id}
        invoice_item_type_id={self.invoice_item_type_id}
        quantity={self.quantity}
        unit_price={self.unit_price}
        invoice_created_ts={self.invoice_created_ts}
        invoice_item_notes={self.invoice_item_notes}
        extended_price={self.extended_price}
        )"""
    
    def delete(self):
        Base.metadata.create_all(db)

        try_session(
            session_type='delete',
            session_object=Invoice
        )

    def upsert(self, records: list):
        Base.metadata.create_all(db)

        session_update_list: list = []
        for record in records:

            invoice_id: int = record['import_id']
            job_id: int = record['job_id']
            customer_id: int = record['customer']['import_id']
            invoice_created_ts: datetime = date_normalization(data=record, data_key='created_at')
            invoice_date_ts: datetime = date_normalization(data=record, data_key='invoiced_date')
            notes: str = record['notes']
            reference: str = record['reference']

            for line_item in record['line_items']:
                invoice_item_id: int = line_item['id']
                quantity: float = line_item['line_quantity']
                unit_price: float = line_item['line_components'][0]['unit_price']
                invoice_item_created_ts: datetime = date_normalization(data=line_item, data_key='created_at')
                invoice_item_notes: str = line_item['line_description']

                # Fields not in FieldPulse
                extended_price: float = None
                invoice_status_type_id: int = None
                invoice_item_type_id: int = None

                record_object: object = Invoice(
                    invoice_id = invoice_id,
                    invoice_item_id = invoice_item_id,
                    job_id = job_id,
                    customer_id = customer_id,
                    invoice_created_ts = invoice_created_ts,
                    invoice_date_ts = invoice_date_ts,
                    notes = notes,
                    reference = reference,
                    quantity = quantity,
                    unit_price = unit_price,
                    invoice_item_created_ts = invoice_item_created_ts,
                    invoice_item_notes = invoice_item_notes,
                    extended_price = extended_price,
                    invoice_status_type_id = invoice_status_type_id,
                    invoice_item_type_id = invoice_item_type_id
                )

                check_local_record = try_session(session_type='get', session_object=Invoice, record_id=invoice_id, composite_key=invoice_item_id)
                if check_local_record:
                    session_update_list.append({
                        "invoice_id": invoice_id,
                        "invoice_item_id": invoice_item_id,
                        "job_id": job_id,
                        "customer_id": customer_id,
                        "invoice_created_ts": invoice_created_ts,
                        "invoice_date_ts": invoice_date_ts,
                        "notes": notes,
                        "reference": reference,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "invoice_item_created_ts": invoice_item_created_ts,
                        "invoice_item_notes": invoice_item_notes,
                        "extended_price": extended_price,
                        "invoice_status_type_id": invoice_status_type_id,
                        "invoice_item_type_id": invoice_item_type_id,
                    })
                else:
                    try_session(session_type='add', session_object=record_object)

            if session_update_list != []:
                try_session(
                    session_type='execute', 
                    session_object=Invoice, 
                    session_list=session_update_list
                )