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

class PurchaseOrder(Base):
    __tablename__ = "_stg_fact_purchase_order"

    purchase_order_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    vendor_id: Mapped[Optional[int]] = mapped_column(Integer, autoincrement=False)
    po_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    purchase_order_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    requested_employee_id: Mapped[Optional[int]] = mapped_column(Integer, autoincrement=False)
    approved_employee_id: Mapped[Optional[int]] = mapped_column(Integer, autoincrement=False)
    purchase_order_status_type_id: Mapped[Optional[int]] = mapped_column(Integer, autoincrement=False)
    po_notes: Mapped[Optional[str]] = mapped_column(String(510), nullable=True)
    purchase_order_item_id: Mapped[Optional[int]] = mapped_column(Integer, primary_key=True, autoincrement=False)
    po_job_id: Mapped[Optional[int]] = mapped_column(Integer, autoincrement=False)
    purchase_order_item_type_id: Mapped[Optional[int]] = mapped_column(Integer, autoincrement=False)
    quantity: Mapped[Optional[float]] = mapped_column(DECIMAL(18,4), nullable=True)
    unit_cost: Mapped[Optional[float]] = mapped_column(DECIMAL(18,4), nullable=True)
    extended_cost: Mapped[Optional[float]] = mapped_column(DECIMAL(37,8), nullable=True)
    poi_created_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    uom: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    poi_notes: Mapped[Optional[str]] = mapped_column(String(510), nullable=True)

    def __repr__(self) -> str:
        return f"""PurchaseOrder(
        purchase_order_id={self.purchase_order_id}
        vendor_id={self.vendor_id}
        po_created_ts={self.po_created_ts}
        purchase_order_ts={self.purchase_order_ts}
        requested_employee_id={self.requested_employee_id}
        approved_employeed_id={self.approved_employee_id}
        purchase_order_status_type_id={self.purchase_order_status_type_id}
        po_notes={self.po_notes}
        purchase_order_item_id={self.purchase_order_item_id}
        po_job_id={self.po_job_id}
        purchase_order_item_type_id={self.purchase_order_item_type_id}
        quantity={self.quantity}
        unit_cost={self.unit_cost}
        extended_cost={self.extended_cost}
        poi_created_ts={self.poi_created_ts}
        uom={self.uom}
        poi_notes={self.poi_notes}
        )"""
    
    def delete(self):
        Base.metadata.create_all(db)

        try_session(
            session_type='delete',
            session_object=PurchaseOrder
        )

    def record_json(self, records: list):
        Base.metadata.create_all(db)

        session_update_list: list = []
        for record in records:

            purchase_order_id: int = record['id']
            vendor_id: int = record['vendor_id']
            po_created_ts: datetime = record['created_at']
            purchase_order_ts: datetime = record['created_at'] #duplicate?
            requested_employee_id: int = None
            approved_employee_id: int = None
            purchase_order_status_type_id = None
            po_notes: str = record['comment']
            po_job_id: int = record['job_id']

            for line_item in record['items']:
                purchase_order_item_id: int = line_item['id']
                purchase_order_item_type_id: int = None
                quantity: float = line_item['quantity']
                unit_cost: float = line_item['unit_cost']
                extended_cost: float = None
                poi_created_ts: datetime = None
                uom: str = None
                poi_notes: str = None

                record_object: object = PurchaseOrder(
                    purchase_order_id =purchase_order_id,
                    vendor_id = vendor_id,
                    po_created_ts = po_created_ts,
                    purchase_order_ts = purchase_order_ts,
                    requested_employee_id = requested_employee_id,
                    approved_employee_id = approved_employee_id,
                    purchase_order_status_type_id = purchase_order_status_type_id,
                    po_notes = po_notes,
                    purchase_order_item_id = purchase_order_item_id,
                    po_job_id = po_job_id,
                    purchase_order_item_type_id = purchase_order_item_type_id,
                    quantity = quantity,
                    unit_cost = unit_cost,
                    extended_cost = extended_cost,
                    poi_created_ts = poi_created_ts,
                    uom = uom,
                    poi_notes = poi_notes
                )

                check_local_record = try_session(session_type='get', session_object=PurchaseOrder, record_id=purchase_order_id, composite_key=purchase_order_item_id)
                if check_local_record:
                    session_update_list.append({
                        "purchase_order_id": purchase_order_id,
                        "vendor_id": vendor_id,
                        "po_created_ts": po_created_ts,
                        "purchase_order_ts": purchase_order_ts,
                        "requested_employee_id": requested_employee_id,
                        "approved_employee_id": approved_employee_id,
                        "purchase_order_status_type_id": purchase_order_status_type_id,
                        "po_notes": po_notes,
                        "purchase_order_item_id": purchase_order_item_id,
                        "po_job_id": po_job_id,
                        "purchase_order_item_type_id": purchase_order_item_type_id,
                        "quantity": quantity,
                        "unit_cost": unit_cost,
                        "extended_cost": extended_cost,
                        "poi_created_ts": poi_created_ts,
                        "uom": uom,
                        "poi_notes": poi_notes
                    })
                else:
                    try_session(session_type='add', session_object=record_object)

            if session_update_list != []:
                try_session(
                    session_type='execute', 
                    session_object=PurchaseOrder, 
                    session_list=session_update_list
                )