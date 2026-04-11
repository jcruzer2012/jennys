from datetime import datetime
from decimal import Decimal

from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash


db = SQLAlchemy()


appointment_services = db.Table(
    "appointment_services",
    db.Column("appointment_id", db.Integer, db.ForeignKey("appointment.id"), primary_key=True),
    db.Column("service_id", db.Integer, db.ForeignKey("service.id"), primary_key=True),
)


recurring_series_services = db.Table(
    "recurring_series_services",
    db.Column("recurring_series_id", db.Integer, db.ForeignKey("recurring_series.id"), primary_key=True),
    db.Column("service_id", db.Integer, db.ForeignKey("service.id"), primary_key=True),
)


change_request_services = db.Table(
    "change_request_services",
    db.Column("change_request_id", db.Integer, db.ForeignKey("change_request.id"), primary_key=True),
    db.Column("service_id", db.Integer, db.ForeignKey("service.id"), primary_key=True),
)


promotion_clients = db.Table(
    "promotion_clients",
    db.Column("promotion_id", db.Integer, db.ForeignKey("promotion.id"), primary_key=True),
    db.Column("client_id", db.Integer, db.ForeignKey("client.id"), primary_key=True),
)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(80), nullable=False)
    last_name = db.Column(db.String(80), nullable=False)
    birthdate = db.Column(db.Date)
    phone = db.Column(db.String(40))
    email = db.Column(db.String(120))
    notes = db.Column(db.Text)
    photo_filename = db.Column(db.String(255))
    is_regular = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    appointments = db.relationship("Appointment", back_populates="client", cascade="all, delete-orphan")
    recurring_series = db.relationship("RecurringSeries", back_populates="client", cascade="all, delete-orphan")
    change_requests = db.relationship("ChangeRequest", back_populates="client", cascade="all, delete-orphan")
    promotions = db.relationship("Promotion", secondary=promotion_clients, back_populates="eligible_clients")

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


class RecurringSeries(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    title = db.Column(db.String(120), nullable=False)
    recurrence = db.Column(db.String(20), nullable=False, default="weekly")
    interval_count = db.Column(db.Integer, nullable=False, default=1)
    start_datetime = db.Column(db.DateTime, nullable=False)
    end_datetime = db.Column(db.DateTime, nullable=False)
    duration_minutes = db.Column(db.Integer, nullable=False, default=60)
    buffer_minutes = db.Column(db.Integer, nullable=False, default=0)
    active = db.Column(db.Boolean, default=True, nullable=False)
    generated_until = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    client = db.relationship("Client", back_populates="recurring_series")
    appointments = db.relationship("Appointment", back_populates="recurring_series")
    services = db.relationship("Service", secondary=recurring_series_services, back_populates="recurring_series")


class Appointment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    recurring_series_id = db.Column(db.Integer, db.ForeignKey("recurring_series.id"))
    promotion_id = db.Column(db.Integer, db.ForeignKey("promotion.id"))
    title = db.Column(db.String(120), nullable=False)
    start_datetime = db.Column(db.DateTime, nullable=False)
    end_datetime = db.Column(db.DateTime, nullable=False)
    duration_minutes = db.Column(db.Integer, nullable=False, default=60)
    buffer_minutes = db.Column(db.Integer, nullable=False, default=0)
    status = db.Column(db.String(20), default="scheduled", nullable=False)
    notes = db.Column(db.Text)
    is_override = db.Column(db.Boolean, default=False, nullable=False)
    override_reason = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    client = db.relationship("Client", back_populates="appointments")
    recurring_series = db.relationship("RecurringSeries", back_populates="appointments")
    change_requests = db.relationship("ChangeRequest", back_populates="appointment")
    services = db.relationship("Service", secondary=appointment_services, back_populates="appointments")
    promotion = db.relationship("Promotion", back_populates="appointments")


class Service(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    display_name = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text)
    internal_description = db.Column(db.Text)
    price = db.Column(db.Numeric(10, 2), nullable=False, default=Decimal("0.00"))
    cost = db.Column(db.Numeric(10, 2), nullable=False, default=Decimal("0.00"))
    buffer_minutes = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    appointments = db.relationship("Appointment", secondary=appointment_services, back_populates="services")
    recurring_series = db.relationship("RecurringSeries", secondary=recurring_series_services, back_populates="services")
    change_requests = db.relationship("ChangeRequest", secondary=change_request_services, back_populates="services")
    promotions = db.relationship("Promotion", back_populates="service")


class Promotion(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text)
    service_id = db.Column(db.Integer, db.ForeignKey("service.id"), nullable=False)
    discount_percent = db.Column(db.Numeric(5, 2), nullable=False, default=Decimal("0.00"))
    date_rule = db.Column(db.String(20), nullable=False, default="always")
    custom_date = db.Column(db.Date)
    days_before = db.Column(db.Integer, nullable=False, default=0)
    days_after = db.Column(db.Integer, nullable=False, default=0)
    applies_to_all_clients = db.Column(db.Boolean, default=False, nullable=False)
    active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    service = db.relationship("Service", back_populates="promotions")
    eligible_clients = db.relationship("Client", secondary=promotion_clients, back_populates="promotions")
    change_requests = db.relationship("ChangeRequest", back_populates="promotion")
    appointments = db.relationship("Appointment", back_populates="promotion")


class Unavailability(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    start_datetime = db.Column(db.DateTime, nullable=False)
    end_datetime = db.Column(db.DateTime, nullable=False)
    is_recurring = db.Column(db.Boolean, default=False, nullable=False)
    recurrence = db.Column(db.String(20))
    weekday = db.Column(db.Integer)
    repeat_until = db.Column(db.DateTime)
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class WorkingHours(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    weekday = db.Column(db.Integer, nullable=False, unique=True)
    is_active = db.Column(db.Boolean, default=False, nullable=False)
    start_time = db.Column(db.String(5))
    end_time = db.Column(db.String(5))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class StylistUser(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False, unique=True)
    password_hash = db.Column(db.String(255), nullable=False)
    reset_code_hash = db.Column(db.String(255))
    minimum_notice_hours = db.Column(db.Integer, nullable=False, default=2)
    booking_horizon_days = db.Column(db.Integer, nullable=False, default=90)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def set_reset_code(self, reset_code: str) -> None:
        self.reset_code_hash = generate_password_hash(reset_code)

    def check_reset_code(self, reset_code: str) -> bool:
        if not self.reset_code_hash:
            return False
        return check_password_hash(self.reset_code_hash, reset_code)


class ChangeRequest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)
    appointment_id = db.Column(db.Integer, db.ForeignKey("appointment.id"))
    promotion_id = db.Column(db.Integer, db.ForeignKey("promotion.id"))
    requested_start = db.Column(db.DateTime, nullable=False)
    requested_end = db.Column(db.DateTime, nullable=False)
    message = db.Column(db.Text)
    status = db.Column(db.String(20), default="pending", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    client = db.relationship("Client", back_populates="change_requests")
    appointment = db.relationship("Appointment", back_populates="change_requests")
    services = db.relationship("Service", secondary=change_request_services, back_populates="change_requests")
    promotion = db.relationship("Promotion", back_populates="change_requests")
