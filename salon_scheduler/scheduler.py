from __future__ import annotations

from datetime import datetime, timedelta

from .models import Appointment, RecurringSeries, db


RECURRENCE_TO_DAYS = {
    "weekly": 7,
    "biweekly": 14,
}


def generate_future_appointments(series: RecurringSeries, horizon_days: int = 120) -> None:
    if not series.active:
        return

    step_days = RECURRENCE_TO_DAYS.get(series.recurrence, 7) * max(series.interval_count, 1)
    duration = timedelta(minutes=series.duration_minutes + series.buffer_minutes)
    horizon = datetime.utcnow() + timedelta(days=horizon_days)

    next_start = series.start_datetime
    if series.generated_until and series.generated_until >= series.start_datetime:
        next_start = series.generated_until + timedelta(days=step_days)

    while next_start <= horizon:
        next_end = next_start + duration
        exists = Appointment.query.filter_by(
            recurring_series_id=series.id,
            start_datetime=next_start,
        ).first()
        if not exists:
            appointment = Appointment(
                client_id=series.client_id,
                recurring_series_id=series.id,
                title=series.title,
                start_datetime=next_start,
                end_datetime=next_end,
                duration_minutes=series.duration_minutes,
                buffer_minutes=series.buffer_minutes,
            )
            appointment.services = list(series.services)
            db.session.add(appointment)
        series.generated_until = next_start
        next_start = next_start + timedelta(days=step_days)


def refresh_series(series: RecurringSeries, from_datetime: datetime | None = None) -> None:
    from_point = from_datetime or datetime.utcnow()
    future_generated = Appointment.query.filter(
        Appointment.recurring_series_id == series.id,
        Appointment.start_datetime >= from_point,
        Appointment.is_override.is_(False),
    )
    future_generated.delete(synchronize_session=False)
    series.generated_until = None
    generate_future_appointments(series)
