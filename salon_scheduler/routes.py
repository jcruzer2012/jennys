from __future__ import annotations

import calendar
import csv
import re
from collections import defaultdict
from functools import wraps
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path
from uuid import uuid4

from flask import (
    Blueprint,
    g,
    current_app,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from sqlalchemy.orm import selectinload

from .models import Appointment, ChangeRequest, Client, Promotion, RecurringSeries, Service, StylistUser, Unavailability, WorkingHours, db
from .scheduler import generate_future_appointments, refresh_series


main_bp = Blueprint("main", __name__)


DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
TIME_FORMAT = "%H:%M"
WEEKDAY_LABELS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


@main_bp.before_app_request
def load_active_session_state():
    g.stylist_user = None
    g.client = None

    stylist_user_id = session.get("stylist_user_id")
    client_id = session.get("client_id")

    if stylist_user_id:
        g.stylist_user = StylistUser.query.get(stylist_user_id)
    if client_id:
        g.client = Client.query.get(client_id)


@main_bp.app_context_processor
def inject_session_state():
    return {
        "current_stylist": getattr(g, "stylist_user", None),
        "current_client": getattr(g, "client", None),
    }


def stylist_login_required(view_func):
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        if StylistUser.query.count() == 0:
            return redirect(url_for("main.stylist_setup"))
        if not g.stylist_user:
            return redirect(url_for("main.stylist_login", next=request.path))
        return view_func(*args, **kwargs)

    return wrapped_view


def client_login_required(view_func):
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        if not g.client:
            flash("Please verify your client account to continue.")
            return redirect(url_for("main.home"))
        return view_func(*args, **kwargs)

    return wrapped_view


def parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, DATETIME_FORMAT)


def parse_int(value: str, default: int = 0) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return default


def normalize_phone_number(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def parse_money_value(value: str | None) -> Decimal:
    cleaned = (value or "").strip()
    if not cleaned:
        return Decimal("0.00")
    try:
        return Decimal(cleaned).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def parse_time_value(value: str | None) -> time | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, TIME_FORMAT).time()
    except ValueError:
        return None


def parse_date_value(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def calculate_end_datetime(start_datetime: datetime, duration_minutes: int, buffer_minutes: int) -> datetime:
    return start_datetime + timedelta(minutes=duration_minutes + buffer_minutes)


def get_primary_stylist() -> StylistUser | None:
    return getattr(g, "stylist_user", None) or StylistUser.query.order_by(StylistUser.id.asc()).first()


def request_policy_window() -> tuple[datetime, datetime | None]:
    stylist = get_primary_stylist()
    now = datetime.utcnow()
    if not stylist:
        return now, None
    earliest = now + timedelta(hours=max(stylist.minimum_notice_hours or 0, 0))
    latest = now + timedelta(days=max(stylist.booking_horizon_days or 0, 0))
    return earliest, latest


def request_time_is_allowed(start_datetime: datetime) -> tuple[bool, str | None]:
    stylist = get_primary_stylist()
    earliest, latest = request_policy_window()
    if start_datetime < earliest:
        return (
            False,
            f"Appointments must be requested at least {max(stylist.minimum_notice_hours if stylist else 0, 0)} hours ahead.",
        )
    if latest and start_datetime > latest:
        return (
            False,
            f"Appointments can only be requested up to {max(stylist.booking_horizon_days if stylist else 0, 0)} days ahead.",
        )
    return True, None


def get_service_options() -> list[Service]:
    return Service.query.order_by(Service.display_name.asc()).all()


def selected_service_ids_from_request() -> list[int]:
    selected: list[int] = []
    for value in request.form.getlist("service_ids"):
        try:
            selected.append(int(value))
        except (TypeError, ValueError):
            continue
    return selected


def load_selected_services(service_ids: list[int]) -> list[Service]:
    if not service_ids:
        return []
    services = Service.query.filter(Service.id.in_(service_ids)).all()
    order_map = {service_id: index for index, service_id in enumerate(service_ids)}
    return sorted(services, key=lambda item: order_map.get(item.id, 0))


def calculate_service_buffer_minutes(services: list[Service], fallback_buffer: int = 0) -> int:
    if services:
        return sum(max(service.buffer_minutes or 0, 0) for service in services)
    return max(fallback_buffer or 0, 0)


def parse_discount_percent(value: str | None) -> Decimal:
    cleaned = (value or "").strip().replace("%", "")
    if not cleaned:
        return Decimal("0.00")
    try:
        return min(max(Decimal(cleaned).quantize(Decimal("0.01")), Decimal("0.00")), Decimal("100.00"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def get_client_promotion_options(client: Client, selected_services: list[Service] | None = None, reference_date: date | None = None) -> list[Promotion]:
    selected_service_ids = {service.id for service in (selected_services or [])}
    promotions = (
        Promotion.query.options(selectinload(Promotion.service), selectinload(Promotion.eligible_clients))
        .filter_by(active=True)
        .order_by(Promotion.created_at.desc(), Promotion.title.asc())
        .all()
    )
    eligible_promotions: list[Promotion] = []
    for promotion in promotions:
        if not promotion.applies_to_all_clients and client not in promotion.eligible_clients:
            continue
        if selected_service_ids and promotion.service_id not in selected_service_ids:
            continue
        if not promotion_matches_date_rule(promotion, client, reference_date=reference_date):
            continue
        eligible_promotions.append(promotion)
    return eligible_promotions


def promotion_is_available_to_client(
    promotion: Promotion | None,
    client: Client,
    selected_services: list[Service],
    reference_date: date | None = None,
) -> bool:
    if not promotion:
        return True
    if not promotion.active:
        return False
    if not promotion.applies_to_all_clients and client not in promotion.eligible_clients:
        return False
    selected_service_ids = {service.id for service in selected_services}
    return promotion.service_id in selected_service_ids and promotion_matches_date_rule(
        promotion,
        client,
        reference_date=reference_date,
    )


def calculate_promotion_discount_amount(promotion: Promotion | None) -> Decimal:
    if not promotion or not promotion.service:
        return Decimal("0.00")
    return ((promotion.service.price or Decimal("0.00")) * (promotion.discount_percent or Decimal("0.00")) / Decimal("100.00")).quantize(Decimal("0.01"))


def selected_promotion_from_request() -> Promotion | None:
    promotion_id = parse_int(request.form.get("promotion_id"))
    if not promotion_id:
        return None
    return Promotion.query.get(promotion_id)


def anniversary_date_for_year(source_date: date, year: int) -> date:
    if source_date.month == 2 and source_date.day == 29:
        try:
            return date(year, 2, 29)
        except ValueError:
            return date(year, 2, 28)
    return date(year, source_date.month, source_date.day)


def promotion_matches_date_rule(promotion: Promotion, client: Client, reference_date: date | None = None) -> bool:
    rule = (promotion.date_rule or "always").strip().lower()
    if rule == "always":
        return True

    target_date = reference_date or datetime.utcnow().date()
    window_before = max(promotion.days_before or 0, 0)
    window_after = max(promotion.days_after or 0, 0)

    if rule == "birthday":
        if not client.birthdate:
            return False
        event_date = anniversary_date_for_year(client.birthdate, target_date.year)
    elif rule == "custom":
        if not promotion.custom_date:
            return False
        event_date = promotion.custom_date
    else:
        return True

    window_start = event_date - timedelta(days=window_before)
    window_end = event_date + timedelta(days=window_after)
    return window_start <= target_date <= window_end


def appointment_financials(appointment: Appointment) -> tuple[Decimal, Decimal, Decimal]:
    income = sum((service.price or Decimal("0.00")) for service in appointment.services)
    cost = sum((service.cost or Decimal("0.00")) for service in appointment.services)
    discount = calculate_promotion_discount_amount(appointment.promotion)
    discounted_income = max(income - discount, Decimal("0.00"))
    return discounted_income, cost, discounted_income - cost


def render_appointment_form_template(
    *,
    clients,
    appointment,
    selected_month,
    month_grid,
    previous_month,
    next_month_param,
    show_series_options,
    service_options,
    selected_service_ids,
):
    return render_template(
        "appointment_form.html",
        clients=clients,
        appointment=appointment,
        selected_month=selected_month,
        month_grid=month_grid,
        previous_month=previous_month,
        next_month_param=next_month_param,
        show_series_options=show_series_options,
        service_options=service_options,
        selected_service_ids=selected_service_ids,
    )


def summarize_appointment_services(appointments: list[Appointment]) -> dict:
    totals = {
        "income": Decimal("0.00"),
        "cost": Decimal("0.00"),
        "profit": Decimal("0.00"),
        "appointment_count": len(appointments),
    }
    client_totals: dict[int, dict] = {}

    for appointment in appointments:
        appointment_income, appointment_cost, appointment_profit = appointment_financials(appointment)

        totals["income"] += appointment_income
        totals["cost"] += appointment_cost
        totals["profit"] += appointment_profit

        client_bucket = client_totals.setdefault(
            appointment.client_id,
            {
                "client": appointment.client,
                "income": Decimal("0.00"),
                "cost": Decimal("0.00"),
                "profit": Decimal("0.00"),
                "appointment_count": 0,
                "service_count": 0,
            },
        )
        client_bucket["income"] += appointment_income
        client_bucket["cost"] += appointment_cost
        client_bucket["profit"] += appointment_profit
        client_bucket["appointment_count"] += 1
        client_bucket["service_count"] += len(appointment.services)

    client_rows = sorted(client_totals.values(), key=lambda item: (item["income"], item["appointment_count"]), reverse=True)
    totals["client_rows"] = client_rows
    return totals


def export_report_csv(appointments: list[Appointment]) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "Appointment Date",
            "Client",
            "Appointment Title",
            "Services",
            "Promotion",
            "Income",
            "Cost",
            "Profit",
            "Appointment Notes",
        ]
    )
    for appointment in appointments:
        income, cost, profit = appointment_financials(appointment)
        writer.writerow(
            [
                appointment.start_datetime.strftime("%Y-%m-%d %H:%M"),
                appointment.client.full_name,
                appointment.title,
                ", ".join(service.display_name for service in appointment.services) or "No services",
                appointment.promotion.title if appointment.promotion else "",
                f"{income:.2f}",
                f"{cost:.2f}",
                f"{profit:.2f}",
                appointment.notes or "",
            ]
        )
    return buffer.getvalue()


def time_ranges_overlap(start_a: datetime, end_a: datetime, start_b: datetime, end_b: datetime) -> bool:
    return start_a < end_b and start_b < end_a


def get_working_hours_map() -> dict[int, WorkingHours]:
    return {item.weekday: item for item in WorkingHours.query.order_by(WorkingHours.weekday.asc()).all()}


def combine_date_and_time(date_value, time_value: time) -> datetime:
    return datetime.combine(date_value, time_value)


def get_day_working_range(target_date) -> tuple[datetime, datetime] | None:
    hours_map = get_working_hours_map()
    entry = hours_map.get(target_date.weekday())
    if not entry or not entry.is_active or not entry.start_time or not entry.end_time:
        return None

    start_time = parse_time_value(entry.start_time)
    end_time = parse_time_value(entry.end_time)
    if not start_time or not end_time:
        return None

    return (
        combine_date_and_time(target_date, start_time),
        combine_date_and_time(target_date, end_time),
    )


def is_all_day_unavailability(item: Unavailability) -> bool:
    working_range = get_day_working_range(item.start_datetime.date())
    if not working_range:
        return False
    return item.start_datetime == working_range[0] and item.end_datetime == working_range[1]


def within_working_hours(start_datetime: datetime, end_datetime: datetime) -> tuple[bool, str | None]:
    hours_map = get_working_hours_map()
    active_hours = [item for item in hours_map.values() if item.is_active and item.start_time and item.end_time]
    if not active_hours:
        return True, None

    entry = hours_map.get(start_datetime.weekday())
    if not entry or not entry.is_active or not entry.start_time or not entry.end_time:
        return False, f"You are not scheduled to work on {WEEKDAY_LABELS[start_datetime.weekday()]}."

    day_start = parse_time_value(entry.start_time)
    day_end = parse_time_value(entry.end_time)
    if not day_start or not day_end:
        return False, "Working hours are incomplete for that day."

    allowed_start = combine_date_and_time(start_datetime.date(), day_start)
    allowed_end = combine_date_and_time(start_datetime.date(), day_end)
    if start_datetime < allowed_start or end_datetime > allowed_end:
        return (
            False,
            f"This appointment is outside your working hours for {WEEKDAY_LABELS[start_datetime.weekday()]} "
            f"({entry.start_time} to {entry.end_time}).",
        )

    return True, None


def recurring_unavailability_overlaps(start_datetime: datetime, end_datetime: datetime) -> Unavailability | None:
    recurring_items = (
        Unavailability.query.filter_by(is_recurring=True, recurrence="weekly")
        .order_by(Unavailability.weekday.asc(), Unavailability.start_datetime.asc())
        .all()
    )
    for item in recurring_items:
        if item.weekday != start_datetime.weekday():
            continue
        if item.repeat_until and start_datetime > item.repeat_until:
            continue
        if start_datetime < item.start_datetime:
            continue
        blocked_start = combine_date_and_time(start_datetime.date(), item.start_datetime.time())
        blocked_end = combine_date_and_time(start_datetime.date(), item.end_datetime.time())
        if time_ranges_overlap(start_datetime, end_datetime, blocked_start, blocked_end):
            return item
    return None


def get_unavailability_for_date(target_date) -> list[tuple[datetime, datetime, Unavailability]]:
    items: list[tuple[datetime, datetime, Unavailability]] = []

    one_time_items = (
        Unavailability.query.filter(
            Unavailability.is_recurring.is_(False),
            Unavailability.start_datetime < combine_date_and_time(target_date, time.max),
            Unavailability.end_datetime > combine_date_and_time(target_date, time.min),
        )
        .order_by(Unavailability.start_datetime.asc())
        .all()
    )
    for item in one_time_items:
        items.append((item.start_datetime, item.end_datetime, item))

    recurring_items = (
        Unavailability.query.filter_by(is_recurring=True, recurrence="weekly", weekday=target_date.weekday())
        .order_by(Unavailability.start_datetime.asc())
        .all()
    )
    for item in recurring_items:
        if target_date < item.start_datetime.date():
            continue
        if item.repeat_until and target_date > item.repeat_until.date():
            continue
        start_at = combine_date_and_time(target_date, item.start_datetime.time())
        end_at = combine_date_and_time(target_date, item.end_datetime.time())
        items.append((start_at, end_at, item))

    return items


def get_appointments_for_date(target_date) -> list[Appointment]:
    day_start = datetime.combine(target_date, time.min)
    day_end = datetime.combine(target_date, time.max)
    return (
        Appointment.query.filter(
            Appointment.status == "scheduled",
            Appointment.start_datetime >= day_start,
            Appointment.start_datetime <= day_end,
        )
        .order_by(Appointment.start_datetime.asc())
        .all()
    )


def get_booked_ranges_for_date(target_date) -> list[tuple[datetime, datetime]]:
    return [(item.start_datetime, item.end_datetime) for item in get_appointments_for_date(target_date)]


def merge_ranges(ranges: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    if not ranges:
        return []
    ranges = sorted(ranges, key=lambda item: item[0])
    merged = [ranges[0]]
    for start_at, end_at in ranges[1:]:
        last_start, last_end = merged[-1]
        if start_at <= last_end:
            merged[-1] = (last_start, max(last_end, end_at))
        else:
            merged.append((start_at, end_at))
    return merged


def get_available_windows_for_date(target_date) -> list[tuple[datetime, datetime]]:
    working_range = get_day_working_range(target_date)
    if not working_range:
        return []

    day_start, day_end = working_range
    earliest_request, latest_request = request_policy_window()
    if latest_request and day_start > latest_request:
        return []
    day_start = max(day_start, earliest_request)
    if latest_request:
        day_end = min(day_end, latest_request)
    if day_start >= day_end:
        return []

    blocked_ranges = [(start_at, end_at) for start_at, end_at, _ in get_unavailability_for_date(target_date)]
    blocked_ranges.extend(get_booked_ranges_for_date(target_date))
    blocked_ranges = merge_ranges(blocked_ranges)

    cursor = day_start
    openings: list[tuple[datetime, datetime]] = []
    for blocked_start, blocked_end in blocked_ranges:
        if blocked_end <= day_start or blocked_start >= day_end:
            continue
        clipped_start = max(blocked_start, day_start)
        clipped_end = min(blocked_end, day_end)
        if clipped_start > cursor:
            openings.append((cursor, clipped_start))
        cursor = max(cursor, clipped_end)

    if cursor < day_end:
        openings.append((cursor, day_end))

    return [(start_at, end_at) for start_at, end_at in openings if start_at < end_at and end_at > datetime.utcnow()]


def find_appointment_overlap(
    start_datetime: datetime,
    end_datetime: datetime,
    ignore_appointment_id: int | None = None,
) -> Appointment | None:
    query = Appointment.query.filter(
        Appointment.status == "scheduled",
        Appointment.start_datetime < end_datetime,
        Appointment.end_datetime > start_datetime,
    )
    if ignore_appointment_id is not None:
        query = query.filter(Appointment.id != ignore_appointment_id)
    return query.order_by(Appointment.start_datetime.asc()).first()


def date_is_all_day_off(target_date) -> tuple[bool, Unavailability | None]:
    working_range = get_day_working_range(target_date)
    if not working_range:
        return False, None

    day_start, day_end = working_range
    for blocked_start, blocked_end, item in get_unavailability_for_date(target_date):
        if blocked_start <= day_start and blocked_end >= day_end:
            return True, item
    return False, None


def find_unavailability_overlap(start_datetime: datetime, end_datetime: datetime) -> Unavailability | None:
    one_time_overlap = (
        Unavailability.query.filter(
            Unavailability.is_recurring.is_(False),
            Unavailability.start_datetime < end_datetime,
            Unavailability.end_datetime > start_datetime,
        )
        .order_by(Unavailability.start_datetime.asc())
        .first()
    )
    return one_time_overlap or recurring_unavailability_overlaps(start_datetime, end_datetime)


def recurring_occurrences(
    start_datetime: datetime,
    duration_minutes: int,
    buffer_minutes: int,
    recurrence: str,
    interval_count: int = 1,
    horizon_days: int = 120,
) -> list[tuple[datetime, datetime]]:
    step_days = {"weekly": 7, "biweekly": 14}.get(recurrence, 7) * max(interval_count, 1)
    end_datetime = calculate_end_datetime(start_datetime, duration_minutes, buffer_minutes)
    occurrences: list[tuple[datetime, datetime]] = []
    current_start = start_datetime
    horizon = datetime.utcnow() + timedelta(days=horizon_days)
    duration = end_datetime - start_datetime

    while current_start <= horizon:
        occurrences.append((current_start, current_start + duration))
        current_start = current_start + timedelta(days=step_days)

    return occurrences


def find_conflicting_occurrence(
    start_datetime: datetime,
    duration_minutes: int,
    buffer_minutes: int,
    recurrence: str,
) -> tuple[datetime, Unavailability] | None:
    for occurrence_start, occurrence_end in recurring_occurrences(
        start_datetime,
        duration_minutes,
        buffer_minutes,
        recurrence,
    ):
        blocked_time = find_unavailability_overlap(occurrence_start, occurrence_end)
        if blocked_time:
            return occurrence_start, blocked_time
    return None


def collect_conflicting_appointments() -> list[Appointment]:
    appointments = (
        Appointment.query.join(Client)
        .filter(Appointment.start_datetime >= datetime.utcnow(), Appointment.status == "scheduled")
        .order_by(Appointment.start_datetime.asc())
        .all()
    )
    conflicts: list[Appointment] = []
    seen_ids: set[int] = set()
    for appointment in appointments:
        within_hours, _ = within_working_hours(appointment.start_datetime, appointment.end_datetime)
        blocked_time = find_unavailability_overlap(appointment.start_datetime, appointment.end_datetime)
        if not within_hours or blocked_time:
            if appointment.id not in seen_ids:
                conflicts.append(appointment)
                seen_ids.add(appointment.id)
    return conflicts


def parse_month(value: str | None) -> datetime:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m")
        except ValueError:
            pass
    now = datetime.utcnow()
    return datetime(now.year, now.month, 1)


def month_bounds(month_start: datetime) -> tuple[datetime, datetime]:
    if month_start.month == 12:
        next_month = datetime(month_start.year + 1, 1, 1)
    else:
        next_month = datetime(month_start.year, month_start.month + 1, 1)
    return month_start, next_month


def calendar_navigation(month_start: datetime) -> tuple[str, str]:
    previous_month = (
        datetime(month_start.year - 1, 12, 1)
        if month_start.month == 1
        else datetime(month_start.year, month_start.month - 1, 1)
    )
    next_month_value = (
        datetime(month_start.year + 1, 1, 1)
        if month_start.month == 12
        else datetime(month_start.year, month_start.month + 1, 1)
    )
    return previous_month.strftime("%Y-%m"), next_month_value.strftime("%Y-%m")


def build_month_grid(month_start: datetime, month_appointments: list[Appointment]) -> list[list[dict]]:
    appointments_by_day: dict[datetime.date, list[Appointment]] = {}
    for appointment in month_appointments:
        day_key = appointment.start_datetime.date()
        appointments_by_day.setdefault(day_key, []).append(appointment)

    first_weekday, days_in_month = calendar.monthrange(month_start.year, month_start.month)
    first_weekday = (first_weekday + 1) % 7
    cells: list[dict] = []

    for _ in range(first_weekday):
        cells.append({"in_month": False})

    for day in range(1, days_in_month + 1):
        current_date = datetime(month_start.year, month_start.month, day).date()
        is_all_day_off, blocking_item = date_is_all_day_off(current_date)
        working_range = get_day_working_range(current_date)
        available_windows = get_available_windows_for_date(current_date)
        is_past = current_date < datetime.utcnow().date()
        cells.append(
            {
                "in_month": True,
                "date": current_date,
                "appointments": appointments_by_day.get(current_date, []),
                "is_today": current_date == datetime.utcnow().date(),
                "is_all_day_off": is_all_day_off,
                "blocking_item": blocking_item,
                "has_working_hours": working_range is not None,
                "working_start": working_range[0].strftime("%H:%M") if working_range else None,
                "working_end": working_range[1].strftime("%H:%M") if working_range else None,
                "available_windows": available_windows,
                "available_window_labels": [
                    f"{start_at.strftime('%H:%M')}-{end_at.strftime('%H:%M')}" for start_at, end_at in available_windows
                ],
                "is_request_available": bool(available_windows) and not is_past,
                "is_past": is_past,
            }
        )

    while len(cells) % 7 != 0:
        cells.append({"in_month": False})

    return [cells[index:index + 7] for index in range(0, len(cells), 7)]


def build_mobile_request_days(month_grid: list[list[dict]]) -> list[dict]:
    in_month_days = [cell for week in month_grid for cell in week if cell.get("in_month")]
    first_available_index = 0
    for index, cell in enumerate(in_month_days):
        if cell.get("is_request_available"):
            first_available_index = index
            break
    return in_month_days[first_available_index:]


def add_months(month_start: datetime, month_offset: int) -> datetime:
    year = month_start.year + ((month_start.month - 1 + month_offset) // 12)
    month = ((month_start.month - 1 + month_offset) % 12) + 1
    return datetime(year, month, 1)


def build_request_calendar_sections(start_month: datetime, month_count: int = 4) -> list[dict]:
    sections: list[dict] = []
    first_available_section_index = 0

    for month_offset in range(max(month_count, 1)):
        month_start = add_months(start_month, month_offset)
        month_grid = build_month_grid(month_start, [])
        mobile_request_days = build_mobile_request_days(month_grid)
        sections.append(
            {
                "month_start": month_start,
                "month_grid": month_grid,
                "mobile_request_days": mobile_request_days,
                "has_requestable_days": any(cell.get("is_request_available") for week in month_grid for cell in week),
            }
        )

    for index, section in enumerate(sections):
        if section["has_requestable_days"]:
            first_available_section_index = index
            break

    return sections[first_available_section_index:]


def request_calendar_month_count() -> int:
    stylist = get_primary_stylist()
    horizon_days = max((stylist.booking_horizon_days if stylist else 90) or 90, 30)
    return min(max((horizon_days // 30) + 2, 3), 12)


def get_client_pending_request(client_id: int, exclude_request_id: int | None = None) -> ChangeRequest | None:
    query = ChangeRequest.query.filter_by(client_id=client_id, status="pending")
    if exclude_request_id is not None:
        query = query.filter(ChangeRequest.id != exclude_request_id)
    return query.order_by(ChangeRequest.created_at.desc()).first()


def change_request_duration_minutes(change_request: ChangeRequest) -> int:
    service_buffer = calculate_service_buffer_minutes(list(change_request.services))
    total_minutes = max(int((change_request.requested_end - change_request.requested_start).total_seconds() // 60), 0)
    if change_request.appointment and change_request.appointment.duration_minutes:
        return max(change_request.appointment.duration_minutes, 0)
    return max(total_minutes - service_buffer, 0) or 60


def build_change_request_form_values(change_request: ChangeRequest | None = None) -> dict:
    if change_request is None:
        return {
            "appointment_id": request.form.get("appointment_id", ""),
            "promotion_id": request.form.get("promotion_id", ""),
            "requested_date": request.form.get("requested_date", ""),
            "requested_start_time": request.form.get("requested_start_time", ""),
            "duration_minutes": request.form.get("duration_minutes", "60"),
            "message": request.form.get("message", ""),
        }

    return {
        "appointment_id": request.form.get("appointment_id", str(change_request.appointment_id or "")),
        "promotion_id": request.form.get("promotion_id", str(change_request.promotion_id or "")),
        "requested_date": request.form.get("requested_date", change_request.requested_start.strftime("%Y-%m-%d")),
        "requested_start_time": request.form.get("requested_start_time", change_request.requested_start.strftime("%H:%M")),
        "duration_minutes": request.form.get("duration_minutes", str(change_request_duration_minutes(change_request))),
        "message": request.form.get("message", change_request.message or ""),
    }


def render_change_request_template(
    *,
    appointments,
    next_appointment,
    service_options,
    selected_service_ids,
    promotion_options=None,
    selected_promotion_id="",
    pending_request=None,
    editing_request=None,
    form_values=None,
):
    form_values = form_values or build_change_request_form_values(editing_request)
    selected_services = load_selected_services(selected_service_ids)
    reference_date = parse_date_value(form_values.get("requested_date"))
    if promotion_options is None:
        promotion_options = get_client_promotion_options(g.client, selected_services, reference_date=reference_date)
    if not selected_promotion_id:
        selected_promotion_id = form_values.get("promotion_id", "")
    selected_month = parse_month(request.args.get("month"))
    calendar_sections = build_request_calendar_sections(selected_month, request_calendar_month_count())
    return render_template(
        "request_form.html",
        client=g.client,
        next_appointment=next_appointment,
        appointments=appointments,
        service_options=service_options,
        selected_service_ids=selected_service_ids,
        promotion_options=promotion_options,
        selected_promotion_id=selected_promotion_id,
        calendar_sections=calendar_sections,
        pending_request=pending_request,
        editing_request=editing_request,
        form_values=form_values,
    )


def appointment_form_calendar(month_value: str | None):
    selected_month = parse_month(month_value)
    month_start, next_month = month_bounds(selected_month)
    month_appointments = (
        Appointment.query.join(Client)
        .filter(Appointment.start_datetime >= month_start, Appointment.start_datetime < next_month)
        .order_by(Appointment.start_datetime.asc())
        .all()
    )
    month_grid = build_month_grid(month_start, month_appointments)
    previous_month, next_month_param = calendar_navigation(month_start)
    return month_start, month_grid, previous_month, next_month_param


def save_photo(upload) -> str | None:
    if not upload or not upload.filename:
        return None
    extension = Path(upload.filename).suffix.lower()
    filename = f"{uuid4().hex}{extension}"
    destination = Path(current_app.config["UPLOAD_FOLDER"]) / filename
    upload.save(destination)
    return filename


def delete_photo(filename: str | None) -> None:
    if not filename:
        return
    photo_path = Path(current_app.config["UPLOAD_FOLDER"]) / filename
    if photo_path.exists():
        photo_path.unlink()


@main_bp.route("/")
def home():
    return render_template("home.html")


@main_bp.route("/client/register", methods=["GET", "POST"])
def client_register():
    if request.method == "POST":
        first_name = request.form.get("first_name", "").strip()
        last_name = request.form.get("last_name", "").strip()
        phone = request.form.get("phone", "").strip()
        email = request.form.get("email", "").strip()
        birthdate = parse_date_value(request.form.get("birthdate"))

        if not first_name or not last_name or not phone:
            flash("First name, last name, and phone number are required.")
            return render_template("client_register.html")

        normalized_phone = normalize_phone_number(phone)
        existing_clients = Client.query.filter(db.func.lower(Client.last_name) == last_name.lower()).all()
        for existing_client in existing_clients:
            if (
                existing_client.first_name.strip().lower() == first_name.lower()
                and normalize_phone_number(existing_client.phone or "") == normalized_phone
            ):
                flash("That client account already exists. Please sign in with your last name and phone number.")
                return redirect(url_for("main.home"))

        client = Client(
            first_name=first_name,
            last_name=last_name,
            phone=phone,
            email=email,
            birthdate=birthdate,
            is_regular=False,
        )
        db.session.add(client)
        db.session.commit()

        session.pop("stylist_user_id", None)
        session["client_id"] = client.id
        flash("Your client portal account is ready.")
        return redirect(url_for("main.create_change_request"))

    return render_template("client_register.html")


@main_bp.route("/client/login", methods=["POST"])
def client_login():
    last_name = request.form.get("last_name", "").strip().lower()
    phone = normalize_phone_number(request.form.get("phone", ""))

    client = None
    matching_clients = (
        Client.query.filter(db.func.lower(Client.last_name) == last_name).all()
    )
    for candidate in matching_clients:
        if normalize_phone_number(candidate.phone or "") == phone:
            client = candidate
            break

    if not client:
        flash("We couldn't verify that client login. Please check the last name and phone number.")
        return redirect(url_for("main.home"))

    session.pop("stylist_user_id", None)
    session["client_id"] = client.id
    return redirect(url_for("main.create_change_request"))


@main_bp.route("/client/logout", methods=["POST"])
def client_logout():
    session.pop("client_id", None)
    flash("Client access ended.")
    return redirect(url_for("main.home"))


@main_bp.route("/stylist/setup", methods=["GET", "POST"])
def stylist_setup():
    if StylistUser.query.count() > 0:
        return redirect(url_for("main.stylist_login"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        reset_code = request.form.get("reset_code", "")
        confirm_reset_code = request.form.get("confirm_reset_code", "")

        if not username or not password:
            flash("Username and password are required.")
            return render_template("stylist_setup.html")
        if password != confirm_password:
            flash("Passwords do not match.")
            return render_template("stylist_setup.html")
        if not reset_code:
            flash("A reset code is required so you can recover your password.")
            return render_template("stylist_setup.html")
        if reset_code != confirm_reset_code:
            flash("Reset codes do not match.")
            return render_template("stylist_setup.html")

        user = StylistUser(username=username)
        user.set_password(password)
        user.set_reset_code(reset_code)
        db.session.add(user)
        db.session.commit()
        flash("Stylist account created. Please sign in.")
        return redirect(url_for("main.stylist_login"))

    return render_template("stylist_setup.html")


@main_bp.route("/stylist/login", methods=["GET", "POST"])
def stylist_login():
    if StylistUser.query.count() == 0:
        return redirect(url_for("main.stylist_setup"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = StylistUser.query.filter_by(username=username).first()

        if not user or not user.check_password(password):
            flash("Incorrect username or password.")
            return render_template(
                "stylist_login.html",
                next=request.form.get("next", "") or request.args.get("next", ""),
            )

        session.pop("client_id", None)
        session["stylist_user_id"] = user.id
        next_path = request.args.get("next") or request.form.get("next") or url_for("main.dashboard")
        return redirect(next_path)

    return render_template("stylist_login.html", next=request.args.get("next", ""))


@main_bp.route("/stylist/reset-password", methods=["GET", "POST"])
def stylist_reset_password():
    if StylistUser.query.count() == 0:
        return redirect(url_for("main.stylist_setup"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        reset_code = request.form.get("reset_code", "")
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        user = StylistUser.query.filter_by(username=username).first()

        if not user or not user.check_reset_code(reset_code):
            flash("That username and reset code did not match our records.")
            return render_template("stylist_reset_password.html")
        if not password:
            flash("Please enter a new password.")
            return render_template("stylist_reset_password.html")
        if password != confirm_password:
            flash("Passwords do not match.")
            return render_template("stylist_reset_password.html")

        user.set_password(password)
        db.session.commit()
        flash("Password reset complete. Please sign in.")
        return redirect(url_for("main.stylist_login"))

    return render_template("stylist_reset_password.html")


@main_bp.route("/stylist/profile", methods=["GET", "POST"])
@stylist_login_required
def stylist_profile():
    stylist = g.stylist_user
    if request.method == "POST":
        stylist.minimum_notice_hours = parse_int(request.form.get("minimum_notice_hours"), default=2)
        stylist.booking_horizon_days = parse_int(request.form.get("booking_horizon_days"), default=90)
        db.session.commit()
        flash("Profile settings updated.")
        return redirect(url_for("main.stylist_profile"))
    return render_template("stylist_profile.html", stylist=stylist)


@main_bp.route("/stylist/logout", methods=["POST"])
def stylist_logout():
    session.pop("stylist_user_id", None)
    flash("Stylist signed out.")
    return redirect(url_for("main.home"))


@main_bp.route("/dashboard")
@stylist_login_required
def dashboard():
    for series in RecurringSeries.query.filter_by(active=True).all():
        generate_future_appointments(series)
    db.session.commit()

    now = datetime.utcnow()
    today_start = datetime.combine(now.date(), time.min)
    tomorrow_end = datetime.combine(now.date() + timedelta(days=1), time.max)
    upcoming_appointments = (
        Appointment.query.join(Client)
        .filter(Appointment.start_datetime >= today_start, Appointment.start_datetime <= tomorrow_end)
        .order_by(Appointment.start_datetime.asc())
        .all()
    )
    pending_requests = (
        ChangeRequest.query.join(Client)
        .filter(ChangeRequest.status == "pending")
        .order_by(ChangeRequest.created_at.desc())
        .all()
    )
    reschedule_appointments = collect_conflicting_appointments()
    upcoming_unavailability = (
        Unavailability.query.filter(
            (Unavailability.is_recurring.is_(True)) | (Unavailability.end_datetime >= datetime.utcnow())
        )
        .order_by(Unavailability.start_datetime.asc())
        .limit(6)
        .all()
    )
    working_hours = [
        get_working_hours_map().get(index) or WorkingHours(weekday=index, is_active=False)
        for index in range(7)
    ]
    client_count = Client.query.count()
    regular_count = Client.query.filter_by(is_regular=True).count()
    return render_template(
        "dashboard.html",
        upcoming_appointments=upcoming_appointments,
        pending_requests=pending_requests,
        reschedule_appointments=reschedule_appointments,
        upcoming_unavailability=upcoming_unavailability,
        working_hours=working_hours,
        client_count=client_count,
        regular_count=regular_count,
    )


@main_bp.route("/uploads/<path:filename>")
def uploaded_file(filename: str):
    return send_from_directory(current_app.config["UPLOAD_FOLDER"], filename)


@main_bp.route("/clients")
@stylist_login_required
def clients():
    clients_list = Client.query.order_by(Client.last_name.asc(), Client.first_name.asc()).all()
    return render_template("clients.html", clients=clients_list)


@main_bp.route("/services")
@stylist_login_required
def services():
    services_list = Service.query.order_by(Service.display_name.asc()).all()
    return render_template("services.html", services=services_list)


@main_bp.route("/reports")
@stylist_login_required
def reports():
    current_year = datetime.utcnow().year
    selected_year_value = request.args.get("year", str(current_year))
    selected_client_id = request.args.get("client_id", "")
    selected_service_id = request.args.get("service_id", "")
    selected_timing = request.args.get("timing", "all")
    start_date = parse_date_value(request.args.get("start_date"))
    end_date = parse_date_value(request.args.get("end_date"))
    export_format = request.args.get("export", "").strip().lower()

    appointment_query = (
        Appointment.query.options(selectinload(Appointment.client), selectinload(Appointment.services))
        .filter(Appointment.status == "scheduled")
        .order_by(Appointment.start_datetime.desc())
    )
    all_appointments = appointment_query.all()

    years = sorted({item.start_datetime.year for item in all_appointments}, reverse=True)
    client_options = Client.query.order_by(Client.last_name.asc(), Client.first_name.asc()).all()
    service_options = Service.query.order_by(Service.display_name.asc()).all()
    now = datetime.utcnow()

    if selected_year_value == "all":
        year_filtered_appointments = all_appointments
        selected_year_label = "All time"
    else:
        try:
            selected_year = int(selected_year_value)
        except (TypeError, ValueError):
            selected_year = current_year
            selected_year_value = str(current_year)
        year_filtered_appointments = [item for item in all_appointments if item.start_datetime.year == selected_year]
        selected_year_label = str(selected_year)

    current_year_appointments = [item for item in all_appointments if item.start_datetime.year == current_year]
    current_year_collected_appointments = [item for item in current_year_appointments if item.start_datetime <= now]
    current_year_scheduled_appointments = [item for item in current_year_appointments if item.start_datetime > now]

    filtered_appointments = year_filtered_appointments

    if selected_client_id:
        try:
            selected_client_id_int = int(selected_client_id)
            filtered_appointments = [item for item in filtered_appointments if item.client_id == selected_client_id_int]
        except (TypeError, ValueError):
            selected_client_id = ""

    if selected_service_id:
        try:
            selected_service_id_int = int(selected_service_id)
            filtered_appointments = [
                item for item in filtered_appointments if any(service.id == selected_service_id_int for service in item.services)
            ]
        except (TypeError, ValueError):
            selected_service_id = ""

    if start_date:
        filtered_appointments = [item for item in filtered_appointments if item.start_datetime.date() >= start_date]
    if end_date:
        filtered_appointments = [item for item in filtered_appointments if item.start_datetime.date() <= end_date]

    if selected_timing == "past":
        filtered_appointments = [item for item in filtered_appointments if item.start_datetime <= now]
    elif selected_timing == "future":
        filtered_appointments = [item for item in filtered_appointments if item.start_datetime > now]
    else:
        selected_timing = "all"

    if export_format == "csv":
        csv_content = export_report_csv(filtered_appointments)
        response = make_response(csv_content)
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = "attachment; filename=income-report.csv"
        return response

    monthly_income = defaultdict(lambda: Decimal("0.00"))
    for appointment in filtered_appointments:
        month_label = appointment.start_datetime.month
        monthly_income[month_label] += appointment_financials(appointment)[0]

    ordered_monthly_income = [
        (calendar.month_abbr[month_number], monthly_income[month_number])
        for month_number in range(1, 13)
        if monthly_income[month_number] > 0
    ]

    return render_template(
        "reports.html",
        selected_year_value=selected_year_value,
        selected_year_label=selected_year_label,
        selected_client_id=selected_client_id,
        selected_service_id=selected_service_id,
        selected_timing=selected_timing,
        start_date=start_date.isoformat() if start_date else "",
        end_date=end_date.isoformat() if end_date else "",
        years=years,
        client_options=client_options,
        service_options=service_options,
        current_year=current_year,
        current_year_collected_totals=summarize_appointment_services(current_year_collected_appointments),
        current_year_scheduled_totals=summarize_appointment_services(current_year_scheduled_appointments),
        filtered_totals=summarize_appointment_services(filtered_appointments),
        monthly_income=ordered_monthly_income,
    )


@main_bp.route("/services/new", methods=["GET", "POST"])
@stylist_login_required
def create_service():
    if request.method == "POST":
        service = Service(
            display_name=request.form.get("display_name", "").strip(),
            description=request.form.get("description", "").strip(),
            internal_description=request.form.get("internal_description", "").strip(),
            price=parse_money_value(request.form.get("price")),
            cost=parse_money_value(request.form.get("cost")),
            buffer_minutes=parse_int(request.form.get("buffer_minutes"), default=0),
        )
        if not service.display_name:
            flash("A display name is required.")
            return render_template("service_form.html", service=None)
        db.session.add(service)
        db.session.commit()
        flash("Service saved.")
        return redirect(url_for("main.services"))
    return render_template("service_form.html", service=None)


@main_bp.route("/services/<int:service_id>/edit", methods=["GET", "POST"])
@stylist_login_required
def edit_service(service_id: int):
    service = Service.query.get_or_404(service_id)
    if request.method == "POST":
        display_name = request.form.get("display_name", "").strip()
        if not display_name:
            flash("A display name is required.")
            return render_template("service_form.html", service=service)
        service.display_name = display_name
        service.description = request.form.get("description", "").strip()
        service.internal_description = request.form.get("internal_description", "").strip()
        service.price = parse_money_value(request.form.get("price"))
        service.cost = parse_money_value(request.form.get("cost"))
        service.buffer_minutes = parse_int(request.form.get("buffer_minutes"), default=0)
        db.session.commit()
        flash("Service updated.")
        return redirect(url_for("main.services"))
    return render_template("service_form.html", service=service)


@main_bp.route("/promotions")
@stylist_login_required
def promotions():
    promotion_list = (
        Promotion.query.options(selectinload(Promotion.service), selectinload(Promotion.eligible_clients))
        .order_by(Promotion.created_at.desc(), Promotion.title.asc())
        .all()
    )
    return render_template("promotions.html", promotions=promotion_list)


def render_promotion_form_template(promotion: Promotion | None):
    return render_template(
        "promotion_form.html",
        promotion=promotion,
        services=Service.query.order_by(Service.display_name.asc()).all(),
        clients=Client.query.order_by(Client.last_name.asc(), Client.first_name.asc()).all(),
    )


@main_bp.route("/promotions/new", methods=["GET", "POST"])
@stylist_login_required
def create_promotion():
    if request.method == "POST":
        title = request.form.get("title", "").strip()
        service_id = parse_int(request.form.get("service_id"))
        service = Service.query.get(service_id) if service_id else None
        date_rule = request.form.get("date_rule", "always").strip().lower()
        custom_date = parse_date_value(request.form.get("custom_date"))
        if not title:
            flash("A promotion title is required.")
            return render_promotion_form_template(None)
        if not service:
            flash("Please choose a service for this promotion.")
            return render_promotion_form_template(None)
        promotion = Promotion(
            title=title,
            description=request.form.get("description", "").strip(),
            service_id=service.id,
            discount_percent=parse_discount_percent(request.form.get("discount_percent")),
            date_rule=date_rule,
            custom_date=custom_date,
            days_before=parse_int(request.form.get("days_before"), default=0),
            days_after=parse_int(request.form.get("days_after"), default=0),
            applies_to_all_clients=request.form.get("audience") == "all",
            active=request.form.get("active", "on") == "on",
        )
        selected_client_ids = []
        for value in request.form.getlist("client_ids"):
            try:
                selected_client_ids.append(int(value))
            except (TypeError, ValueError):
                continue
        if not promotion.applies_to_all_clients and not selected_client_ids:
            flash("Choose at least one client or select all clients.")
            return render_promotion_form_template(None)
        if promotion.discount_percent <= Decimal("0.00"):
            flash("Enter a discount percentage greater than 0.")
            return render_promotion_form_template(None)
        if promotion.date_rule == "birthday" and not promotion.applies_to_all_clients:
            selected_clients = Client.query.filter(Client.id.in_(selected_client_ids)).all() if selected_client_ids else []
            if any(client.birthdate is None for client in selected_clients):
                flash("Every selected client needs a birthdate saved for a birthday promotion.")
                return render_promotion_form_template(None)
        if promotion.date_rule == "custom" and not promotion.custom_date:
            flash("Choose the custom date for this promotion.")
            return render_promotion_form_template(None)
        promotion.eligible_clients = Client.query.filter(Client.id.in_(selected_client_ids)).all() if selected_client_ids else []
        db.session.add(promotion)
        db.session.commit()
        flash("Promotion saved.")
        return redirect(url_for("main.promotions"))
    return render_promotion_form_template(None)


@main_bp.route("/promotions/<int:promotion_id>/edit", methods=["GET", "POST"])
@stylist_login_required
def edit_promotion(promotion_id: int):
    promotion = Promotion.query.get_or_404(promotion_id)
    if request.method == "POST":
        title = request.form.get("title", "").strip()
        service_id = parse_int(request.form.get("service_id"))
        service = Service.query.get(service_id) if service_id else None
        date_rule = request.form.get("date_rule", "always").strip().lower()
        custom_date = parse_date_value(request.form.get("custom_date"))
        if not title:
            flash("A promotion title is required.")
            return render_promotion_form_template(promotion)
        if not service:
            flash("Please choose a service for this promotion.")
            return render_promotion_form_template(promotion)
        selected_client_ids = []
        for value in request.form.getlist("client_ids"):
            try:
                selected_client_ids.append(int(value))
            except (TypeError, ValueError):
                continue
        promotion.title = title
        promotion.description = request.form.get("description", "").strip()
        promotion.service_id = service.id
        promotion.discount_percent = parse_discount_percent(request.form.get("discount_percent"))
        promotion.date_rule = date_rule
        promotion.custom_date = custom_date
        promotion.days_before = parse_int(request.form.get("days_before"), default=0)
        promotion.days_after = parse_int(request.form.get("days_after"), default=0)
        promotion.applies_to_all_clients = request.form.get("audience") == "all"
        promotion.active = request.form.get("active", "on") == "on"
        if promotion.discount_percent <= Decimal("0.00"):
            flash("Enter a discount percentage greater than 0.")
            return render_promotion_form_template(promotion)
        if promotion.date_rule == "birthday" and not promotion.applies_to_all_clients:
            selected_clients = Client.query.filter(Client.id.in_(selected_client_ids)).all() if selected_client_ids else []
            if any(client.birthdate is None for client in selected_clients):
                flash("Every selected client needs a birthdate saved for a birthday promotion.")
                return render_promotion_form_template(promotion)
        if promotion.date_rule == "custom" and not promotion.custom_date:
            flash("Choose the custom date for this promotion.")
            return render_promotion_form_template(promotion)
        if not promotion.applies_to_all_clients and not selected_client_ids:
            flash("Choose at least one client or select all clients.")
            return render_promotion_form_template(promotion)
        promotion.eligible_clients = Client.query.filter(Client.id.in_(selected_client_ids)).all() if selected_client_ids else []
        db.session.commit()
        flash("Promotion updated.")
        return redirect(url_for("main.promotions"))
    return render_promotion_form_template(promotion)


@main_bp.route("/clients/<int:client_id>")
@stylist_login_required
def client_profile(client_id: int):
    client = Client.query.get_or_404(client_id)
    date_range = request.args.get("range", "upcoming")
    start_date = parse_date_value(request.args.get("start_date"))
    end_date = parse_date_value(request.args.get("end_date"))

    query = Appointment.query.filter_by(client_id=client.id)
    now = datetime.utcnow()

    if start_date:
        query = query.filter(Appointment.start_datetime >= datetime.combine(start_date, time.min))
    if end_date:
        query = query.filter(Appointment.start_datetime <= datetime.combine(end_date, time.max))

    if not start_date and not end_date:
        if date_range == "past":
            query = query.filter(Appointment.start_datetime < now).order_by(Appointment.start_datetime.desc())
        elif date_range == "all":
            query = query.order_by(Appointment.start_datetime.desc())
        else:
            date_range = "upcoming"
            query = query.filter(Appointment.start_datetime >= now).order_by(Appointment.start_datetime.asc())
    else:
        query = query.order_by(Appointment.start_datetime.desc())

    appointments = query.all()
    return render_template(
        "client_profile.html",
        client=client,
        appointments=appointments,
        date_range=date_range,
        start_date=start_date.isoformat() if start_date else "",
        end_date=end_date.isoformat() if end_date else "",
    )


@main_bp.route("/clients/new", methods=["GET", "POST"])
@stylist_login_required
def create_client():
    if request.method == "POST":
        photo_filename = save_photo(request.files.get("photo"))
        client = Client(
            first_name=request.form["first_name"].strip(),
            last_name=request.form["last_name"].strip(),
            birthdate=parse_date_value(request.form.get("birthdate")),
            phone=request.form.get("phone", "").strip(),
            email=request.form.get("email", "").strip(),
            notes=request.form.get("notes", "").strip(),
            is_regular=request.form.get("is_regular") == "on",
            photo_filename=photo_filename,
        )
        db.session.add(client)
        db.session.commit()
        flash("Client added to your schedule book.")
        return redirect(url_for("main.clients"))
    return render_template("client_form.html", client=None)


@main_bp.route("/clients/<int:client_id>/edit", methods=["GET", "POST"])
@stylist_login_required
def edit_client(client_id: int):
    client = Client.query.get_or_404(client_id)
    if request.method == "POST":
        client.first_name = request.form["first_name"].strip()
        client.last_name = request.form["last_name"].strip()
        client.birthdate = parse_date_value(request.form.get("birthdate"))
        client.phone = request.form.get("phone", "").strip()
        client.email = request.form.get("email", "").strip()
        client.notes = request.form.get("notes", "").strip()
        client.is_regular = request.form.get("is_regular") == "on"
        new_photo = save_photo(request.files.get("photo"))
        if new_photo:
            client.photo_filename = new_photo
        db.session.commit()
        flash("Client details updated.")
        return redirect(url_for("main.clients"))
    return render_template("client_form.html", client=client)


@main_bp.route("/clients/<int:client_id>/delete", methods=["GET", "POST"])
@stylist_login_required
def delete_client(client_id: int):
    client = Client.query.get_or_404(client_id)
    expected_name = client.full_name

    if request.method == "POST":
        confirmation_name = request.form.get("confirmation_name", "").strip()
        if confirmation_name != expected_name:
            flash(f'Type "{expected_name}" exactly to delete this client.')
            return render_template("client_delete.html", client=client, expected_name=expected_name)

        delete_photo(client.photo_filename)
        db.session.delete(client)
        db.session.commit()
        flash(f"{expected_name} was deleted.")
        return redirect(url_for("main.clients"))

    return render_template("client_delete.html", client=client, expected_name=expected_name)


@main_bp.route("/schedule")
@stylist_login_required
def schedule():
    for series in RecurringSeries.query.filter_by(active=True).all():
        generate_future_appointments(series)
    db.session.commit()

    now = datetime.utcnow()
    start = now
    end = start + timedelta(days=21)
    appointments = (
        Appointment.query.join(Client)
        .filter(Appointment.start_datetime >= start, Appointment.start_datetime <= end)
        .order_by(Appointment.start_datetime.asc())
        .all()
    )

    selected_month = parse_month(request.args.get("month"))
    month_start, next_month = month_bounds(selected_month)
    month_appointments = (
        Appointment.query.join(Client)
        .filter(Appointment.start_datetime >= month_start, Appointment.start_datetime < next_month)
        .order_by(Appointment.start_datetime.asc())
        .all()
    )
    month_grid = build_month_grid(month_start, month_appointments)
    selected_date_value = request.args.get("selected_date")
    selected_date = None
    daily_appointments = []
    if selected_date_value:
        try:
            selected_date = datetime.strptime(selected_date_value, "%Y-%m-%d").date()
            day_start = datetime.combine(selected_date, time.min)
            day_end = datetime.combine(selected_date, time.max)
            daily_appointments = (
                Appointment.query.join(Client)
                .filter(Appointment.start_datetime >= day_start, Appointment.start_datetime <= day_end)
                .order_by(Appointment.start_datetime.asc())
                .all()
            )
        except ValueError:
            selected_date = None

    previous_month = (
        datetime(month_start.year - 1, 12, 1)
        if month_start.month == 1
        else datetime(month_start.year, month_start.month - 1, 1)
    )
    next_month_value = (
        datetime(month_start.year + 1, 1, 1)
        if month_start.month == 12
        else datetime(month_start.year, month_start.month + 1, 1)
    )

    return render_template(
        "schedule.html",
        appointments=appointments,
        now=now,
        month_grid=month_grid,
        selected_month=month_start,
        selected_date=selected_date,
        daily_appointments=daily_appointments,
        previous_month=previous_month.strftime("%Y-%m"),
        next_month_param=next_month_value.strftime("%Y-%m"),
    )


@main_bp.route("/unavailability", methods=["GET", "POST"])
@stylist_login_required
def manage_unavailability():
    if request.method == "POST":
        form_type = request.form.get("form_type", "time_off")
        if form_type == "working_hours":
            for weekday in range(7):
                entry = WorkingHours.query.filter_by(weekday=weekday).first()
                if not entry:
                    entry = WorkingHours(weekday=weekday)
                    db.session.add(entry)
                entry.is_active = request.form.get(f"active_{weekday}") == "on"
                entry.start_time = request.form.get(f"start_{weekday}") or None
                entry.end_time = request.form.get(f"end_{weekday}") or None
            db.session.commit()
            flash("Weekly working hours updated.")
            return redirect(url_for("main.manage_unavailability"))

        start_datetime = parse_datetime(request.form["start_datetime"])
        end_datetime = parse_datetime(request.form["end_datetime"])
        title = request.form["title"].strip() or "Time off"
        notes = request.form.get("notes", "").strip()
        is_recurring = request.form.get("is_recurring") == "on"
        is_all_day = request.form.get("is_all_day") == "on"

        if is_all_day:
            working_range = get_day_working_range(start_datetime.date())
            if not working_range:
                flash("All day time off needs working hours set for that day first.")
                return redirect(url_for("main.manage_unavailability"))
            start_datetime, end_datetime = working_range

        if end_datetime <= start_datetime:
            flash("Time off must end after it starts.")
            return redirect(url_for("main.manage_unavailability"))

        blocked_time = Unavailability(
            title=title,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            notes=notes,
            is_recurring=is_recurring,
        )
        if is_recurring:
            blocked_time.recurrence = "weekly"
            blocked_time.weekday = start_datetime.weekday()
            repeat_until_value = request.form.get("repeat_until")
            blocked_time.repeat_until = parse_datetime(f"{repeat_until_value}T23:59") if repeat_until_value else None
        db.session.add(blocked_time)
        db.session.commit()
        flash("Time off saved. Any conflicting clients now appear on the dashboard.")
        return redirect(url_for("main.manage_unavailability"))

    upcoming_unavailability = (
        Unavailability.query.filter(
            (Unavailability.is_recurring.is_(True)) | (Unavailability.end_datetime >= datetime.utcnow())
        )
        .order_by(Unavailability.start_datetime.asc())
        .all()
    )
    conflicting_appointments = collect_conflicting_appointments()
    working_hours = [
        get_working_hours_map().get(index) or WorkingHours(weekday=index, is_active=False)
        for index in range(7)
    ]
    return render_template(
        "unavailability.html",
        upcoming_unavailability=upcoming_unavailability,
        conflicting_appointments=conflicting_appointments,
        working_hours=working_hours,
        weekday_labels=WEEKDAY_LABELS,
    )


@main_bp.route("/unavailability/<int:unavailability_id>/edit", methods=["GET", "POST"])
@stylist_login_required
def edit_unavailability(unavailability_id: int):
    blocked_time = Unavailability.query.get_or_404(unavailability_id)

    if request.method == "POST":
        start_datetime = parse_datetime(request.form["start_datetime"])
        end_datetime = parse_datetime(request.form["end_datetime"])
        title = request.form["title"].strip() or "Time off"
        notes = request.form.get("notes", "").strip()
        is_recurring = request.form.get("is_recurring") == "on"
        is_all_day = request.form.get("is_all_day") == "on"

        if is_all_day:
            working_range = get_day_working_range(start_datetime.date())
            if not working_range:
                flash("All day time off needs working hours set for that day first.")
                return redirect(url_for("main.edit_unavailability", unavailability_id=blocked_time.id))
            start_datetime, end_datetime = working_range

        if end_datetime <= start_datetime:
            flash("Time off must end after it starts.")
            return redirect(url_for("main.edit_unavailability", unavailability_id=blocked_time.id))

        blocked_time.title = title
        blocked_time.start_datetime = start_datetime
        blocked_time.end_datetime = end_datetime
        blocked_time.notes = notes
        blocked_time.is_recurring = is_recurring
        blocked_time.recurrence = "weekly" if is_recurring else None
        blocked_time.weekday = start_datetime.weekday() if is_recurring else None
        repeat_until_value = request.form.get("repeat_until")
        blocked_time.repeat_until = parse_datetime(f"{repeat_until_value}T23:59") if is_recurring and repeat_until_value else None
        db.session.commit()
        flash("Time off updated.")
        return redirect(url_for("main.manage_unavailability"))

    return render_template(
        "edit_unavailability.html",
        blocked_time=blocked_time,
        weekday_labels=WEEKDAY_LABELS,
        is_all_day=is_all_day_unavailability(blocked_time),
    )


@main_bp.route("/appointments/new", methods=["GET", "POST"])
@stylist_login_required
def create_appointment():
    clients = Client.query.order_by(Client.last_name.asc()).all()
    service_options = get_service_options()
    selected_month, month_grid, previous_month, next_month_param = appointment_form_calendar(request.args.get("month"))
    if request.method == "POST":
        client_id = int(request.form["client_id"])
        start_datetime = parse_datetime(request.form["start_datetime"])
        duration_minutes = parse_int(request.form.get("duration_minutes"), default=60)
        selected_service_ids = selected_service_ids_from_request()
        selected_services = load_selected_services(selected_service_ids)
        buffer_minutes = calculate_service_buffer_minutes(selected_services)
        end_datetime = calculate_end_datetime(start_datetime, duration_minutes, buffer_minutes)
        title = request.form["title"].strip()
        notes = request.form.get("notes", "").strip()
        recurrence = request.form.get("recurrence", "none")

        within_hours, hours_message = within_working_hours(start_datetime, end_datetime)
        if recurrence == "none":
            if not within_hours:
                flash(f"{hours_message} Please select a different date.")
                return render_appointment_form_template(
                    clients=clients,
                    appointment=None,
                    selected_month=selected_month,
                    month_grid=month_grid,
                    previous_month=previous_month,
                    next_month_param=next_month_param,
                    show_series_options=False,
                    service_options=service_options,
                    selected_service_ids=selected_service_ids,
                )
        else:
            for occurrence_start, occurrence_end in recurring_occurrences(
                start_datetime,
                duration_minutes,
                buffer_minutes,
                recurrence,
            ):
                valid, recurring_hours_message = within_working_hours(occurrence_start, occurrence_end)
                if not valid:
                    flash(
                        f"The recurring schedule falls outside working hours on "
                        f"{occurrence_start.strftime('%b %d, %Y at %I:%M %p')}. Please select a different date."
                    )
                    return render_appointment_form_template(
                        clients=clients,
                        appointment=None,
                        selected_month=selected_month,
                        month_grid=month_grid,
                        previous_month=previous_month,
                        next_month_param=next_month_param,
                        show_series_options=False,
                        service_options=service_options,
                        selected_service_ids=selected_service_ids,
                    )

        if recurrence == "none":
            blocked_time = find_unavailability_overlap(start_datetime, end_datetime)
            if blocked_time:
                flash(
                    f"This time is unavailable because of {blocked_time.title}. Please select a different date."
                )
                return render_appointment_form_template(
                    clients=clients,
                    appointment=None,
                    selected_month=selected_month,
                    month_grid=month_grid,
                    previous_month=previous_month,
                    next_month_param=next_month_param,
                    show_series_options=False,
                    service_options=service_options,
                    selected_service_ids=selected_service_ids,
                )
        else:
            conflicting_occurrence = find_conflicting_occurrence(
                start_datetime,
                duration_minutes,
                buffer_minutes,
                recurrence,
            )
            if conflicting_occurrence:
                conflict_start, blocked_time = conflicting_occurrence
                flash(
                    f"The recurring schedule conflicts with {blocked_time.title} on "
                    f"{conflict_start.strftime('%b %d, %Y at %I:%M %p')}. Please select a different date."
                )
                return render_appointment_form_template(
                    clients=clients,
                    appointment=None,
                    selected_month=selected_month,
                    month_grid=month_grid,
                    previous_month=previous_month,
                    next_month_param=next_month_param,
                    show_series_options=False,
                    service_options=service_options,
                    selected_service_ids=selected_service_ids,
                )

        if recurrence == "none":
            appointment = Appointment(
                client_id=client_id,
                title=title,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                duration_minutes=duration_minutes,
                buffer_minutes=buffer_minutes,
                notes=notes,
            )
            appointment.services = selected_services
            db.session.add(appointment)
        else:
            series = RecurringSeries(
                client_id=client_id,
                title=title,
                recurrence=recurrence,
                interval_count=1,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                duration_minutes=duration_minutes,
                buffer_minutes=buffer_minutes,
            )
            series.services = selected_services
            db.session.add(series)
            db.session.flush()
            generate_future_appointments(series)

        db.session.commit()
        flash("Appointment saved.")
        return redirect(url_for("main.schedule"))
    return render_appointment_form_template(
        clients=clients,
        appointment=None,
        selected_month=selected_month,
        month_grid=month_grid,
        previous_month=previous_month,
        next_month_param=next_month_param,
        show_series_options=False,
        service_options=service_options,
        selected_service_ids=[],
    )


@main_bp.route("/appointments/<int:appointment_id>/edit", methods=["GET", "POST"])
@stylist_login_required
def edit_appointment(appointment_id: int):
    appointment = Appointment.query.get_or_404(appointment_id)
    clients = Client.query.order_by(Client.last_name.asc()).all()
    service_options = get_service_options()
    show_series_options = request.args.get("source") == "schedule"
    selected_month, month_grid, previous_month, next_month_param = appointment_form_calendar(
        request.args.get("month") or appointment.start_datetime.strftime("%Y-%m")
    )

    if request.method == "POST":
        appointment.client_id = int(request.form["client_id"])
        appointment.title = request.form["title"].strip()
        appointment.notes = request.form.get("notes", "").strip()
        new_start = parse_datetime(request.form["start_datetime"])
        duration_minutes = parse_int(request.form.get("duration_minutes"), default=appointment.duration_minutes or 60)
        selected_service_ids = selected_service_ids_from_request()
        selected_services = load_selected_services(selected_service_ids)
        buffer_minutes = calculate_service_buffer_minutes(selected_services, fallback_buffer=appointment.buffer_minutes or 0)
        new_end = calculate_end_datetime(new_start, duration_minutes, buffer_minutes)
        edit_scope = request.form.get("edit_scope", "single")
        apply_to_series = appointment.recurring_series_id and show_series_options and edit_scope == "series"
        recurrence = request.form.get("recurrence", "none")
        make_recurring = appointment.recurring_series_id is None and recurrence != "none"

        if appointment.recurring_series_id and apply_to_series:
            for occurrence_start, occurrence_end in recurring_occurrences(
                new_start,
                duration_minutes,
                buffer_minutes,
                appointment.recurring_series.recurrence,
            ):
                valid, recurring_hours_message = within_working_hours(occurrence_start, occurrence_end)
                if not valid:
                    flash(
                        f"The recurring schedule falls outside working hours on "
                        f"{occurrence_start.strftime('%b %d, %Y at %I:%M %p')}. Please select a different date."
                    )
                    return render_appointment_form_template(
                        clients=clients,
                        appointment=appointment,
                        selected_month=selected_month,
                        month_grid=month_grid,
                        previous_month=previous_month,
                        next_month_param=next_month_param,
                        show_series_options=show_series_options,
                        service_options=service_options,
                        selected_service_ids=selected_service_ids,
                    )
        elif make_recurring:
            for occurrence_start, occurrence_end in recurring_occurrences(
                new_start,
                duration_minutes,
                buffer_minutes,
                recurrence,
            ):
                valid, recurring_hours_message = within_working_hours(occurrence_start, occurrence_end)
                if not valid:
                    flash(
                        f"The recurring schedule falls outside working hours on "
                        f"{occurrence_start.strftime('%b %d, %Y at %I:%M %p')}. Please select a different date."
                    )
                    return render_appointment_form_template(
                        clients=clients,
                        appointment=appointment,
                        selected_month=selected_month,
                        month_grid=month_grid,
                        previous_month=previous_month,
                        next_month_param=next_month_param,
                        show_series_options=show_series_options,
                        service_options=service_options,
                        selected_service_ids=selected_service_ids,
                    )
        else:
            valid, hours_message = within_working_hours(new_start, new_end)
            if not valid:
                flash(f"{hours_message} Please select a different date.")
                return render_appointment_form_template(
                    clients=clients,
                    appointment=appointment,
                    selected_month=selected_month,
                    month_grid=month_grid,
                    previous_month=previous_month,
                    next_month_param=next_month_param,
                    show_series_options=show_series_options,
                    service_options=service_options,
                    selected_service_ids=selected_service_ids,
                )

        if appointment.recurring_series_id and apply_to_series:
            conflicting_occurrence = find_conflicting_occurrence(
                new_start,
                duration_minutes,
                buffer_minutes,
                appointment.recurring_series.recurrence,
            )
            if conflicting_occurrence:
                conflict_start, blocked_time = conflicting_occurrence
                flash(
                    f"The recurring schedule conflicts with {blocked_time.title} on "
                    f"{conflict_start.strftime('%b %d, %Y at %I:%M %p')}. Please select a different date."
                )
                return render_appointment_form_template(
                    clients=clients,
                    appointment=appointment,
                    selected_month=selected_month,
                    month_grid=month_grid,
                    previous_month=previous_month,
                    next_month_param=next_month_param,
                    show_series_options=show_series_options,
                    service_options=service_options,
                    selected_service_ids=selected_service_ids,
                )
        elif make_recurring:
            conflicting_occurrence = find_conflicting_occurrence(
                new_start,
                duration_minutes,
                buffer_minutes,
                recurrence,
            )
            if conflicting_occurrence:
                conflict_start, blocked_time = conflicting_occurrence
                flash(
                    f"The recurring schedule conflicts with {blocked_time.title} on "
                    f"{conflict_start.strftime('%b %d, %Y at %I:%M %p')}. Please select a different date."
                )
                return render_appointment_form_template(
                    clients=clients,
                    appointment=appointment,
                    selected_month=selected_month,
                    month_grid=month_grid,
                    previous_month=previous_month,
                    next_month_param=next_month_param,
                    show_series_options=show_series_options,
                    service_options=service_options,
                    selected_service_ids=selected_service_ids,
                )
        else:
            blocked_time = find_unavailability_overlap(new_start, new_end)
            if blocked_time:
                flash(
                    f"This time is unavailable because of {blocked_time.title}. Please select a different date."
                )
                return render_appointment_form_template(
                    clients=clients,
                    appointment=appointment,
                    selected_month=selected_month,
                    month_grid=month_grid,
                    previous_month=previous_month,
                    next_month_param=next_month_param,
                    show_series_options=show_series_options,
                    service_options=service_options,
                    selected_service_ids=selected_service_ids,
                )

        if appointment.recurring_series_id and apply_to_series:
            series = appointment.recurring_series
            series.client_id = appointment.client_id
            series.title = appointment.title
            series.start_datetime = new_start
            series.end_datetime = new_end
            series.duration_minutes = duration_minutes
            series.buffer_minutes = buffer_minutes
            series.services = selected_services
            appointment.services = selected_services
            refresh_series(series, from_datetime=appointment.start_datetime)
        elif make_recurring:
            series = RecurringSeries(
                client_id=appointment.client_id,
                title=appointment.title,
                recurrence=recurrence,
                interval_count=1,
                start_datetime=new_start,
                end_datetime=new_end,
                duration_minutes=duration_minutes,
                buffer_minutes=buffer_minutes,
                generated_until=new_start,
            )
            series.services = selected_services
            db.session.add(series)
            db.session.flush()
            appointment.start_datetime = new_start
            appointment.end_datetime = new_end
            appointment.duration_minutes = duration_minutes
            appointment.buffer_minutes = buffer_minutes
            appointment.recurring_series_id = series.id
            appointment.is_override = False
            appointment.override_reason = None
            appointment.services = selected_services
            generate_future_appointments(series)
        else:
            appointment.start_datetime = new_start
            appointment.end_datetime = new_end
            appointment.duration_minutes = duration_minutes
            appointment.buffer_minutes = buffer_minutes
            appointment.services = selected_services
            if appointment.recurring_series_id:
                appointment.is_override = True
                appointment.override_reason = "Adjusted without changing the recurring series."

        db.session.commit()
        flash("Appointment updated.")
        return redirect(url_for("main.schedule"))

    return render_appointment_form_template(
        clients=clients,
        appointment=appointment,
        selected_month=selected_month,
        month_grid=month_grid,
        previous_month=previous_month,
        next_month_param=next_month_param,
        show_series_options=show_series_options,
        service_options=service_options,
        selected_service_ids=[service.id for service in appointment.services],
    )


@main_bp.route("/requests/new", methods=["GET", "POST"])
@client_login_required
def create_change_request():
    appointments = (
        Appointment.query.filter(
            Appointment.client_id == g.client.id,
            Appointment.start_datetime >= datetime.utcnow(),
        )
        .order_by(Appointment.start_datetime.asc())
        .all()
    )
    service_options = get_service_options()
    next_appointment = appointments[0] if appointments else None
    pending_request = get_client_pending_request(g.client.id)
    if request.method == "POST":
        if pending_request:
            flash("You already have a schedule request pending. Please edit that request instead of creating a new one.")
            return redirect(url_for("main.create_change_request"))

        appointment_id_value = request.form.get("appointment_id") or None
        requested_date = request.form.get("requested_date")
        requested_start_time = request.form.get("requested_start_time")
        duration_minutes = parse_int(request.form.get("duration_minutes"), default=60)
        selected_service_ids = selected_service_ids_from_request()
        selected_services = load_selected_services(selected_service_ids)
        selected_promotion = selected_promotion_from_request()
        buffer_minutes = calculate_service_buffer_minutes(selected_services)
        if not requested_date or not requested_start_time:
            flash("Please choose a date on the calendar and enter a start time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=pending_request,
                form_values=build_change_request_form_values(),
            )

        requested_start = datetime.strptime(f"{requested_date}T{requested_start_time}", DATETIME_FORMAT)
        request_allowed, request_message = request_time_is_allowed(requested_start)
        if not request_allowed:
            flash(f"{request_message} Please choose a different date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=pending_request,
                form_values=build_change_request_form_values(),
            )
        if appointment_id_value:
            linked_appointment = Appointment.query.get(int(appointment_id_value))
            if linked_appointment:
                if linked_appointment.client_id != g.client.id:
                    flash("Please choose one of your own appointments.")
                    return render_change_request_template(
                        appointments=appointments,
                        next_appointment=next_appointment,
                        service_options=service_options,
                        selected_service_ids=selected_service_ids,
                        pending_request=pending_request,
                        form_values=build_change_request_form_values(),
                    )
                duration_minutes = linked_appointment.duration_minutes
                if not selected_service_ids:
                    selected_services = list(linked_appointment.services)
                    selected_service_ids = [service.id for service in selected_services]
                buffer_minutes = calculate_service_buffer_minutes(
                    selected_services,
                    fallback_buffer=linked_appointment.buffer_minutes or 0,
                )
        if selected_promotion and not promotion_is_available_to_client(
            selected_promotion,
            g.client,
            selected_services,
            reference_date=requested_start.date(),
        ):
            flash("That promotion is not available for the services on this request.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                selected_promotion_id=str(selected_promotion.id),
                pending_request=pending_request,
                form_values=build_change_request_form_values(),
            )
        requested_end = calculate_end_datetime(requested_start, duration_minutes, buffer_minutes)
        if requested_end <= requested_start:
            flash("Length and buffer must leave time for the appointment.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=pending_request,
                form_values=build_change_request_form_values(),
            )

        valid, hours_message = within_working_hours(requested_start, requested_end)
        if not valid:
            flash(f"{hours_message} Please choose a different date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=pending_request,
                form_values=build_change_request_form_values(),
            )
        blocked_time = find_unavailability_overlap(requested_start, requested_end)
        if blocked_time:
            flash("That time is not available. Please choose another date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=pending_request,
                form_values=build_change_request_form_values(),
            )
        existing_appointment = find_appointment_overlap(requested_start, requested_end)
        if existing_appointment:
            flash("That time is already booked. Please choose another date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=pending_request,
                form_values=build_change_request_form_values(),
            )

        request_record = ChangeRequest(
            client_id=g.client.id,
            appointment_id=int(appointment_id_value) if appointment_id_value else None,
            promotion=selected_promotion,
            requested_start=requested_start,
            requested_end=requested_end,
            message=request.form.get("message", "").strip(),
        )
        request_record.services = selected_services
        db.session.add(request_record)
        db.session.commit()
        flash("Schedule request submitted. It is now pending review.")
        return redirect(url_for("main.create_change_request"))
    return render_change_request_template(
        appointments=appointments,
        next_appointment=next_appointment,
        service_options=service_options,
        selected_service_ids=[],
        pending_request=pending_request,
        form_values=build_change_request_form_values(),
    )


@main_bp.route("/requests/<int:request_id>/edit", methods=["GET", "POST"])
@client_login_required
def edit_change_request(request_id: int):
    change_request = ChangeRequest.query.get_or_404(request_id)
    if change_request.client_id != g.client.id:
        flash("You can only edit your own requests.")
        return redirect(url_for("main.create_change_request"))
    if change_request.status != "pending":
        flash("Only pending requests can still be edited.")
        return redirect(url_for("main.create_change_request"))

    other_pending_request = get_client_pending_request(g.client.id, exclude_request_id=change_request.id)
    if other_pending_request:
        flash("You already have another schedule request pending.")
        return redirect(url_for("main.create_change_request"))

    appointments = (
        Appointment.query.filter(
            Appointment.client_id == g.client.id,
            Appointment.start_datetime >= datetime.utcnow(),
        )
        .order_by(Appointment.start_datetime.asc())
        .all()
    )
    service_options = get_service_options()
    next_appointment = appointments[0] if appointments else None

    if request.method == "POST":
        appointment_id_value = request.form.get("appointment_id") or None
        requested_date = request.form.get("requested_date")
        requested_start_time = request.form.get("requested_start_time")
        duration_minutes = parse_int(request.form.get("duration_minutes"), default=60)
        selected_service_ids = selected_service_ids_from_request()
        selected_services = load_selected_services(selected_service_ids)
        selected_promotion = selected_promotion_from_request()
        buffer_minutes = calculate_service_buffer_minutes(selected_services)
        if not requested_date or not requested_start_time:
            flash("Please choose a date on the calendar and enter a start time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=change_request,
                editing_request=change_request,
                form_values=build_change_request_form_values(change_request),
            )

        requested_start = datetime.strptime(f"{requested_date}T{requested_start_time}", DATETIME_FORMAT)
        request_allowed, request_message = request_time_is_allowed(requested_start)
        if not request_allowed:
            flash(f"{request_message} Please choose a different date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=change_request,
                editing_request=change_request,
                form_values=build_change_request_form_values(change_request),
            )
        if appointment_id_value:
            linked_appointment = Appointment.query.get(int(appointment_id_value))
            if linked_appointment:
                if linked_appointment.client_id != g.client.id:
                    flash("Please choose one of your own appointments.")
                    return render_change_request_template(
                        appointments=appointments,
                        next_appointment=next_appointment,
                        service_options=service_options,
                        selected_service_ids=selected_service_ids,
                        pending_request=change_request,
                        editing_request=change_request,
                        form_values=build_change_request_form_values(change_request),
                    )
                duration_minutes = linked_appointment.duration_minutes
                if not selected_service_ids:
                    selected_services = list(linked_appointment.services)
                    selected_service_ids = [service.id for service in selected_services]
                buffer_minutes = calculate_service_buffer_minutes(
                    selected_services,
                    fallback_buffer=linked_appointment.buffer_minutes or 0,
                )
        if selected_promotion and not promotion_is_available_to_client(
            selected_promotion,
            g.client,
            selected_services,
            reference_date=requested_start.date(),
        ):
            flash("That promotion is not available for the services on this request.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                selected_promotion_id=str(selected_promotion.id),
                pending_request=change_request,
                editing_request=change_request,
                form_values=build_change_request_form_values(change_request),
            )
        requested_end = calculate_end_datetime(requested_start, duration_minutes, buffer_minutes)
        if requested_end <= requested_start:
            flash("Length and buffer must leave time for the appointment.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=change_request,
                editing_request=change_request,
                form_values=build_change_request_form_values(change_request),
            )

        valid, hours_message = within_working_hours(requested_start, requested_end)
        if not valid:
            flash(f"{hours_message} Please choose a different date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=change_request,
                editing_request=change_request,
                form_values=build_change_request_form_values(change_request),
            )
        blocked_time = find_unavailability_overlap(requested_start, requested_end)
        if blocked_time:
            flash("That time is not available. Please choose another date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=change_request,
                editing_request=change_request,
                form_values=build_change_request_form_values(change_request),
            )
        existing_appointment = find_appointment_overlap(
            requested_start,
            requested_end,
            ignore_appointment_id=change_request.appointment_id,
        )
        if existing_appointment:
            flash("That time is already booked. Please choose another date or time.")
            return render_change_request_template(
                appointments=appointments,
                next_appointment=next_appointment,
                service_options=service_options,
                selected_service_ids=selected_service_ids,
                pending_request=change_request,
                editing_request=change_request,
                form_values=build_change_request_form_values(change_request),
            )

        change_request.appointment_id = int(appointment_id_value) if appointment_id_value else None
        change_request.promotion = selected_promotion
        change_request.requested_start = requested_start
        change_request.requested_end = requested_end
        change_request.message = request.form.get("message", "").strip()
        change_request.services = selected_services
        db.session.commit()
        flash("Pending schedule request updated.")
        return redirect(url_for("main.create_change_request"))

    return render_change_request_template(
        appointments=appointments,
        next_appointment=next_appointment,
        service_options=service_options,
        selected_service_ids=[service.id for service in change_request.services],
        selected_promotion_id=str(change_request.promotion_id or ""),
        pending_request=change_request,
        editing_request=change_request,
        form_values=build_change_request_form_values(change_request),
    )


@main_bp.route("/requests/<int:request_id>/<action>", methods=["POST"])
@stylist_login_required
def handle_change_request(request_id: int, action: str):
    change_request = ChangeRequest.query.get_or_404(request_id)
    if action == "approve":
        valid, hours_message = within_working_hours(change_request.requested_start, change_request.requested_end)
        if not valid:
            flash(f"Cannot approve this request because it falls outside working hours. {hours_message}")
            return redirect(url_for("main.dashboard"))
        blocked_time = find_unavailability_overlap(change_request.requested_start, change_request.requested_end)
        if blocked_time:
            flash(
                f"Cannot approve this request because the stylist is unavailable for {blocked_time.title}."
            )
            return redirect(url_for("main.dashboard"))
        change_request.status = "approved"
        if change_request.appointment:
            change_request.appointment.start_datetime = change_request.requested_start
            change_request.appointment.end_datetime = change_request.requested_end
            if change_request.services:
                change_request.appointment.services = list(change_request.services)
            change_request.appointment.promotion = change_request.promotion
            if change_request.appointment.recurring_series_id:
                change_request.appointment.is_override = True
                change_request.appointment.override_reason = "Approved from client request."
        flash("Change request approved and schedule updated.")
    elif action == "deny":
        change_request.status = "denied"
        flash("Change request denied.")
    db.session.commit()
    return redirect(url_for("main.dashboard"))
