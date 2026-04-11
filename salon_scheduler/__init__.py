import os
import secrets
from pathlib import Path

from flask import Flask
from sqlalchemy import inspect, text

from .models import db


def get_secret_key(instance_path: Path) -> str:
    configured_secret = os.environ.get("FLASK_SECRET_KEY")
    if configured_secret:
        return configured_secret

    secret_file = instance_path / ".secret_key"
    if secret_file.exists():
        return secret_file.read_text(encoding="utf-8").strip()

    generated_secret = secrets.token_urlsafe(32)
    secret_file.write_text(generated_secret, encoding="utf-8")
    return generated_secret


def ensure_schema_updates() -> None:
    inspector = inspect(db.engine)
    changes = {
        "appointment": {
            "duration_minutes": "INTEGER NOT NULL DEFAULT 60",
            "buffer_minutes": "INTEGER NOT NULL DEFAULT 0",
            "promotion_id": "INTEGER",
        },
        "recurring_series": {
            "duration_minutes": "INTEGER NOT NULL DEFAULT 60",
            "buffer_minutes": "INTEGER NOT NULL DEFAULT 0",
        },
        "unavailability": {
            "is_recurring": "INTEGER NOT NULL DEFAULT 0",
            "recurrence": "VARCHAR(20)",
            "weekday": "INTEGER",
            "repeat_until": "DATETIME",
        },
        "stylist_user": {
            "reset_code_hash": "VARCHAR(255)",
            "minimum_notice_hours": "INTEGER NOT NULL DEFAULT 2",
            "booking_horizon_days": "INTEGER NOT NULL DEFAULT 90",
        },
        "service": {
            "buffer_minutes": "INTEGER NOT NULL DEFAULT 0",
        },
        "change_request": {
            "promotion_id": "INTEGER",
        },
        "client": {
            "birthdate": "DATE",
        },
        "promotion": {
            "date_rule": "VARCHAR(20) NOT NULL DEFAULT 'always'",
            "custom_date": "DATE",
            "days_before": "INTEGER NOT NULL DEFAULT 0",
            "days_after": "INTEGER NOT NULL DEFAULT 0",
        },
    }

    with db.engine.begin() as connection:
        for table_name, columns in changes.items():
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, definition in columns.items():
                if column_name not in existing_columns:
                    connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"))


def create_app() -> Flask:
    app = Flask(__name__)

    instance_path = Path(app.instance_path)
    instance_path.mkdir(parents=True, exist_ok=True)

    app.config["SECRET_KEY"] = get_secret_key(instance_path)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{instance_path / 'scheduler.db'}"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["UPLOAD_FOLDER"] = str(instance_path / "uploads")

    Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)

    db.init_app(app)

    from .routes import main_bp

    app.register_blueprint(main_bp)

    with app.app_context():
        db.create_all()
        ensure_schema_updates()

    return app
