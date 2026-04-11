import os
import secrets
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

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


def configure_logging(app: Flask) -> None:
    log_level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    app.logger.handlers.clear()
    app.logger.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    app.logger.addHandler(console_handler)

    logs_path = Path(app.instance_path) / "logs"
    logs_path.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        logs_path / "app.log",
        maxBytes=1_048_576,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)
    app.logger.propagate = False

    @app.before_request
    def log_request_start():
        app.logger.info(
            "Request started method=%s path=%s remote_addr=%s client_id=%s stylist_user_id=%s",
            request.method,
            request.path,
            request.headers.get("X-Forwarded-For", request.remote_addr),
            session.get("client_id", ""),
            session.get("stylist_user_id", ""),
        )

    @app.after_request
    def log_request_end(response):
        app.logger.info(
            "Request completed method=%s path=%s status=%s",
            request.method,
            request.path,
            response.status_code,
        )
        return response

    def log_exception(sender, exception, **extra):
        sender.logger.exception("Unhandled application error on path=%s", request.path)

    from flask import got_request_exception

    got_request_exception.connect(log_exception, app)

    def fallback_endpoint() -> str:
        if session.get("stylist_user_id"):
            return "main.dashboard"
        if session.get("client_id"):
            return "main.create_change_request"
        return "main.home"

    @app.teardown_request
    def rollback_failed_requests(error):
        if error is not None:
            db.session.rollback()

    @app.errorhandler(404)
    def handle_not_found(error):
        return (
            render_template(
                "error.html",
                error_title="Page not found",
                error_message="That page is not available. Try one of the main navigation links instead.",
                action_url=url_for(fallback_endpoint()),
                action_label="Return to the app",
            ),
            404,
        )

    @app.errorhandler(400)
    @app.errorhandler(413)
    def handle_bad_request(error):
        message = "That request could not be completed. Please check your entry and try again."
        if getattr(error, "code", None) == 413:
            message = "That upload or request was too large. Please try a smaller file."
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            flash(message)
            return redirect(url_for(fallback_endpoint()))
        return (
            render_template(
                "error.html",
                error_title="Request problem",
                error_message=message,
                action_url=url_for(fallback_endpoint()),
                action_label="Go back",
            ),
            getattr(error, "code", 400),
        )

    @app.errorhandler(Exception)
    def handle_unexpected_error(error):
        if isinstance(error, HTTPException):
            return error
        db.session.rollback()
        if isinstance(error, SQLAlchemyError):
            app.logger.error("Database error handled gracefully on path=%s", request.path)
        message = "Something went wrong, and your last action was not completed."
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            flash(message)
            return redirect(url_for(fallback_endpoint()))
        return (
            render_template(
                "error.html",
                error_title="Something went wrong",
                error_message=message,
                action_url=url_for(fallback_endpoint()),
                action_label="Return safely",
            ),
            500,
        )


def create_app() -> Flask:
    app = Flask(__name__)

    instance_path = Path(app.instance_path)
    instance_path.mkdir(parents=True, exist_ok=True)

    app.config["SECRET_KEY"] = get_secret_key(instance_path)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{instance_path / 'scheduler.db'}"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["UPLOAD_FOLDER"] = str(instance_path / "uploads")

    Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)
    configure_logging(app)

    db.init_app(app)

    from .routes import main_bp

    app.register_blueprint(main_bp)

    with app.app_context():
        db.create_all()
        ensure_schema_updates()

    return app
