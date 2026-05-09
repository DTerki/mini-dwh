from pathlib import Path
import os
from xmlrpc import server

import pyodbc
from dotenv import load_dotenv


def load_environment() -> None:
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    load_dotenv(env_path, override=False)


def get_connection(database: str | None = None) -> pyodbc.Connection:
    load_environment()

    server = os.getenv("SQL_SERVER")
    default_database = os.getenv("SQL_DATABASE")
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    encrypt = os.getenv("SQL_ENCRYPT", "no")
    trust_cert = os.getenv("SQL_TRUST_CERT", "yes")

    if not server:
        raise ValueError("Missing SQL_SERVER in .env")
    if not default_database and not database:
        raise ValueError("Missing SQL_DATABASE in .env")
    if not user:
        raise ValueError("Missing SQL_USER in .env")
    if not password:
        raise ValueError("Missing SQL_PASSWORD in .env")

    target_database = database or default_database

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={target_database};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
    )

    print(f"[DB CONNECT] server={server} database={target_database}")

    return pyodbc.connect(connection_string)