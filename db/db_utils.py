import os
from dotenv import load_dotenv

"""
Database Utility Module

This module provides utility functions for database operations.
It includes methods for retrieving connection strings and managing database sessions.
"""

load_dotenv()


class DatabaseUtils:
    @staticmethod
    def get_connection_string() -> str:
        db_host = os.getenv("DB_SERVER")
        db_port = os.getenv("DB_PORT", "1433")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")

        missing = [
            name
            for name, value in {
                "DB_SERVER": db_host,
                "DB_PORT": db_port,
                "DB_NAME": db_name,
                "DB_USERNAME": db_user,
                "DB_PASSWORD": db_password,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

        return f"mssql+pymssql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
