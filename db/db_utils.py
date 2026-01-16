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
        # Try Streamlit secrets first
        try:
            import streamlit as st

            secrets = st.secrets.get("database", {})
        except Exception:
            secrets = {}

        db_host = secrets.get("DB_SERVER") or os.getenv("DB_SERVER")
        db_port = secrets.get("DB_PORT") or os.getenv("DB_PORT", "1433")
        db_name = secrets.get("DB_NAME") or os.getenv("DB_NAME")
        db_user = secrets.get("DB_USERNAME") or os.getenv("DB_USERNAME")
        db_password = secrets.get("DB_PASSWORD") or os.getenv("DB_PASSWORD")

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
            raise RuntimeError(
                f"Missing configuration (env or st.secrets): {', '.join(missing)}"
            )

        return f"mssql+pymssql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
