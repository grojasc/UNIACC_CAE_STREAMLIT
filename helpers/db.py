from sqlalchemy import create_engine
import streamlit as st

@st.cache_data(show_spinner=False)
def get_connection(server, database, user, password, driver="ODBC Driver 17 for SQL Server"):
    conn_str = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    engine = create_engine(conn_str, fast_executemany=False)
    return engine.connect()
