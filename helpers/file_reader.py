import pandas as pd
import chardet
import streamlit as st
from io import BytesIO


def _detect_encoding(raw):
    result = chardet.detect(raw)
    return result["encoding"] or "utf-8"


@st.cache_data(show_spinner=False)
def read_any_file(uploaded_file: BytesIO) -> pd.DataFrame | None:
    if not uploaded_file:
        return None

    name = uploaded_file.name.lower()
    raw = uploaded_file.getvalue()
    if name.endswith((".csv", ".txt")):
        enc = _detect_encoding(raw[:20000])
        first_line = raw.decode(enc, errors="replace").splitlines()[0]
        delim = ";" if first_line.count(";") > first_line.count(",") else ","
        return pd.read_csv(BytesIO(raw), delimiter=delim, encoding=enc)
    elif name.endswith((".xlsx", ".xls")):
        return pd.read_excel(BytesIO(raw))
    st.error("Extensión no soportada")
    return None
