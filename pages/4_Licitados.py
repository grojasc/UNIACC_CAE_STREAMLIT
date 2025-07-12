import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
from helpers.file_reader import read_any_file

st.set_page_config(page_title="Licitados", page_icon="📑")


def to_excel_bytes(df: pd.DataFrame) -> BytesIO:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer


def process_licitados_1(df_licitados: pd.DataFrame, df_csv: pd.DataFrame):
    df_licitados = df_licitados.copy()
    df_csv = df_csv.copy()
    df_licitados["RUT"] = df_licitados["RUT"].astype(str)
    df_licitados["PORCENTAJE_AVANCE"] = df_licitados["PORCENTAJE_AVANCE"].round(0)
    df_csv["RUT"] = df_csv["RUT"].astype(str)
    if "MOROSOS" not in df_csv.columns:
        df_csv["MOROSOS"] = ""
    keep_cols = [
        "RUT",
        "IES_RESPALDO",
        "NOMBRE_IES_RESPALDO",
        "GLOSA_NUEVO",
        "GLOSA_SUPERIOR",
        "NO_VIDENTE",
        "ESTUDIOS_EXTRANJEROS",
        "EXTRANJERO",
        "INFORMADO_CON_BEA",
        "PSU_USADA",
        "ACREDITACION_EXTRANJEROS_PDI",
        "MOROSOS",
    ]
    df_csv = df_csv[keep_cols]
    df_res = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
    df_res["RUT"] = df_res["RUT"].str.zfill(8)

    cond_gnew = df_res["GLOSA_NUEVO"] == "Seleccionado Normal ESTADO_SELECCION = 1 - 2 - 3"
    cond_gsup = df_res["GLOSA_SUPERIOR"] == "Seleccionado Normal ESTADO_SELECCION = 1 - 2 - 3"
    cond_ies = df_res["CODIGO_IES"] == "013"
    mask = (cond_gnew | cond_gsup) & cond_ies

    df_cumple = df_res[mask].copy()
    df_no_cumple = df_res[~mask].copy()

    def generar_observacion(row):
        obs = []
        if row.get("NO_VIDENTE", 0) == 1:
            obs.append("no vidente")
        if row.get("ESTUDIOS_EXTRANJEROS", 0) == 1:
            obs.append("estudios extranjeros")
        if row.get("EXTRANJERO", 0) == 1 or row.get("ACREDITACION_EXTRANJEROS_PDI", 0) == 1:
            obs.append("extranjeros PDI")
        if row.get("INFORMADO_CON_BEA", 0) == 1:
            obs.append("BEA")
        psu = row.get("PSU_USADA", 0)
        if pd.notnull(psu) and psu >= 485:
            obs.append("cumple PSU")
        if row.get("MOROSOS", 0) == 1:
            obs.append("morosos")
        return ", ".join(dict.fromkeys(obs))

    df_cumple["OBSERVACIONES"] = df_cumple.apply(generar_observacion, axis=1)
    return df_res, df_cumple, df_no_cumple


def process_licitados_2(df_licitados: pd.DataFrame, df_csv: pd.DataFrame, year_ref: int):
    df_licitados = df_licitados.copy()
    df_csv = df_csv.copy()
    df_licitados["PORCENTAJE_AVANCE"] = df_licitados["PORCENTAJE_AVANCE"].round(0)
    df_csv["RUT"] = df_csv["RUT"].astype(str)
    keep_cols = [
        "RUT",
        "IES_RESPALDO",
        "NOMBRE_IES_RESPALDO",
        "GLOSA_NUEVO",
        "GLOSA_SUPERIOR",
        "NO_VIDENTE",
        "ESTUDIOS_EXTRANJEROS",
        "EXTRANJERO",
        "INFORMADO_CON_BEA",
        "PSU_USADA",
        "ACREDITACION_EXTRANJEROS_PDI",
        "MOROSO",
    ]
    df_csv = df_csv[keep_cols]
    df_cruce = pd.merge(df_licitados, df_csv, on="RUT", how="inner")

    c1 = (
        (df_cruce["GLOSA_NUEVO"] == "PRESELECCIONADOS DE 1ER AÑO CON RESTRICCIÓN CFT/IP (CORTE 1)")
        & (df_cruce["GLOSA_SUPERIOR"] != "PRESELECCIONADOS DE CURSO SUPERIOR (CORTE 1)")
    )
    c2 = (
        (df_cruce["GLOSA_NUEVO"] == "ELIMINADO POR NO ELEGIBLE ACADÉMICAMENTE PARA 1ER AÑO")
        & (df_cruce["GLOSA_SUPERIOR"] != "PRESELECCIONADOS DE CURSO SUPERIOR (CORTE 1)")
    )
    cond_gnew = ~(c1 | c2)

    cond_primer_anio = df_cruce["GLOSA_SUPERIOR"] == "Preseleccionados de Curso Superior (corte 1)"
    cond_curso_superior = (
        (df_cruce["GLOSA_SUPERIOR"] == "Preseleccionados de Curso Superior (corte 1)")
        & ((df_cruce["AÑO_INGRESO_CARRERA"] < year_ref) & (df_cruce["PORCENTAJE_AVANCE"] >= 70))
    )
    cond_eliminado = (
        (df_cruce["GLOSA_SUPERIOR"] == "Eliminado por no respaldo para curso superior")
        & (df_cruce["AÑO_INGRESO_CARRERA"] < year_ref)
        & (df_cruce["PORCENTAJE_AVANCE"] >= 70)
    )

    cond_gsup = cond_primer_anio | cond_curso_superior | cond_eliminado
    mask = cond_gnew & cond_gsup

    df_cruce["RUT"] = df_cruce["RUT"].str.zfill(8)
    df_cumple = df_cruce[mask].copy()
    df_no_cumple = df_cruce[~mask].copy()

    def generar_observacion(row):
        obs = []
        if row.get("NO_VIDENTE", 0) == 1:
            obs.append("no vidente")
        if row.get("ESTUDIOS_EXTRANJEROS", 0) == 1:
            obs.append("estudios extranjeros")
        if row.get("EXTRANJERO", 0) == 1 or row.get("ACREDITACION_EXTRANJEROS_PDI", 0) == 1:
            obs.append("extranjeros PDI")
        if row.get("INFORMADO_CON_BEA", 0) == 1:
            obs.append("BEA")
        psu = row.get("PSU_USADA", 0)
        if pd.notnull(psu) and (psu / 100) >= 485:
            obs.append("cumple PSU")
        if row.get("MOROSO", 0) == 1:
            obs.append("Morosos")
        return ", ".join(dict.fromkeys(obs))

    df_cumple["OBSERVACIONES"] = df_cumple.apply(generar_observacion, axis=1)
    return df_cruce, df_cumple, df_no_cumple


def process_licitados_3(df_licitados: pd.DataFrame, df_csv: pd.DataFrame):
    df_licitados = df_licitados.copy()
    df_csv = df_csv.copy()
    df_licitados["PORCENTAJE_AVANCE"] = df_licitados["PORCENTAJE_AVANCE"].round(0)
    df_licitados["RUT"] = df_licitados["RUT"].astype(str)
    df_csv["RUT"] = df_csv["RUT"].astype(str)
    return pd.merge(df_licitados, df_csv, on="RUT", how="inner")


def process_rut(df_licitados: pd.DataFrame, df_csv: pd.DataFrame):
    df_licitados = df_licitados.copy()
    df_csv = df_csv.copy()
    df_licitados["PORCENTAJE_AVANCE"] = df_licitados["PORCENTAJE_AVANCE"].round(0)
    df_licitados["RUT"] = df_licitados["RUT"].astype(str)
    df_csv["RUT"] = df_csv["RUT"].astype(str)
    return pd.merge(df_licitados, df_csv, on="RUT", how="inner")


st.title("Licitados")

anio = st.number_input(
    "Año de ingreso", min_value=2000, max_value=2100, value=int(st.session_state.get("anio", datetime.now().year))
)
st.session_state["anio"] = int(anio)

base_file = st.file_uploader("Cargar base de licitados", type=["csv", "xlsx"], key="base")
if base_file:
    st.session_state["df_licitados"] = read_any_file(base_file)

df_licitados = st.session_state.get("df_licitados")
if df_licitados is not None:
    st.success(f"Base cargada: {len(df_licitados)} filas")
    dup = df_licitados[df_licitados.duplicated(subset=["RUT"], keep=False)]
    if not dup.empty:
        st.download_button(
            "Exportar duplicados",
            data=to_excel_bytes(dup),
            file_name="Duplicados_RUT.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

extra_file = st.file_uploader("Cargar refinanciamiento (extra)", type=["csv", "xlsx"], key="extra")
if extra_file:
    st.session_state["df_extra"] = read_any_file(extra_file)

df_extra = st.session_state.get("df_extra")

with st.expander("Sub-proceso #1 - Seleccionados"):
    file1 = st.file_uploader("Archivo #1", type=["csv", "xlsx"], key="sp1")
    if file1 and df_licitados is not None:
        df1 = read_any_file(file1)
        if df1 is not None:
            res1, cumple1, no_cumple1 = process_licitados_1(df_licitados, df1)
            st.session_state["res1"] = res1
            st.session_state["cumple1"] = cumple1
            st.session_state["nocumple1"] = no_cumple1
            st.success(f"Registros cruce: {len(res1)}")
            st.download_button(
                "Exportar cruce con Matrícula",
                to_excel_bytes(res1),
                "Licitados_Seleccionados_1.xlsx",
            )
            st.download_button(
                "Exportar cumple",
                to_excel_bytes(cumple1),
                "Licitados_1_B.xlsx",
            )
            st.download_button(
                "Exportar no cumple",
                to_excel_bytes(no_cumple1),
                "Licitados_1_C.xlsx",
            )
            if df_extra is not None:
                cruz = pd.merge(cumple1, df_extra, on="RUT", how="inner")
                st.download_button(
                    "Cruzar con Refinanciamiento",
                    to_excel_bytes(cruz),
                    "Cruce_Extra_1.xlsx",
                )

with st.expander("Sub-proceso #2 - Preseleccionados"):
    file2 = st.file_uploader("Archivo #2", type=["csv", "xlsx"], key="sp2")
    if file2 and df_licitados is not None:
        df2 = read_any_file(file2)
        if df2 is not None:
            res2, cumple2, no_cumple2 = process_licitados_2(df_licitados, df2, st.session_state["anio"])
            st.session_state["res2"] = res2
            st.session_state["cumple2"] = cumple2
            st.session_state["nocumple2"] = no_cumple2
            st.success(f"Registros cruce: {len(res2)}")
            st.download_button(
                "Exportar cruce con Matrícula",
                to_excel_bytes(res2),
                "Licitados_Preseleccionados_2.xlsx",
            )
            st.download_button(
                "Exportar Cumple",
                to_excel_bytes(cumple2),
                "Licitados_2_B.xlsx",
            )
            st.download_button(
                "Exportar No cumple",
                to_excel_bytes(no_cumple2),
                "Licitados_2_C.xlsx",
            )
            if df_extra is not None:
                cruz = pd.merge(res2, df_extra, on="RUT", how="inner")
                st.download_button(
                    "Cruzar con Refinanciamiento",
                    to_excel_bytes(cruz),
                    "Cruce_Extra_2.xlsx",
                )

with st.expander("Sub-proceso #3 - No seleccionados"):
    file3 = st.file_uploader("Archivo #3", type=["csv", "xlsx"], key="sp3")
    if file3 and df_licitados is not None:
        df3 = read_any_file(file3)
        if df3 is not None:
            res3 = process_licitados_3(df_licitados, df3)
            st.session_state["res3"] = res3
            st.success(f"Registros cruce: {len(res3)}")
            st.download_button(
                "Exportar cruce con Matrícula",
                to_excel_bytes(res3),
                "Licitados_NoSeleccionados_3.xlsx",
            )
            if df_extra is not None:
                cruz = pd.merge(res3, df_extra, on="RUT", how="inner")
                st.download_button(
                    "Cruzar con Refinanciamiento",
                    to_excel_bytes(cruz),
                    "Cruce_Extra_3.xlsx",
                )
                file3b = st.file_uploader("Archivo 3b (morosos)", type=["csv", "xlsx"], key="sp3b")
                if file3b:
                    df3b = read_any_file(file3b)
                    if df3b is not None:
                        df3b["RUT"] = df3b["RUT"].astype(str)
                        final3b = pd.merge(cruz, df3b, on="RUT", how="inner")
                        st.download_button(
                            "Exportar salida 3b",
                            to_excel_bytes(final3b),
                            "Salida_Final_3b.xlsx",
                        )

with st.expander("Sub-proceso RUT"):
    filerut = st.file_uploader("Archivo RUT", type=["csv", "xlsx"], key="rut")
    if filerut and df_licitados is not None:
        dfrut = read_any_file(filerut)
        if dfrut is not None:
            merge_rut = process_rut(df_licitados, dfrut)
            st.session_state["merge_rut"] = merge_rut
            st.success(f"Cruce obtenido: {len(merge_rut)} filas")
            st.download_button(
                "Exportar cruce con Matrícula",
                to_excel_bytes(merge_rut),
                "Cruce_Matricula_RUT.xlsx",
            )
            if df_extra is not None:
                rut_extra = pd.merge(merge_rut, df_extra, on="RUT", how="inner")
                st.download_button(
                    "Exportar cruce c/Refinanciamiento",
                    to_excel_bytes(rut_extra),
                    "Cruce_Refinanciamiento_RUT.xlsx",
                )
            st.download_button(
                "Exportar RUT-B",
                to_excel_bytes(dfrut),
                "RUT_B.xlsx",
            )
            st.download_button(
                "Exportar RUT-C",
                to_excel_bytes(dfrut),
                "RUT_C.xlsx",
            )

