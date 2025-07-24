import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import pandas as pd
import threading
import unicodedata
import re
from pathlib import Path
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys
import chardet
from pathlib import Path
from typing import Final, List
import datetime as _dt        # ya al inicio del script
import logging


###############################################################################
#                              UTILITY FUNCTIONS
###############################################################################


df_glosas = None
df_renovantes = None
df_pot_renovantes = None
df_preseleccion = None
df_resultado_11 = None
df_resultado_3_non_nan = None
df_resultado_4_non_nan = None
df_preseleccion_updated = None
df_licitados = None
df_paises = None
df_duplicated = None
df_cc = None
current_year = _dt.datetime.now().year 

#Fnuctions
def create_sql_server_connection(server, database, username, password, driver='ODBC Driver 17 for SQL Server', port='1433'):
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?trusted_connection=no&driver={driver}'
    try:
        engine = create_engine(connection_string, fast_executemany=False)
        conn = engine.connect()
        print(f'Conectado a {database} en {server}.')
        return conn
    except Exception as e:
        print(f'Error de conexión: {e}')
        return None

server = 'PUACSCLBI.uniacc.local'
username = 'usr_dwhppto'
password = 'g8)yT1m23u7H'
connection1 = create_sql_server_connection(server, 'DWH_DAI', username, password)
connection2 = create_sql_server_connection(server, 'UConectores', username, password)
# ───────────────── LOGGING ─────────────────
# ─────────── LOGGING ───────────
#logging.basicConfig(
#    level=logging.INFO,
#    format="%(asctime)s [%(levelname)s] %(message)s",
#    handlers=[
#        logging.FileHandler("solicitud_monto.log", mode="a", encoding="utf-8"),
#        logging.StreamHandler()
#    ]
#)
#log = logging.getLogger(__name__)

#============================================================================
#Utilities
#============================================================================

def _clean_rut(series: pd.Series) -> pd.Series:
    return (series.astype(str)
                  .str.strip()
                  .str.replace(r'\D', '', regex=True)
                  .str.zfill(8))
# --------------------------------------------------
#  Utilidades de normalización
# --------------------------------------------------
def clean_text(text):
    if isinstance(text, str):
        text = unicodedata.normalize('NFKD', text).encode('ASCII','ignore').decode('utf-8')
        text = text.upper()
        text = re.sub(r'[^A-ZÜ\s-]', '', text)
    return text


"""SeguimientosFrame – agrega columna VALIDACION_REGLAS
-----------------------------------------------------------------------------
• No modifica el valor original de `df_licitados`.
• Genera una columna `VALIDACION_REGLAS` con las reglas MINEDUC incumplidas
  (separadas por `; `). Si no hay faltas, queda vacía.
• Mantiene formato previo; sólo se emplea para validación.
"""
# --------------------------------------------------
#  Config MINEDUC & reglas simples (longitud, numérico)
# --------------------------------------------------
SPEC = {
    "RUT":            dict(len_=8,  numeric=True),
    "DV":             dict(len_=1,  numeric=False),
    "APELLIDO PATERNO":dict(max_=200),
    "APELLIDO MATERNO":dict(max_=200),
    "NOMBRES":        dict(max_=201),
    "CELULAR":        dict(len_=9,  numeric=True),
    "EMAIL":          dict(max_=200),
    "CODIGO CARRERA": dict(len_=4,  numeric=True),
    "JORNADA":        dict(len_=1,  numeric=True),
    "AÑO INGRESO CARRERA":dict(len_=4, numeric=True),
    "ARANCEL SOLICITADO":dict(len_=10, numeric=True),
    "ARANCEL REAL":   dict(len_=10, numeric=True),
    "CODIGO UNICO MINEDUC":dict(len_=24, numeric=False),
}
MINUD_COLS = list(SPEC.keys()) + ["ETAPA FIRMA"]
ALIASES = {
    #"ANO_INGRESO_CARRERA": "AÑO INGRESO CARRERA",
    "AÑO_INGRESO_CARRERA": "AÑO INGRESO CARRERA",
    # nombres con guion bajo desde la vista SQL
    "APELLIDO_PATERNO": "APELLIDO PATERNO",
    "APELLIDO_MATERNO": "APELLIDO MATERNO",
    "CODIGO_CARRERA": "CODIGO CARRERA",
    "ARANCEL_SOLICITADO": "ARANCEL SOLICITADO",
    "ARANCEL_REAL": "ARANCEL REAL",
    "CODIGO_UNICO_MINEDUC": "CODIGO UNICO MINEDUC",
    "CODIGO_UNICO_MINEDUC ": "CODIGO UNICO MINEDUC",  # por si trae espacio extra
}

# --------------------------------------------------
#  Utils
# --------------------------------------------------

def _upper_ascii(text):
    if pd.isna(text):
        return pd.NA
    txt = str(text)
    txt = unicodedata.normalize("NFKD", txt).encode("ASCII", "ignore").decode()
    return re.sub(r"\s{2,}", " ", txt.upper().strip())


def _ensure(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={k: v for k, v in ALIASES.items() if k in df.columns})
    for col in MINUD_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    return df


# --------------------------------------------------
#  Validación sin modificar valores
# --------------------------------------------------

def _is_na_scalar(v):
    """True si v es escalar NaN/NA, False en otro caso."""
    return (v is pd.NA) or (pd.isna(v) if not isinstance(v, (list, tuple, dict, pd.Series)) else False)


def validate_minud(df: pd.DataFrame) -> pd.DataFrame:
    """Valida reglas + formatea RUT/CELULAR/etc. al final."""
    df = _ensure(df.copy())
    issues_all = []
    for i, row in df.iterrows():
        issues = []
        for col, rule in SPEC.items():
            val = row[col]
            if _is_na_scalar(val):
                issues.append(f"{col}:VACIO")
                continue
            txt = str(val)
            if rule.get("numeric") and not txt.isdigit():
                issues.append(f"{col}:NO_NUM")
            if "len_" in rule and len(txt) != rule["len_"]:
                issues.append(f"{col}:LEN{len(txt)}")
            if "max_" in rule and len(txt) > rule.get("max_", len(txt)):
                issues.append(f"{col}:>{rule['max_']}")
        issues_all.append("; ".join(issues))

    df["VALIDACION_REGLAS"] = issues_all

    # Normaliza textos relevantes
    for col in ("APELLIDO PATERNO", "APELLIDO MATERNO", "NOMBRES", "EMAIL", "ETAPA FIRMA"):
        df[col] = df[col].apply(_upper_ascii)

    # ➜ Ahora SÍ se realiza padding para los campos numéricos final
    #for col, rule in SPEC.items():
    #    if rule.get("numeric") and "len_" in rule:
    #        df[col] = df[col].apply(lambda x: _pad_num(x, rule["len_"]))

    ordered_cols = MINUD_COLS + ["VALIDACION_REGLAS"]
    return df[ordered_cols][ordered_cols]

# --------------------------------------------------
#  Merge & clean (igual que antes, pero usa validate_minud)
# --------------------------------------------------

def merge_and_clean(df_base: pd.DataFrame, df_csv: pd.DataFrame):
    df_csv = df_csv[["RUT"]].copy()
    df_base = _ensure(df_base.copy())
    df_base["RUT"] = df_base["RUT"].astype(str)
    df_csv["RUT"] = df_csv["RUT"].astype(str)
    
    df_ok = pd.merge(df_base, df_csv, on="RUT", how="inner").drop_duplicates("RUT")
    df_nc = df_csv[~df_csv["RUT"].isin(df_ok["RUT"])]
    
    df_ok = validate_minud(df_ok)
    df_ok["RUT"] = df_ok["RUT"].str.zfill(8)
    
    return df_ok, df_nc.rename(columns={"RUT": "RUT SIN CRUCE"})
#########-----------------QUERY LICITADOS AL INICIO-------------------##################
query = """
        SELECT
    --  1  RUT (8)
    --RIGHT(REPLICATE('0', 8) + CAST(RUT AS varchar(50)), 8)               AS RUT,
    CAST(RUT AS varchar(50))               AS RUT,
    --  2  DV (1)
    UPPER(DV)                                                            AS DV,

    --  3-5 Apellidos / Nombres
    UPPER(APELLIDO_PATERNO)                                              AS APELLIDO_PATERNO,
    UPPER(APELLIDO_MATERNO)                                              AS APELLIDO_MATERNO,
    UPPER(NOMBRES)                                                       AS NOMBRES,

    --  6  SEXO (1)
    UPPER(SEXO)                                                          AS SEXO,

    --  7  FECHA_NACIMIENTO (10)  dd/MM/yyyy
    CONVERT(char(10), TRY_CONVERT(date, FECHA_NACIMIENTO), 103)          AS FECHA_NACIMIENTO,

    --  8  DIRECCION
    UPPER(DIRECCION)                                                     AS DIRECCION,

    --  9-11  CIUDAD (5) / COMUNA (5) / REGION (2)
    RIGHT(REPLICATE('0', 5) + CAST(CIUDAD AS varchar(50)), 5)            AS CIUDAD,
    RIGHT(REPLICATE('0', 5) + CAST(COMUNA AS varchar(50)), 5)            AS COMUNA,
    RIGHT(REPLICATE('0', 2) + CAST(REGION AS varchar(50)), 2)            AS REGION,

    -- 12-14  COD_AREA (2) / FONO_FIJO (8) / CELULAR (9)
    RIGHT(REPLICATE('0', 2) + CAST(COD_AREA   AS varchar(50)), 2)        AS COD_AREA,
    RIGHT(REPLICATE('0', 8) + CAST(FONO_FIJO  AS varchar(50)), 8)        AS FONO_FIJO,
    RIGHT(REPLICATE('0', 9) + CAST(CELULAR    AS varchar(50)), 9)        AS CELULAR,

    -- 15  EMAIL
    UPPER(EMAIL)                                                         AS EMAIL,

    -- 16-19  Códigos IES / Sede / Carrera
    RIGHT(REPLICATE('0', 1) + CAST(CODIGO_TIPO_IES AS varchar(50)), 1)   AS CODIGO_TIPO_IES,
    RIGHT(REPLICATE('0', 3) + CAST(CODIGO_DE_IES   AS varchar(50)), 3)   AS CODIGO_IES,
    RIGHT(REPLICATE('0', 3) + CAST(CODIGO_SEDE     AS varchar(50)), 3)   AS CODIGO_SEDE,
    RIGHT(REPLICATE('0', 4) + CAST(CODIGO_CARRERA  AS varchar(50)), 4)   AS CODIGO_CARRERA,

    -- 20-22  Jornada / Año ingreso / Nivel
    RIGHT(REPLICATE('0', 1) + CAST(JORNADA             AS varchar(50)), 1) AS JORNADA,
    RIGHT(REPLICATE('0', 4) + CAST(AÑO_INGRESO_CARRERA  AS varchar(50)), 4) AS ANO_INGRESO_CARRERA,
    RIGHT(REPLICATE('0', 1) + CAST(NIVEL_DE_ESTUDIOS    AS varchar(50)), 1) AS NIVEL_DE_ESTUDIOS,

    -- 23-24  Aranceles (10)
    -- 23-24  Aranceles formateados como texto fijo de 10 dígitos
    CONVERT(bigint, ARANCEL_SOLICITADO)  AS ARANCEL_SOLICITADO,
    CONVERT(bigint, ARANCEL_REAL)       AS ARANCEL_REAL,


    -- 25  Comprobante matrícula
    UPPER(COMPROBANTE_MATRICULA)                                         AS COMPROBANTE_MATRICULA,

    -- 26  Fecha última matrícula
    CONVERT(char(10), TRY_CONVERT(date, FECHA_ÚLTIMA_MATRICULA), 103)    AS FECHA_ULTIMA_MATRICULA,

    -- 27-30  Región / Comuna / Ciudad / Dirección Sede
    RIGHT(REPLICATE('0', 2) + CAST(REGION_SEDE AS varchar(50)), 2)       AS REGION_SEDE,
    RIGHT(REPLICATE('0', 5) + CAST(COMUNA_SEDE AS varchar(50)), 5)       AS COMUNA_SEDE,
    RIGHT(REPLICATE('0', 5) + CAST(CIUDAD_SEDE AS varchar(50)), 5)       AS CIUDAD_SEDE,
    UPPER(DIRECCIÓN_SEDE)                                               AS DIRECCION_SEDE,

    -- 31-32  Porcentaje avance (3) + Código único MINEDUC (24)
    PORCENTAJE_AVANCE,
    CODIGO_UNICO_MINEDUC,                                               -- 24 chars (sin cambio)
    AÑO_INGRESO_CARRERA
FROM dbo.vw_beneficios

        """


df_licitados = pd.read_sql_query(query, connection2)


def read_any_file(title="Seleccionar archivo"):
    """
    1) Abre un file dialog.
    2) Si la extensión es .csv o .txt => lee con pd.read_csv, detectando
       automáticamente el delimitador (, o ;) y la codificación.
    3) Si la extensión es .xlsx o .xls => pregunta la hoja a usar.
    4) Retorna (df, file_path) o (None, None) si se cancela o hay error.
    """
    
    file_path = filedialog.askopenfilename(
        title=title,
        filetypes=[
            ("Archivos CSV, TXT o Excel", "*.csv *.txt *.xlsx *.xls"),
            ("Todos", "*.*")
        ],
        initialdir=os.path.expanduser("~")
    )
    if not file_path:
        return None, None  # El usuario canceló

    ext = os.path.splitext(file_path)[1].lower()
    df = None
    
    try:
        if ext in [".csv", ".txt"]:
            # 1) Leemos una porción del archivo en binario para detectar encoding con chardet
            with open(file_path, 'rb') as f:
                raw_data = f.read(20000)  # Puedes ajustar el tamaño según tus necesidades
            result = chardet.detect(raw_data)
            detected_encoding = result['encoding']
            
            # Evitar que la codificación sea None (caso extremo de chardet)
            if not detected_encoding:
                detected_encoding = "utf-8"
            
            # 2) Leemos la primera línea en texto (usando la encoding detectada) para intentar
            #    determinar el delimitador principal (, o ;)
            with open(file_path, 'r', encoding=detected_encoding, errors='replace') as f:
                first_line = f.readline()
            
            # 3) Contamos qué separador aparece más en la primera línea
            commas = first_line.count(',')
            semicolons = first_line.count(';')
            
            if semicolons > commas:
                delimiter = ';'
            else:
                delimiter = ','
            
            # 4) Finalmente, leemos el archivo con pandas
            df = pd.read_csv(file_path, 
                             delimiter=delimiter, 
                             encoding=detected_encoding,
                             # Si quieres ignorar líneas con errores irreparables:
                             # on_bad_lines='skip',
                             # Si prefieres que lance excepción en lugar de ignorar:
                             # on_bad_lines='error',
                             )
        
        elif ext in [".xlsx", ".xls"]:
            xls = pd.ExcelFile(file_path)
            sheets = xls.sheet_names
            
            if len(sheets) == 1:
                sheet_name = sheets[0]
            else:
                # Pedimos al usuario que indique qué hoja usar
                sheet_name = simpledialog.askstring(
                    "Hoja de Excel",
                    f"Hojas disponibles:\n{', '.join(sheets)}\n\n"
                    "Escribe el nombre de la hoja a usar:"
                )
                if not sheet_name:
                    messagebox.showinfo("Cancelado", "No se seleccionó hoja.")
                    return None, None
                if sheet_name not in sheets:
                    messagebox.showerror("Hoja inválida", f"La hoja '{sheet_name}' no existe en el Excel.")
                    return None, None
            
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            messagebox.showerror("Error", f"Extensión de archivo no soportada: {ext}")
            return None, None

        return df, file_path
    
    except Exception as e:
        messagebox.showerror("Error al leer archivo", f"{e}")
        return None, None





# ============================================================== 
#               FRAMES PRINCIPALES
# ============================================================== 

class LoginFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        # ========== PATRÓN PARA OBTENER RUTA IMAGEN ========== 
        if hasattr(sys, '_MEIPASS'):
            # Cuando está empaquetado con PyInstaller
            base_path = sys._MEIPASS
        else:
            # Cuando corres el .py “normal”
            base_path = os.path.dirname(os.path.abspath(__file__))

        # Unir la carpeta y archivo de imagen
        logo_path = os.path.join(base_path, 'images', 'logo.png')
        
        # Cargar la imagen usando logo_path
        try:
            self.logo = tk.PhotoImage(file=logo_path)
            tk.Label(self, image=self.logo, bg="#FFFFFF").pack(pady=10)

        except Exception as e:
            print(f"No se pudo cargar la imagen del logo: {e}")
            tk.Label(self, text="[Logo Aquí]", bg="#FFFFFF", fg="#107FFD",
                     font=("Arial", 12, "bold")).pack(pady=10)

        tk.Label(self, text="Login de Usuario", bg="#FFFFFF", fg="#107FFD",
                 font=("Arial", 16, "bold")).pack(pady=10)

   
        self.user_entry = tk.Entry(self)
        self.user_entry.pack()

        tk.Label(self, text="Contraseña:", bg="#FFFFFF").pack()
        self.pass_entry = tk.Entry(self, show="*")
        self.pass_entry.pack()

        tk.Button(
            self, text="Ingresar", bg="#107FFD", fg="white",
            command=self.check_credentials
        ).pack(pady=10)

    def check_credentials(self):
        usuario = self.user_entry.get().strip()
        clave = self.pass_entry.get().strip()
        if usuario == "admin" and clave == "12345":
            messagebox.showinfo("Login Correcto", f"Bienvenido/a {usuario}")
            self.controller.show_frame("MainMenuFrame")
        else:
            messagebox.showerror("Error", "Usuario/Contraseña inválidos.")


class MainMenuFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")
                        # ========== PATRÓN PARA OBTENER RUTA IMAGEN ========== 
        if hasattr(sys, '_MEIPASS'):
            # Cuando está empaquetado con PyInstaller
            base_path = sys._MEIPASS
        else:
            # Cuando corres el .py “normal”
            base_path = os.path.dirname(os.path.abspath(__file__))

        # Unir la carpeta y archivo de imagen
        logo_path = os.path.join(base_path, 'images', 'logo.png')
        
        # Cargar la imagen usando logo_path
        try:
            self.logo = tk.PhotoImage(file=logo_path)
            tk.Label(self, image=self.logo, bg="#FFFFFF").pack(pady=10)
        except Exception as e:
            print(f"No se pudo cargar la imagen del logo: {e}")
            tk.Label(self, text="[Logo Aquí]", bg="#FFFFFF", fg="#107FFD",
                     font=("Arial", 12, "bold")).pack(pady=10)
        
        tk.Label(
            self, text="Menú Principal", bg="#FFFFFF", fg="#107FFD",
            font=("Arial", 20, "bold")
        ).pack(pady=20)

        tk.Button(
            self, text="Ir a Ingresa", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("IngresaFrame"),
            width=20
        ).pack(pady=5)

        #tk.Button(
        #    self, text="Ir a Validaciones Previas", bg="#107FFD", fg="white",
        #    command=lambda: controller.show_frame("ValidacionesFrame"),
        #    width=20
        #).pack(pady=5)

        tk.Button(
            self, text="Ir a Becas", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("BecasFrame"),
            width=20
        ).pack(pady=5)

class IngresaFrame(tk.Frame):
    """
    Pantalla principal de Ingresa, con sub-procesos:
    - FUAS
    - Licitados
    - Renovantes (separado)
    - Solicitud de Monto
    - Egresados
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        # ========== PATRÓN PARA OBTENER RUTA IMAGEN ========== 
        if hasattr(sys, '_MEIPASS'):
            # Cuando está empaquetado con PyInstaller
            base_path = sys._MEIPASS
        else:
            # Cuando corres el .py “normal”
            base_path = os.path.dirname(os.path.abspath(__file__))

        # Unir la carpeta y archivo de imagen
        logo_path = os.path.join(base_path, 'images', 'logo.png')
        
        # Cargar la imagen usando logo_path
        try:
            self.logo = tk.PhotoImage(file=logo_path)
            tk.Label(self, image=self.logo, bg="#FFFFFF").pack(pady=10)

        except Exception as e:
            print(f"No se pudo cargar la imagen del logo: {e}")
            tk.Label(self, text="[Logo Aquí]", bg="#FFFFFF", fg="#107FFD",
                     font=("Arial", 12, "bold")).pack(pady=10)
        
        tk.Label(self, text="Pantalla: Ingresa", bg="#FFFFFF", fg="#107FFD",
                 font=("Arial", 20, "bold")).pack(pady=20)

        tk.Button(
            self, text="FUAS", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("FUASFrame"),
            width=20
        ).pack(pady=5)

        tk.Button(
            self, text="Licitados", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("LicitadosFrame"),
            width=20
        ).pack(pady=5)
        
        tk.Button(
            self, text="Seguimiento Firmas", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("SeguimientosFrame"),
            width=20
        ).pack(pady=5)

        tk.Button(
            self, text="Renovantes (Ingresa)", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("IngresaRenovantesFrame"),
            width=20
        ).pack(pady=5)

        tk.Button(
            self, text="Solicitud de Monto", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame('SolicitudMontoFrame'),
            width=20
        ).pack(pady=5)


        tk.Button(
            self, text="Egresados", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("EgresadosFrame"),
            width=20
        ).pack(pady=5)

        tk.Button(
            self, text="Volver Menú", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("MainMenuFrame"),
            width=20
        ).pack(pady=20)


class FUASFrame(tk.Frame):
    """
    Adaptamos load_file_fuas_1 / 2 / 3 a usar read_any_file()
    """
    def __init__(self, parent, controller, connection=None):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        self.connection = connection1  
        self.df_fuas_hist = None
        self.run_query()

        self.df_fuas_merged_1 = None
        self.df_fuas_merged_2 = None
        self.df_fuas_merged_3 = None

        self.df_fuas_1_dup = None
        self.df_fuas_2_dup = None

        tk.Label(self, text="Sub-proceso: FUAS", font=("Arial", 16, "bold"),
                 bg="#FFFFFF").grid(row=0, column=0, columnspan=4, pady=(10,10))

        self.button_load_1 = tk.Button(
            self, text="Cargar FUAS", bg="#107FFD", fg="white",
            command=self.load_file_fuas_1
        )
        self.button_load_1.grid(row=1, column=0, padx=5, pady=5)
        self.label_file_1 = tk.Label(self, text="Ningún archivo cargado (FUAS)", bg="#FFFFFF")
        self.label_file_1.grid(row=1, column=1, padx=5, pady=5)
        self.button_export_1 = tk.Button(
            self, text="Exportar FUAS", bg="#cccccc", fg="white",
            command=self.export_fuas_1
        )
        self.button_export_1_dup = tk.Button(
            self, text="Exportar Duplicados", bg="#cccccc", fg="white",
            command=self.export_duplicados_1
        )
        self.button_export_1.grid(row=1, column=2, padx=5, pady=5)
        self.button_export_1_dup.grid(row=1,column=3, padx=5, pady=5)

        self.button_load_2 = tk.Button(
            self, text="Cargar FUAS Rezagados", bg="#107FFD", fg="white",
            command=self.load_file_fuas_2
        )
        self.button_load_2.grid(row=2, column=0, padx=5, pady=5)
        self.label_file_2 = tk.Label(self, text="Ningún archivo cargado (FUAS Rezagados)", bg="#FFFFFF")
        self.label_file_2.grid(row=2, column=1, padx=5, pady=5)
        self.button_export_2 = tk.Button(
            self, text="Exportar FUAS Rezagados", bg="#cccccc", fg="white",
            command=self.export_fuas_2
        )
        self.button_export_2.grid(row=2, column=2, padx=5, pady=5)
        self.button_export_2_dup = tk.Button(
            self, text="Exportar Rezagados Duplicados", bg="#cccccc", fg="white",
            command=self.export_duplicados_2
        )
        self.button_export_2_dup.grid(row=2, column=3, padx=5, pady=5)

        self.button_load_3 = tk.Button(
            self, text="Cargar RUT", bg="#107FFD", fg="white",
            command=self.load_file_fuas_3
        )
        self.button_load_3.grid(row=3, column=0, padx=5, pady=5)
        self.label_file_3 = tk.Label(self, text="Ningún archivo cargado (RUT)", bg="#FFFFFF")
        self.label_file_3.grid(row=3, column=1, padx=5, pady=5)
        self.button_export_3 = tk.Button(
            self, text="Exportar RUT", bg="#cccccc", fg="white",
            command=self.export_fuas_3
        )
        self.button_export_3.grid(row=3, column=2, padx=5, pady=5)

        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=4, column=0, columnspan=3, pady=20)
        # 2.1) Logo
            # ========== PATRÓN PARA OBTENER RUTA IMAGEN ========== 
        if hasattr(sys, '_MEIPASS'):
            # Cuando está empaquetado con PyInstaller
            base_path = sys._MEIPASS
        else:
            # Cuando corres el .py “normal”
            base_path = os.path.dirname(os.path.abspath(__file__))

        # Unir la carpeta y archivo de imagen
        logo_path = os.path.join(base_path, 'images', 'logo.png')
        
        # Cargar la imagen usando logo_path
        try:
            self.logo = tk.PhotoImage(file=logo_path)
            #tk.Label(self, image=self.logo, bg="#FFFFFF").pack(pady=10)
            logo_label = tk.Label(self, image=self.logo, bg="#FFFFFF")
            logo_label.grid(row=0, column=0, columnspan=4, pady=(10,0))
        except Exception as e:
            print(f"No se pudo cargar la imagen del logo: {e}")
            tk.Label(self, text="[Logo Aquí]", bg="#FFFFFF", fg="#107FFD",
                     font=("Arial", 12, "bold")).pack(pady=10)
        

    def run_query(self):
        query = text("""
                SELECT*, (cast(APROBADAS as numeric(4,2)) /cast(INSCRITAS as numeric(4,2)))*100 as PORCENTAJE_AVANCE 

                from  (

                                SELECT

                                    [rut] AS RUT

                                    ,DV AS DV

                                    ,[PATERNO] AS [APELLIDO_PATERNO]

                                    ,[MATERNO] AS [APELLIDO_MATERNO]

                                    ,[NOMBRES]

                                    ,1 AS CODIGO_TIPO_IES

                                    ,13 AS CODIGO_IES

                                    ,1 AS CODIGO_SEDE

                                    ,[carrn_cod] AS CODIGO_CARRERA

                                    ,[ano_ingreso] AS ANO_INGRESO

                                    ,JORNADA

                                    ,JORNN_COD AS CODIGO_JORNADA

                                    ,ARANCEL_REFERENCIA AS ARANCEL_REAL

                                    ,(ISNULL([INS_ULTIMO_PERIODO],0)+ISNULL([INS_PENULTIMO_PERIODO],0)) as INSCRITAS

                                    ,(ISNULL([APR_ULTIMO_PERIODO],0)+ISNULL([APR_PENULTIMO_PERIODO],0)) as APROBADAS

                                --   ,((ISNULL([APR_ULTIMO_PERIODO],0)+ISNULL([APR_PENULTIMO_PERIODO],0))) /

                                --  ((ISNULL([INS_ULTIMO_PERIODO],0)+ISNULL([INS_PENULTIMO_PERIODO],0))) * 100 as PORCENTAJE_AVANCE

                                    ,[CODCLI]

                                FROM [DWH_DAI].[dbo].[vw_fuas_historico]

                                WHERE (ISNULL([INS_ULTIMO_PERIODO],0)+ISNULL([INS_PENULTIMO_PERIODO],0)) <> 0

                            )

                            b

                            where (cast(APROBADAS as numeric(5,2)) /cast(INSCRITAS as numeric(5,2)))*100 >= 70
                
        """)
        try:
            if self.connection is not None:
                self.df_fuas_hist = pd.read_sql_query(query, self.connection)
                print("Query ejecutada y df_fuas_hist cargado correctamente.")
            else:
                print("No se ejecutó la query: conexión no proporcionada.")
        except Exception as e:
            print(f"Error al ejecutar query: {e}")

    def merge_and_cleanup(self, df_csv):
        """
        Ajusta la columna RUT a string en ambos DF, 
        elimina columnas duplicadas (salvo RUT) y hace merge on='RUT' (inner).
        """        
        if self.df_fuas_hist is None:
            return None
        if 'rut' in df_csv.columns:
            df_csv.rename(columns={'rut': 'RUT'}, inplace=True)

        df_csv['RUT'] = df_csv['RUT'].astype(str)
        self.df_fuas_hist['RUT'] = self.df_fuas_hist['RUT'].astype(str)

        common_cols = set(df_csv.columns).intersection(self.df_fuas_hist.columns)
        common_cols.discard('RUT')
        if common_cols:
            df_csv.drop(columns=list(common_cols), inplace=True)

        df_merged = pd.merge(df_csv, self.df_fuas_hist, on='RUT', how='inner')
        df_merged.drop_duplicates(inplace=True)
        return df_merged

    # NUEVO: Cargamos con read_any_file
    def load_file_fuas_1(self):
        df_csv, file_path = read_any_file("Seleccionar archivo FUAS #1")
        if df_csv is not None and file_path is not None:
            df_merged = self.merge_and_cleanup(df_csv)
            if df_merged is not None:
                self.df_fuas_merged_1 = df_merged
                self.df_fuas_1_dup = self.df_fuas_merged_1[self.df_fuas_merged_1.duplicated("RUT", keep=False)]
                self.df_fuas_merged_1  =self.df_fuas_merged_1.drop_duplicates('RUT', keep=False)
                self.label_file_1.config(text=f"Archivo cargado: {Path(file_path).name}")
                self.button_export_1.config(bg="green")
                self.button_export_1_dup.config(bg="green")
                messagebox.showinfo(
                    "Cargado",
                    f"Archivo '{Path(file_path).name}' cargado y cruzado correctamente (FUAS #1)."
                )
            else:
                messagebox.showwarning("Cruce no realizado", "Verifica columna RUT en ambos DataFrames.")
        else:
            # Canceló o error
            pass
    


  
    def export_fuas_1(self):
        if self.df_fuas_merged_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (FUAS #1).")
            return
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            title="Guardar FUAS (cruce 1)"
        )
        if file_path:
            try:
                self.df_fuas_merged_1 = self.df_fuas_merged_1[['RUT','DV','APELLIDO_PATERNO', 'APELLIDO_MATERNO'
                                       , 'NOMBRES', 'CODIGO_TIPO_IES', 'CODIGO_IES'
                                       , 'CODIGO_SEDE','CODIGO_CARRERA', 'ANO_INGRESO', 
                                       'CODIGO_JORNADA', 'ARANCEL_REAL', 'PORCENTAJE_AVANCE']]
                
                self.df_fuas_merged_1['PORCENTAJE_AVANCE'] = self.df_fuas_merged_1['PORCENTAJE_AVANCE'].round(0)
                dtype_map = {
                    # identificadores y texto → string (Pandas 1.4+)
                    'RUT'              : 'string',
                    'DV'               : 'string',
                    'APELLIDO_PATERNO' : 'string',
                    'APELLIDO_MATERNO' : 'string',
                    'NOMBRES'          : 'string',
                    'CODIGO_TIPO_IES'  : 'string',
                    'CODIGO_IES'       : 'string',
                    'CODIGO_SEDE'      : 'string',
                    'CODIGO_CARRERA'   : 'string',
                    'CODIGO_JORNADA'   : 'Int64',

                    # numéricos
                    'ANO_INGRESO'      : 'Int64',   # entero “nullable” de pandas
                    'ARANCEL_REAL'     : 'Int64',   # o 'Float64' si quieres nullable
                    'PORCENTAJE_AVANCE': 'Int64'
                }

                # 1) Si el DataFrame ya existe:
                cols = dtype_map.keys() & self.df_fuas_merged_1.columns     # sólo las presentes
                self.df_fuas_merged_1 = self.df_fuas_merged_1.astype(
                    {col: dtype_map[col] for col in cols}
                )
                 # ──>>  padding con ceros según largo fijo  ──────────────────────────────
                pad = {'CODIGO_IES': 3, 'CODIGO_SEDE': 3, 'CODIGO_CARRERA': 4}
                for col, width in pad.items():
                    if col in self.df_fuas_merged_1.columns:
                        self.df_fuas_merged_1[col] = self.df_fuas_merged_1[col].str.zfill(width)
        # ────────────────────────────────────────────────────────────────────────
                self.df_fuas_merged_1.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Archivo Excel guardado: {file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar.\n{e}")
    def export_duplicados_1(self):
        if self.df_fuas_1_dup is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (Duplicados).")
            return
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            title="Guardar FUAS  (Duplicados)"
        )
        if file_path:
            try:
                self.df_fuas_1_dup = self.df_fuas_1_dup[['RUT','DV','APELLIDO_PATERNO', 'APELLIDO_MATERNO'
                                       , 'NOMBRES', 'CODIGO_TIPO_IES', 'CODIGO_IES'
                                       , 'CODIGO_SEDE','CODIGO_CARRERA', 'ANO_INGRESO', 
                                       'CODIGO_JORNADA', 'ARANCEL_REAL', 'PORCENTAJE_AVANCE']]
                self.df_fuas_1_dup['PORCENTAJE_AVANCE'] = self.df_fuas_1_dup['PORCENTAJE_AVANCE'].round(0)
                dtype_map = {
                    # identificadores y texto → string (Pandas 1.4+)
                    'RUT'              : 'string',
                    'DV'               : 'string',
                    'APELLIDO_PATERNO' : 'string',
                    'APELLIDO_MATERNO' : 'string',
                    'NOMBRES'          : 'string',
                    'CODIGO_TIPO_IES'  : 'string',
                    'CODIGO_IES'       : 'string',
                    'CODIGO_SEDE'      : 'string',
                    'CODIGO_CARRERA'   : 'string',
                    'CODIGO_JORNADA'   : 'Int64',

                    # numéricos
                    'ANO_INGRESO'      : 'Int64',   # entero “nullable” de pandas
                    'ARANCEL_REAL'     : 'float',   # o 'Float64' si quieres nullable
                    'PORCENTAJE_AVANCE': 'float'
                }

                # 1) Si el DataFrame ya existe:
                cols = dtype_map.keys() & self.df_fuas_1_dup.columns     # sólo las presentes
                self.df_fuas_1_dup = self.df_fuas_1_dup.astype(
                    {col: dtype_map[col] for col in cols}
                )
                pad = {'CODIGO_IES': 3, 'CODIGO_SEDE': 3, 'CODIGO_CARRERA': 4}
                for col, width in pad.items():
                    if col in self.df_fuas_1_dup.columns:
                        self.df_fuas_1_dup[col] = self.df_fuas_1_dup[col].str.zfill(width)
                
                
                self.df_fuas_1_dup.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Archivo Excel guardado: {file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar.\n{e}")
    def load_file_fuas_2(self):
        df_csv, file_path = read_any_file("Seleccionar archivo FUAS Rezagados #2")
        if df_csv is not None and file_path is not None:
            df_merged = self.merge_and_cleanup(df_csv)
            if df_merged is not None:
                self.df_fuas_merged_2 = df_merged
                self.df_fuas_2_dup = self.df_fuas_merged_2[self.df_fuas_merged_2.duplicated("RUT", keep=False)]
                self.df_fuas_merged_2  =self.df_fuas_merged_2.drop_duplicates('RUT', keep=False)
                self.label_file_2.config(text=f"Archivo cargado: {Path(file_path).name}")
                self.button_export_2.config(bg="green")
                self.button_export_2_dup.config(bg="green")
                messagebox.showinfo(
                    "Cargado",
                    f"Archivo '{Path(file_path).name}' cargado y cruzado (Rezagados)."
                )
            else:
                messagebox.showwarning("Cruce no realizado", "Verifica RUT en DataFrames.")
        else:
            pass

    def export_fuas_2(self):
        if self.df_fuas_merged_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (FUAS Rezagados).")
            return
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            title="Guardar FUAS Rezagados (cruce 2)"
        )
        if file_path:
            try:
                self.df_fuas_merged_2 = self.df_fuas_merged_2[['RUT','DV','APELLIDO_PATERNO', 'APELLIDO_MATERNO'
                                       , 'NOMBRES', 'CODIGO_TIPO_IES', 'CODIGO_IES'
                                       , 'CODIGO_SEDE','CODIGO_CARRERA', 'ANO_INGRESO', 
                                       'CODIGO_JORNADA', 'ARANCEL_REAL', 'PORCENTAJE_AVANCE']]
                self.df_fuas_merged_2['PORCENTAJE_AVANCE'] = self.df_fuas_merged_2['PORCENTAJE_AVANCE'].round(0)
                dtype_map = {
                    # identificadores y texto → string (Pandas 1.4+)
                    'RUT'              : 'string',
                    'DV'               : 'string',
                    'APELLIDO_PATERNO' : 'string',
                    'APELLIDO_MATERNO' : 'string',
                    'NOMBRES'          : 'string',
                    'CODIGO_TIPO_IES'  : 'string',
                    'CODIGO_IES'       : 'string',
                    'CODIGO_SEDE'      : 'string',
                    'CODIGO_CARRERA'   : 'string',
                    'CODIGO_JORNADA'   : 'Int64',

                    # numéricos
                    'ANO_INGRESO'      : 'Int64',   # entero “nullable” de pandas
                    'ARANCEL_REAL'     : 'float',   # o 'Float64' si quieres nullable
                    'PORCENTAJE_AVANCE': 'float'
                }

                # 1) Si el DataFrame ya existe:
                cols = dtype_map.keys() & self.df_fuas_merged_2.columns     # sólo las presentes
                self.df_fuas_merged_2 = self.df_fuas_merged_2.astype(
                    {col: dtype_map[col] for col in cols}
                )
                
                # ──>>  padding con ceros según largo fijo  ──────────────────────────────
                pad = {'CODIGO_IES': 3, 'CODIGO_SEDE': 3, 'CODIGO_CARRERA': 4}
                for col, width in pad.items():
                    if col in self.df_fuas_merged_2.columns:
                        self.df_fuas_merged_2[col] = self.df_fuas_merged_2[col].str.zfill(width)
                # ────────────────────────────────────────────────────────────────────────

                
                
                self.df_fuas_merged_2.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Archivo Excel guardado: {file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar.\n{e}")

    def export_duplicados_2(self):
        if self.df_fuas_2_dup is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (Duplicados).")
            return
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            title="Guardar FUAS Rezagados (Duplicados)"
        )
        if file_path:
            try:
                self.df_fuas_2_dup = self.df_fuas_2_dup[['RUT','DV','APELLIDO_PATERNO', 'APELLIDO_MATERNO'
                                       , 'NOMBRES', 'CODIGO_TIPO_IES', 'CODIGO_IES'
                                       , 'CODIGO_SEDE','CODIGO_CARRERA', 'ANO_INGRESO', 
                                       'CODIGO_JORNADA', 'ARANCEL_REAL', 'PORCENTAJE_AVANCE']]
                self.df_fuas_2_dup['PORCENTAJE_AVANCE'] = self.df_fuas_2_dup['PORCENTAJE_AVANCE'].round(0)
                dtype_map = {
                    # identificadores y texto → string (Pandas 1.4+)
                    'RUT'              : 'string',
                    'DV'               : 'string',
                    'APELLIDO_PATERNO' : 'string',
                    'APELLIDO_MATERNO' : 'string',
                    'NOMBRES'          : 'string',
                    'CODIGO_TIPO_IES'  : 'string',
                    'CODIGO_IES'       : 'string',
                    'CODIGO_SEDE'      : 'string',
                    'CODIGO_CARRERA'   : 'string',
                    'CODIGO_JORNADA'   : 'Int64',

                    # numéricos
                    'ANO_INGRESO'      : 'Int64',   # entero “nullable” de pandas
                    'ARANCEL_REAL'     : 'float',   # o 'Float64' si quieres nullable
                    'PORCENTAJE_AVANCE': 'float'
                }

                # 1) Si el DataFrame ya existe:
                cols = dtype_map.keys() & self.df_fuas_2_dup.columns     # sólo las presentes
                self.df_fuas_2_dup = self.df_fuas_2_dup.astype(
                    {col: dtype_map[col] for col in cols}
                )
                pad = {'CODIGO_IES': 3, 'CODIGO_SEDE': 3, 'CODIGO_CARRERA': 4}
                for col, width in pad.items():
                    if col in self.df_fuas_2_dup.columns:
                        self.df_fuas_2_dup[col] = self.df_fuas_2_dup[col].str.zfill(width)
                
                
                self.df_fuas_2_dup.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Archivo Excel guardado: {file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar.\n{e}")

    def load_file_fuas_3(self):
        df_csv, file_path = read_any_file("Seleccionar archivo FUAS proceso anterior #3")
        if df_csv is not None and file_path is not None:
            df_merged = self.merge_and_cleanup(df_csv)
            if df_merged is not None:
                self.df_fuas_merged_3 = df_merged
                self.label_file_3.config(text=f"Archivo cargado: {Path(file_path).name}")
                self.button_export_3.config(bg="green")
                messagebox.showinfo(
                    "Cargado",
                    f"Archivo '{Path(file_path).name}' cruzado (proceso anterior)."
                )
            else:
                messagebox.showwarning("Cruce no realizado", "Verifica RUT.")
        else:
            pass

    def export_fuas_3(self):
        if self.df_fuas_merged_3 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#3).")
            return
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            title="Guardar FUAS proceso anterior (cruce 3)"
        )
        if file_path:
            try:
                self.df_fuas_merged_3 = self.df_fuas_merged_3[['RUT','DV','APELLIDO_PATERNO', 'APELLIDO_MATERNO'
                                       , 'NOMBRES', 'CODIGO_TIPO_IES', 'CODIGO_IES'
                                       , 'CODIGO_SEDE','CODIGO_CARRERA', 'ANO_INGRESO', 
                                       'CODIGO_JORNADA', 'ARANCEL_REAL', 'PORCENTAJE_AVANCE']]
                self.df_fuas_merged_3['PORCENTAJE_AVANCE'] = self.df_fuas_merged_3['PORCENTAJE_AVANCE'].round(0)
                dtype_map = {
                    # identificadores y texto → string (Pandas 1.4+)
                    'RUT'              : 'string',
                    'DV'               : 'string',
                    'APELLIDO_PATERNO' : 'string',
                    'APELLIDO_MATERNO' : 'string',
                    'NOMBRES'          : 'string',
                    'CODIGO_TIPO_IES'  : 'string',
                    'CODIGO_IES'       : 'string',
                    'CODIGO_SEDE'      : 'string',
                    'CODIGO_CARRERA'   : 'string',
                    'CODIGO_JORNADA'   : 'Int64',

                    # numéricos
                    'ANO_INGRESO'      : 'Int64',   # entero “nullable” de pandas
                    'ARANCEL_REAL'     : 'float',   # o 'Float64' si quieres nullable
                    'PORCENTAJE_AVANCE': 'float'
                }

                # 1) Si el DataFrame ya existe:
                cols = dtype_map.keys() & self.df_fuas_merged_3.columns     # sólo las presentes
                self.df_fuas_merged_3 = self.df_fuas_merged_3.astype(
                    {col: dtype_map[col] for col in cols}
                )
                
                        # ──>>  padding con ceros según largo fijo  ──────────────────────────────
                pad = {'CODIGO_IES': 3, 'CODIGO_SEDE': 3, 'CODIGO_CARRERA': 4}
                for col, width in pad.items():
                    if col in self.df_fuas_merged_3.columns:
                        self.df_fuas_merged_3[col] = self.df_fuas_merged_3[col].str.zfill(width)
        # 
                
                
                self.df_fuas_merged_3.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Archivo Excel guardado: {file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar.\n{e}")

# ============================================================== 
#       CLASE SOLICITUD DE MONTO (ADAPTADA) 
# ============================================================== 


class SolicitudMontoFrame(tk.Frame):
    """
    Flujo:
      1) Cargar Excel de refinanciamiento (df_extra).
      2) Cargar archivos 5A y 5B.
      3) "Cruce Matrícula" -> (5A ∩ 5B) se cruza con df_licitados => self.df_result
      4) Se habilita "Cruce Refinanciamiento" (para 5A+5B) -> self.df_result se cruza con df_extra.
      5) Cargar RUT (5C) -> cruces análogos en otra fila.
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        global df_licitados

        # DataFrames sesión
        self.df_extra = None
        self.df_csv_1 = None        # 5A
        self.df_csv_2 = None        # 5B
        self.df_csv_rut = None      # 5C
        self.df_result = None
        self.df_nc_mat = None
        self.df_nc_ref = None
        self.df_result_rut = None
        self.df_nc_mat_rut = None
        self.df_nc_ref_rut = None
        self.df_dup = df_licitados[df_licitados.duplicated("RUT", keep=False)].copy()

        # ---------------- UI ----------------
        for r in range(14): self.rowconfigure(r, weight=1)
        for c in range(6): self.columnconfigure(c, weight=1)

        tk.Label(self, text="SOLICITUD DE MONTO", font=("Arial",16,"bold"), bg="#FFFFFF")\
            .grid(row=0,column=0,columnspan=6,pady=10)
        tk.Button(self,text="Exportar duplicados",bg="#FF8C00",fg="white",command=self.export_dup)\
            .grid(row=0,column=5,sticky="e",padx=5)

        # Cargar Refinanciamiento
        self.btn_ref = tk.Button(self,text="Cargar Refinanciamiento",bg="#008000",fg="white",command=self.load_ref)
        self.btn_ref.grid(row=1,column=0,padx=5)
        self.lbl_ref = tk.Label(self,text="Sin archivo ref",bg="#FFFFFF")
        self.lbl_ref.grid(row=1,column=1,columnspan=5,sticky="w")

        # 5A y 5B
        self.btn1a = tk.Button(self,text="Cargar 5A",bg="#107FFD",fg="white",command=self.load_1a)
        self.btn1a.grid(row=2,column=0,padx=5)
        self.lbl1a = tk.Label(self,text="Sin 5A",bg="#FFFFFF")
        self.lbl1a.grid(row=2,column=1,sticky="w")
        self.btn1b = tk.Button(self,text="Cargar Reporte Sol Monto",bg="#107FFD",fg="white",command=self.load_1b)
        self.btn1b.grid(row=3,column=0,padx=5)
        self.lbl1b = tk.Label(self,text="Sin Reporte Sol Monto",bg="#FFFFFF")
        self.lbl1b.grid(row=3,column=1,sticky="w")

        # Botones cruce 5A+5B
        self.btn_mat = tk.Button(self,text="Cruce Matrícula",bg="#cccccc",fg="white",command=self.run_mat)
        self.btn_mat.grid(row=4,column=0,pady=5)
        self.btn_view_mat = tk.Button(self,text="Ver",bg="#cccccc",state="disabled",
                                      command=lambda: self._show_df(self.df_result,"Cruce Matrícula 5A+Reporte Sol Monto"))
        self.btn_view_mat.grid(row=4,column=1)
        self.btn_nc_mat = tk.Button(self,text="SIN cruce Matrícula",bg="#cccccc",fg="white",
                                    state="disabled",command=lambda: self.save_nc(self.df_nc_mat,"NoCruce_Mat"))
        self.btn_nc_mat.grid(row=4,column=2)
        self.btn_ref_run = tk.Button(self,text="Cruce Refinanciamiento",bg="#cccccc",fg="white",
                                     state="disabled",command=self.run_ref)
        self.btn_ref_run.grid(row=4,column=3)
        self.btn_view_ref = tk.Button(self,text="Ver",bg="#cccccc",state="disabled",
                                      command=lambda: self._show_df(self.df_ref_result,"Cruce Refinanciamiento 5A+Reporte Sol Monto"))
        self.btn_view_ref.grid(row=4,column=4)
        self.btn_nc_ref = tk.Button(self,text="SIN cruce Ref.",bg="#cccccc",fg="white",
                                    state="disabled",command=lambda: self.save_nc(self.df_nc_ref,"NoCruce_Ref"))
        self.btn_nc_ref.grid(row=4,column=5)

        self.lbl_status_mat = tk.Label(self,text="",bg="#FFFFFF",anchor="w")
        self.lbl_status_mat.grid(row=5,column=0,columnspan=6,sticky="we")

        # 5C (RUT)
        self.btn_rut = tk.Button(self,text="Cargar RUT (5C)",bg="#107FFD",fg="white",command=self.load_rut)
        self.btn_rut.grid(row=6,column=0,padx=5)
        self.lbl_rut = tk.Label(self,text="Sin 5C",bg="#FFFFFF")
        self.lbl_rut.grid(row=6,column=1,sticky="w")

        self.btn_mat_rut = tk.Button(self,text="Cruce Matrícula (RUT)",bg="#cccccc",fg="white",command=self.run_mat_rut)
        self.btn_mat_rut.grid(row=7,column=0)
        self.btn_view_mat_rut = tk.Button(self,text="Ver",state="disabled",bg="#cccccc",
                                          command=lambda:self._show_df(self.df_result_rut,"Cruce Matrícula RUT"))
        self.btn_view_mat_rut.grid(row=7,column=1)
        self.btn_nc_mat_rut = tk.Button(self,text="SIN cruce Matrícula (RUT)",bg="#cccccc",fg="white",
                                        state="disabled",command=lambda:self.save_nc(self.df_nc_mat_rut,"NoCruce_Mat_RUT"))
        self.btn_nc_mat_rut.grid(row=7,column=2)
        self.btn_ref_rut = tk.Button(self,text="Cruce Ref. (RUT)",bg="#cccccc",fg="white",
                                     state="disabled",command=self.run_ref_rut)
        self.btn_ref_rut.grid(row=7,column=3)
        self.btn_view_ref_rut = tk.Button(self,text="Ver",state="disabled",bg="#cccccc",
                                          command=lambda:self._show_df(self.df_ref_result_rut,"Cruce Refinanciamiento RUT"))
        self.btn_view_ref_rut.grid(row=7,column=4)
        self.btn_nc_ref_rut = tk.Button(self,text="SIN cruce Ref. (RUT)",bg="#cccccc",fg="white",
                                        state="disabled",command=lambda:self.save_nc(self.df_nc_ref_rut,"NoCruce_Ref_RUT"))
        self.btn_nc_ref_rut.grid(row=7,column=5)

        self.lbl_status_rut = tk.Label(self,text="",bg="#FFFFFF",anchor="w")
        self.lbl_status_rut.grid(row=8,column=0,columnspan=6,sticky="we")

        tk.Button(self,text="Volver",bg="#aaaaaa",fg="white",
                  command=lambda: controller.show_frame("IngresaFrame")).grid(row=13,column=0,columnspan=6,pady=15)

        # Para almacenar temporalmente resultados de refinanciamiento (vista previa)
        self.df_ref_result = None
        self.df_ref_result_rut = None

    # ---------------- Export duplicados ----------------
    def export_dup(self):
        if self.df_dup.empty:
            messagebox.showinfo("Duplicados","No hay RUT duplicados."); return
        self._save_df(self.df_dup,"Duplicados_RUT")

    # ---------------- Loaders ----------------
    def load_ref(self):
        df,p=read_any_file("Excel Refinanciamiento")
        if df is None: return
        col="RUTALU" if "RUTALU" in df.columns else "RUT"
        if col not in df.columns:
            messagebox.showerror("Error","Sin RUT en refinanciamiento"); return
        df["RUT"]=df[col].astype(str).str.strip()
        self.df_extra=df
        self.lbl_ref.config(text=os.path.basename(p))

    def load_1a(self):
        df,p=read_any_file("Archivo 5A")
        if df is None or "RUT" not in df.columns:
            messagebox.showerror("Error","5A sin RUT"); return
        df["RUT"]=df["RUT"].astype(str).str.strip()
        self.df_csv_1=df
        self.lbl1a.config(text=os.path.basename(p))

    def load_1b(self):
        df,p=read_any_file("Archivo 5B")
        if df is None or "RUT" not in df.columns:
            messagebox.showerror("Error","5B sin RUT"); return
        df["RUT"]=df["RUT"].astype(str).str.strip()
        self.df_csv_2=df
        self.lbl1b.config(text=os.path.basename(p))

    def load_rut(self):
        df,p=read_any_file("Archivo 5C")
        if df is None or "RUT" not in df.columns:
            messagebox.showerror("Error","5C sin RUT"); return
        df["RUT"]=df["RUT"].astype(str).str.strip()
        self.df_csv_rut=df
        self.lbl_rut.config(text=os.path.basename(p))

    # ---------------- Cruces ----------------
    def run_mat(self):
        if self.df_csv_1 is None or self.df_csv_2 is None:
            messagebox.showwarning("Faltan archivos","Carga 5A y 5B primero."); return
        global df_licitados
        left = self.df_csv_1[["RUT"]].drop_duplicates()
        right = self.df_csv_2[["RUT"]].drop_duplicates()
        df_concat = pd.merge(left,right,on="RUT",how="inner")  # intersección 5A ∩ 5B

        self.df_result,self.df_nc_mat = merge_and_clean(df_licitados,df_concat)
        self.df_result = self._drop_firma(self.df_result)

        if self.df_result.empty:
            messagebox.showinfo("Cruce Matrícula","No hubo coincidencias.")
        else:
            # Vista previa y export automático opcional
            self._save_df(self.df_result,"Cruce_Mat_5A5B")

        # Actualiza UI
        self.btn_mat.config(bg="#107FFD")
        self.btn_nc_mat.config(state="normal",bg="#107FFD")
        self.btn_ref_run.config(state="normal",bg="#107FFD")
        self.btn_view_mat.config(state="normal",bg="#107FFD")
        self.lbl_status_mat.config(
            text=f"Cruce Matrícula 5A+5B: {len(self.df_result)} coincidencias / {len(self.df_nc_mat)} sin cruce."
        )

    def run_ref(self):
        if self.df_result is None or self.df_extra is None:
            messagebox.showwarning("Faltan datos","Primero realiza el cruce de Matrícula y carga Refinanciamiento."); return
        current = self.df_extra[["RUT","SALDO"]].copy()
        current["RUT"]=current["RUT"].astype(str)
        current = current.rename(columns={'SALDO': 'MONTO REFINANCIAMIENTO'})
        current['REFINANCIAMIENTO'] = '1'
        self.df_ref_result = pd.merge(self.df_result,current,on="RUT",how="inner")
        self.df_nc_ref = self.df_result[~self.df_result["RUT"].isin(self.df_ref_result["RUT"])]

        if self.df_ref_result.empty:
            messagebox.showinfo("Cruce Refinanciamiento","Sin coincidencias.")
        else:
            self._save_df(self.df_ref_result,"Cruce_Ref_5A5B")

        self.btn_nc_ref.config(state="normal",bg="#107FFD")
        self.btn_view_ref.config(state="normal",bg="#107FFD")
        self.lbl_status_mat.config(
            text=self.lbl_status_mat.cget("text") + 
                 f" | Refinanciamiento: {len(self.df_ref_result)} coincidencias / {len(self.df_nc_ref)} sin cruce."
        )

    def run_mat_rut(self):
        if self.df_csv_rut is None:
            messagebox.showwarning("Falta archivo","Carga 5C primero."); return
        global df_licitados
        self.df_result_rut,self.df_nc_mat_rut = merge_and_clean(df_licitados,self.df_csv_rut)
        self.df_result_rut = self._drop_firma(self.df_result_rut)

        if self.df_result_rut.empty:
            messagebox.showinfo("Cruce Matrícula (RUT)","No hubo coincidencias.")
        else:
            self._save_df(self.df_result_rut,"Cruce_Mat_RUT")

        self.btn_mat_rut.config(bg="#107FFD")
        self.btn_nc_mat_rut.config(state="normal",bg="#107FFD")
        self.btn_ref_rut.config(state="normal",bg="#107FFD")
        self.btn_view_mat_rut.config(state="normal",bg="#107FFD")
        self.lbl_status_rut.config(
            text=f"Cruce Matrícula (RUT): {len(self.df_result_rut)} coincidencias / {len(self.df_nc_mat_rut)} sin cruce."
        )

    def run_ref_rut(self):
        if self.df_result_rut is None or self.df_extra is None:
            messagebox.showwarning("Faltan datos","Primero realiza el cruce Matrícula (RUT) y carga Refinanciamiento."); return
        current = self.df_extra[["RUT","SALDO"]].copy()
        current["RUT"]=current["RUT"].astype(str)
        current = current.rename(columns={'SALDO': 'MONTO REFINANCIAMIENTO'})
        current['REFINANCIAMIENTO'] = '1'
        self.df_ref_result_rut = pd.merge(self.df_result_rut,current,on="RUT",how="inner")
        self.df_nc_ref_rut = self.df_result_rut[~self.df_result_rut["RUT"].isin(self.df_ref_result_rut["RUT"])]

        if self.df_ref_result_rut.empty:
            messagebox.showinfo("Cruce Refinanciamiento (RUT)","Sin coincidencias.")
        else:
            self._save_df(self.df_ref_result_rut,"Cruce_Ref_RUT")

        self.btn_nc_ref_rut.config(state="normal",bg="#107FFD")
        self.btn_view_ref_rut.config(state="normal",bg="#107FFD")
        self.lbl_status_rut.config(
            text=self.lbl_status_rut.cget("text") + 
                 f" | Refinanciamiento: {len(self.df_ref_result_rut)} coincidencias / {len(self.df_nc_ref_rut)} sin cruce."
        )

    # ---------------- Utilidades ----------------
    def _drop_firma(self, df):
        if df is not None and not df.empty and 'ETAPA FIRMA' in df.columns:
            return df.drop(columns=['ETAPA FIRMA'])
        return df

    def _show_df(self, df, title):
        if df is None or df.empty:
            messagebox.showinfo("Sin datos","No hay datos para mostrar."); return
        win = tk.Toplevel(self)
        win.title(title)
        frame = ttk.Frame(win); frame.pack(fill="both", expand=True)
        cols = list(df.columns)
        tree = ttk.Treeview(frame, columns=cols, show="headings")
        for c in cols:
            tree.heading(c, text=c)
            tree.column(c, width=120, anchor="w")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        for _,row in df.iterrows():
            tree.insert("", "end", values=[row[c] for c in cols])

    def save_nc(self, df, name):
        if df is None or df.empty:
            messagebox.showinfo("Sin datos", "No hay registros sin cruce para exportar."); return
        self._save_df(df,name)

    def _save_df(self, df, default_name):
        path=filedialog.asksaveasfilename(
            title="Guardar Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel","*.xlsx")],
            initialfile=f"{default_name}.xlsx"
        )
        if not path: return
        try:
            df.to_excel(path, index=False)
            messagebox.showinfo("Exportado", f"Guardado en:\n{path}")
        except Exception as e:
            messagebox.showerror("Error", str(e))





class LicitadosFrame(tk.Frame):
    """
    Sub-proceso Licitados, con soporte para:
      - Cargar un "archivo extra" (df_extra) = Refinanciamiento
      - Sub-procesos #1 (Seleccionados), #2 (Preseleccionados), #3 (No seleccionados).
      - Un sub-proceso #3b adicional.
      - Sub-proceso "RUT".
      
      Se han añadido 2 botones extra de exportación para cada sub-proceso.
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")
        self.anio_ingresado = tk.StringVar(value=_dt.datetime.now().year)  # valor por defecto
        self.current_year   = int(self.anio_ingresado.get())
        # Filtro opcional por CODIGO_IES
        self.codigo_ies_var = tk.StringVar(value="")
        # Se asume que df_licitados está declarado globalmente
        global df_licitados
        ## NUEVO
        self.df_licitados_query = df_licitados.drop_duplicates(subset=["RUT"], keep="first").copy()
        #self.df_licitados_query = df_licitados
        ## NUEVO
        self.df_duplicados = df_licitados[df_licitados.duplicated(subset=["RUT"], keep=False)].copy()

        # DataFrames de los sub-procesos
        self.df_resultado_1 = None
        self.df_resultado_cruce_1 = None
        self.df_resultado_no_cruce_1 = None
        self.df_resultado_2 = None
        self.df_resultado_cruce_2 = None
        self.df_resultado_no_cruce_2 = None
        self.df_resultado_3 = None
        self.df_resultado_cruce_3 = None
        self.df_resultado_no_cruce_3 = None
        self.df_resultado_3_extra = None  # Para el 3b

        # DataFrame del "archivo extra" (Refinanciamiento)
        self.df_extra = None

        # NUEVO: DataFrame para el sub-proceso "RUT"
        self.df_csv_rut = None
        self.df_resultado_rut = None
        self.df_resultado_cruce_rut = None
        self.df_resultado_no_cruce_rut = None
        self.df_csv_1 = None
        self.df_csv_2 = None
        self.df_csv_3 = None

        #
        # Layout base
        #
        # Aumentamos a 12 filas y 8 columnas para acomodar los botones extra
        for row_idx in range(12):
            self.rowconfigure(row_idx, weight=1)
        # aumentamos a 8 columnas por los nuevos controles
        for col_idx in range(8):
            self.columnconfigure(col_idx, weight=1)

        tk.Label(
            self, text="LICITADOS", font=("Arial", 16, "bold"),
            bg="#FFFFFF"
        ).grid(row=0, column=0, columnspan=1, padx=5)

        # ---- Fila 0  ----------------------------------------------------
        tk.Label(self, text="Año de ingreso:", bg="#FFFFFF")\
            .grid(row=0, column=1, sticky="e")

        tk.Entry(self, textvariable=self.anio_ingresado, width=6)\
            .grid(row=0, column=2, sticky="w")

        tk.Label(self, text="Código IES:", bg="#FFFFFF")\
            .grid(row=0, column=4, sticky="e")
        tk.Entry(self, textvariable=self.codigo_ies_var, width=6)\
            .grid(row=0, column=5, sticky="w")

        tk.Button(
            self, text="Filtrar",
            command=self.apply_filter,
            bg="#107FFD", fg="white", width=8
        ).grid(row=0, column=6, padx=5, sticky="w")

        # NUEVO: Botón “Guardar” a la derecha del Entry
        tk.Button(
            self, text="Guardar",
            command=self._set_anio_ingreso,           # función que definimos más abajo
            bg="#107FFD", fg="white", width=8
        ).grid(row=0, column=3, padx=5, sticky="w")

                # Botón – Exportar duplicados (esquina superior‑derecha)
        self.btn_exportar_duplicados = tk.Button(
            self,
            text="Exportar duplicados",
            bg="#FF8C00",
            fg="white",
            command=self.exportar_duplicados
        )
        self.btn_exportar_duplicados.grid(row=0, column=7, padx=5, pady=5, sticky="e")

        #
        # 1) Botón para cargar el archivo EXTRA (refinanciamiento)
        #
        self.btn_cargar_extra = tk.Button(
            self, text="Cargar Refinanciamiento", bg="#008000", fg="white",
            command=self.load_file_extra
        )
        self.btn_cargar_extra.grid(row=1, column=0, padx=5, pady=5)

        self.label_file_extra = tk.Label(
            self, text="Sin archivo refinanciamiento", bg="#FFFFFF"
        )
        self.label_file_extra.grid(row=1, column=1, columnspan=4, padx=5, pady=5, sticky="w")

        #
        # Sub-proceso #1
        #
        self.btn_cargar_1 = tk.Button(
            self, text="Cargar Seleccionados #1", bg="#107FFD", fg="white",
            command=self.load_file_licitados_1
        )
        self.btn_cargar_1.grid(row=4, column=0, padx=5, pady=5)

        self.label_file_1 = tk.Label(self, text="Sin archivo (#1)", bg="#FFFFFF")
        self.label_file_1.grid(row=4, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_1 = tk.Button(
            self, text="Exportar cruce con Matrícula", bg="#cccccc", fg="white",
            command=self.export_licitados_1
        )
        self.btn_export_1.grid(row=4, column=2, padx=5, pady=5)

        # Botones extra de exportación (ejemplo)
        self.btn_export_1_b = tk.Button(
            self, text="Exportar cumple", bg="#cccccc", fg="white",
            command=self.export_licitados_1_b
        )
        self.btn_export_1_b.grid(row=4, column=3, padx=5, pady=5)

        self.btn_export_1_c = tk.Button(
            self, text="Exportación no cumple", bg="#cccccc", fg="white",
            command=self.export_licitados_1_c
        )
        self.btn_export_1_c.grid(row=4, column=4, padx=5, pady=5)

        self.btn_run_1 = tk.Button(
            self, text="Run #1", bg="#cccccc", fg="white", state="disabled",
            command=self.run_licitados_1
        )
        self.btn_run_1.grid(row=4, column=5, padx=5, pady=5)

        self.btn_preview_1 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_1, "Preview #1")
        )
        self.btn_preview_1.grid(row=4, column=6, padx=5, pady=5)

        self.btn_usa_extra_1 = tk.Button(
            self, text="Cruzar con Refinanciamiento", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_1
        )
        ####
        self.btn_usa_extra_1.grid(row=5, column=0, columnspan=8, pady=5)

        #
        # Sub-proceso #2
        #
        self.btn_cargar_2 = tk.Button(
            self, text="Cargar Preseleccionados #2", bg="#107FFD", fg="white",
            command=self.load_file_licitados_2
        )
        self.btn_cargar_2.grid(row=2, column=0, padx=5, pady=5)

        self.label_file_2 = tk.Label(self, text="Sin archivo (#2)", bg="#FFFFFF")
        self.label_file_2.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_2 = tk.Button(
            self, text="Exportar cruce con Matrícula", bg="#cccccc", fg="white",
            command=self.export_licitados_2
        )
        self.btn_export_2.grid(row=2, column=2, padx=5, pady=5)

        # Botones extra de exportación (ejemplo)
        self.btn_export_2_b = tk.Button(
            self, text="Exportación Cumple", bg="#cccccc", fg="white",
            command=self.export_licitados_2_b
        )
        self.btn_export_2_b.grid(row=2, column=3, padx=5, pady=5)

        self.btn_export_2_c = tk.Button(
            self, text="Exportación No cumple", bg="#cccccc", fg="white",
            command=self.export_licitados_2_c
        )
        self.btn_export_2_c.grid(row=2, column=4, padx=5, pady=5)

        self.btn_run_2 = tk.Button(
            self, text="Run #2", bg="#cccccc", fg="white", state="disabled",
            command=self.run_licitados_2
        )
        self.btn_run_2.grid(row=2, column=5, padx=5, pady=5)

        self.btn_preview_2 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_2, "Preview #2")
        )
        self.btn_preview_2.grid(row=2, column=6, padx=5, pady=5)

        self.btn_usa_extra_2 = tk.Button(
            self, text="Cruzar con refinanciamiento", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_2
        )
        self.btn_usa_extra_2.grid(row=3, column=0, columnspan=8, pady=5)

        #
        # Sub-proceso #3
        #
        self.btn_cargar_3 = tk.Button(
            self, text="Cargar No seleccionados #3", bg="#107FFD", fg="white",
            command=self.load_file_licitados_3
        )
        self.btn_cargar_3.grid(row=6, column=0, padx=5, pady=5)
        self.label_file_3 = tk.Label(self, text="Sin archivo (#3)", bg="#FFFFFF")
        self.label_file_3.grid(row=6, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_3 = tk.Button(
            self, text="Exportar cruce con Matrícula", bg="#cccccc", fg="white",
            command=self.export_licitados_3
        )
        self.btn_export_3.grid(row=6, column=2, padx=5, pady=5)

        self.btn_run_3 = tk.Button(
            self, text="Run #3", bg="#cccccc", fg="white", state="disabled",
            command=self.run_licitados_3
        )
        self.btn_run_3.grid(row=6, column=5, padx=5, pady=5)

        self.btn_preview_3 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_3, "Preview #3")
        )
        self.btn_preview_3.grid(row=6, column=6, padx=5, pady=5)

        self.btn_usa_extra_3 = tk.Button(
            self, text="Cruzar con refinanciamiento", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_3
        )
        self.btn_usa_extra_3.grid(row=7, column=0, columnspan=8, pady=5)

        #
        # Sub-proceso #3b (opcional)
        #
        self.btn_cargar_3b = tk.Button(
            self, text="Cargar Archivo Morosos (3b)", 
            bg="#cccccc", fg="white",
            state="disabled",
            command=self.load_file_licitados_3b
        )
        self.btn_cargar_3b.grid(row=8, column=0, columnspan=3, pady=5)


        #
        # NUEVO: Sub-proceso "RUT"
        #
        self.btn_cargar_rut = tk.Button(
            self, text="Cargar RUT (Adicional)", bg="#107FFD", fg="white",
            command=self.load_file_rut
        )
        self.btn_cargar_rut.grid(row=9, column=0, padx=5, pady=5)

        self.label_file_rut = tk.Label(self, text="Sin archivo (RUT)", bg="#FFFFFF")
        self.label_file_rut.grid(row=9, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_rut_matricula = tk.Button(
            self, text="Exportar cruce con Matrícula (RUT)", bg="#cccccc", fg="white",
            command=self.export_rut_matricula
        )
        self.btn_export_rut_matricula.grid(row=9, column=2, padx=5, pady=5)

        # Botones extra RUT
        self.btn_export_rut_b = tk.Button(
            self, text="Exportar RUT-B", bg="#cccccc", fg="white",
            command=self.export_rut_b
        )
        self.btn_export_rut_b.grid(row=9, column=3, padx=5, pady=5)

        self.btn_export_rut_c = tk.Button(
            self, text="Exportar RUT-C", bg="#cccccc", fg="white",
            command=self.export_rut_c
        )
        self.btn_export_rut_c.grid(row=9, column=4, padx=5, pady=5)

        self.btn_export_rut_refinanciamiento = tk.Button(
            self, text="Exportar cruce c/Refinanciamiento (RUT)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.export_rut_refinanciamiento
        )
        self.btn_export_rut_refinanciamiento.grid(row=10, column=0, columnspan=5, pady=5)

        #
        # Botón para volver
        #
        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=11, column=0, columnspan=5, pady=20)


    # --------------------------------------------------------------------
    #       1) CARGAR ARCHIVO EXTRA (REFINANCIAMIENTO)
    # --------------------------------------------------------------------
    def _set_anio_ingreso(self):
        """
        Toma el valor del Entry, lo convierte a int y lo guarda en self.current_year.
        Si el valor no es numérico muestra un error.
        """
        valor_str = self.anio_ingresado.get().strip()
        try:
            self.current_year = int(valor_str)
            messagebox.showinfo("Año de ingreso",
                                f"El año se actualizó a {self.current_year}.")
        except ValueError:
            messagebox.showerror("Error",
                                 "Ingresa un año numérico válido (-ej. 2025-).")

    def apply_filter(self):
        """Filtra df_licitados por CODIGO_IES si se ingresó un valor."""
        global df_licitados
        codigo = self.codigo_ies_var.get().strip()
        if df_licitados is None:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        if codigo:
            self.df_licitados_query = df_licitados[df_licitados["CODIGO_IES"].astype(str) == codigo]
        else:
            self.df_licitados_query = df_licitados
        messagebox.showinfo("Filtrado", f"Registros: {len(self.df_licitados_query)}")
    def load_file_extra(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel - Archivo EXTRA (Refinanciamiento)")
        if df_csv is None:
            return
        # Validación básica
        if "RUT" not in df_csv.columns and "RUTALU" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo EXTRA no contiene 'RUT' o 'RUTALU'.")
            return

        self.df_extra = df_csv.copy()
        # Ajustar a tu caso: si manejas RUTALU, separas en RUT,DV
        if "RUTALU" in self.df_extra.columns:
            self.df_extra["RUTALU"] = self.df_extra["RUTALU"].astype(str)
            #self.df_extra[["RUT","DV"]] = self.df_extra["RUTALU"].str.split("-", expand=True)
            #self.df_extra.drop('DV', axis=1, inplace=True)
            self.df_extra = self.df_extra.rename(columns={'RUTALU': 'RUT'})
        self.label_file_extra.config(text=f"Archivo EXTRA: {os.path.basename(file_path)}")
        self.enable_extra_buttons()

    def enable_extra_buttons(self):
        """
        Habilita 'Operar con EXTRA' en #1, #2, #3 si sus DF ya existen.
        """
        if self.df_resultado_1 is not None and self.df_extra is not None:
            self.btn_usa_extra_1.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_1.config(state="disabled", bg="#cccccc")

        if self.df_resultado_2 is not None and self.df_extra is not None:
            self.btn_usa_extra_2.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_2.config(state="disabled", bg="#cccccc")

        if self.df_resultado_3 is not None and self.df_extra is not None:
            self.btn_usa_extra_3.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_3.config(state="disabled", bg="#cccccc")
    # ------------------------------------------------------------------
    #  MÉTODO NUEVO: exportar duplicados                               
    # ------------------------------------------------------------------
    def exportar_duplicados(self):
        """Guarda en Excel todos los registros que presentan duplicidad en RUT."""
        if self.df_duplicados is None or self.df_duplicados.empty:
            messagebox.showinfo("Sin duplicados", "No se encontraron registros duplicados por RUT.")
            return
        self._save_df_to_excel(self.df_duplicados, "Duplicados_RUT")

    # --------------------------------------------------------------------
    #       SUB-PROCESO #1
    # --------------------------------------------------------------------
    def load_file_licitados_1(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel Licitados #1")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #1 no contiene 'RUT'.")
            return
        if "MOROSOS" not in df_csv.columns:
            df_csv['MOROSOS'] = ""
        self.df_csv_1 = df_csv.copy()
        self.label_file_1.config(text=f"Archivo #1: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_1.config(state="normal", bg="#107FFD")
        self.btn_preview_1.config(bg="#107FFD")

    def run_licitados_1(self):
        global df_licitados
        if self.df_csv_1 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #1.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return

        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)

        df_csv = self.df_csv_1.copy()
        df_csv["RUT"] = df_csv["RUT"].astype(str)
        df_csv = df_csv.rename(columns={"GLOSA_NUEVA": "GLOSA_NUEVO", "GLOSA_SUPERIO": "GLOSA_SUPERIOR"})
        df_csv = df_csv[['RUT', 'IES_RESPALDO', 'NOMBRE_IES_RESPALDO','GLOSA_NUEVO','GLOSA_SUPERIOR','NO_VIDENTE','ESTUDIOS_EXTRANJEROS','EXTRANJERO','INFORMADO_CON_BEA','PSU_USADA','ACREDITACION_EXTRANJEROS_PDI','MOROSOS']]

        self.df_resultado_1 = pd.merge(self.df_licitados_query, df_csv, on="RUT", how="inner")
        self.df_resultado_1["RUT"] = self.df_resultado_1["RUT"].str.zfill(8)
        cond_gnew = (self.df_resultado_1['GLOSA_NUEVO'] == "Seleccionado Normal ESTADO_SELECCION = 1 - 2 - 3")
        cond_gsup = (self.df_resultado_1['GLOSA_SUPERIOR'] == "Seleccionado Normal ESTADO_SELECCION = 1 - 2 - 3")
        cond_ies = (self.df_resultado_1['CODIGO_IES'] == '013')
        mask_final = (cond_gnew | cond_gsup) & cond_ies

        df_cumple = self.df_resultado_1[mask_final].copy()
        df_no_cumple = self.df_resultado_1[~mask_final].copy()
        self.df_resultado_no_cruce_1 = df_no_cumple

        def generar_observacion(row):
            observaciones = []
            if row.get('NO_VIDENTE', 0) == 1:
                observaciones.append("no vidente")
            if row.get('ESTUDIOS_EXTRANJEROS', 0) == 1:
                observaciones.append("estudios extranjeros")
            extranjero_flag = (row.get('EXTRANJERO', 0) == 1) or (row.get('ACREDITACION_EXTRANJEROS_PDI', 0) == 1)
            if extranjero_flag:
                observaciones.append("extranjeros PDI")
            if row.get('INFORMADO_CON_BEA', 0) == 1:
                observaciones.append("BEA")
            psu_val = row.get('PSU_USADA', 0)
            if pd.notnull(psu_val/100) and psu_val >= 485:
                observaciones.append("cumple PSU")
            if row.get('MOROSO', 0) == 1:
                observaciones.append("morosos")
            observaciones_unicas = list(dict.fromkeys(observaciones))
            return ", ".join(observaciones_unicas)

        df_cumple['OBSERVACIONES'] = df_cumple.apply(generar_observacion, axis=1)
        self.df_resultado_cruce_1 = df_cumple

        self.btn_export_1.config(bg="#107FFD")
        self.btn_export_1_b.config(bg="#107FFD")
        self.btn_export_1_c.config(bg="#107FFD")
        self.enable_extra_buttons()
        messagebox.showinfo("Procesado", f"CUMPLE: {len(df_cumple)} | NO CUMPLE: {len(df_no_cumple)}")

    def export_licitados_1(self):
        if self.df_resultado_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#1).")
            return
        self._save_df_to_excel(self.df_resultado_1, "Licitados_Seleccionados_1")

    # Botones extra de ejemplo (sub-proceso #1)
    def export_licitados_1_b(self):
        if self.df_resultado_cruce_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#1-B).")
            return
        # Puedes cambiar la lógica si deseas filtrar o procesar de otra forma
        self._save_df_to_excel(self.df_resultado_cruce_1,"")

    def export_licitados_1_c(self):
        if self.df_resultado_no_cruce_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#1-C).")
            return
        self._save_df_to_excel(self.df_resultado_no_cruce_1,"")

    def operar_con_extra_1(self):
        if self.df_resultado_cruce_1 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "Sub-proceso #1 o archivo extra no cargados.")
            return
        df_out = pd.merge(self.df_resultado_cruce_1, self.df_extra, on="RUT", how="inner")
        messagebox.showinfo("Operación con Extra #1", f"Merged con df_extra. Filas: {len(df_out)}")
        self._save_df_to_excel(df_out, "Cruce_Extra_1")


    # --------------------------------------------------------------------
    #       SUB-PROCESO #2
    # --------------------------------------------------------------------
    def load_file_licitados_2(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel Licitados #2")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #2 no contiene 'RUT'.")
            return
        self.df_csv_2 = df_csv.copy()
        self.label_file_2.config(text=f"Archivo #2: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_2.config(state="normal", bg="#107FFD")
        self.btn_preview_2.config(bg="#107FFD")

    def run_licitados_2(self):
        global df_licitados
        if self.df_csv_2 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #2.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return

        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_csv = self.df_csv_2.copy()
        df_csv["RUT"] = df_csv["RUT"].astype(str)
        df_csv = df_csv.rename(columns={"GLOSA_NUEVA": "GLOSA_NUEVO", "GLOSA_SUPERIO": "GLOSA_SUPERIOR"})
        df_csv = df_csv[['RUT', 'IES_RESPALDO', 'NOMBRE_IES_RESPALDO','GLOSA_NUEVO','GLOSA_SUPERIOR','NO_VIDENTE','ESTUDIOS_EXTRANJEROS','EXTRANJERO','INFORMADO_CON_BEA','PSU_USADA','ACREDITACION_EXTRANJEROS_PDI','MOROSO']]

        df_cruce = pd.merge(self.df_licitados_query, df_csv, on="RUT", how="inner")
        print(len(df_cruce))
        # =============================
        # 2) Definir condiciones y FILTROS
        # =============================

        # -- A) Condición para eliminar según GLOSA_NUEVO --
        condicion_1 = (
            (df_cruce["GLOSA_NUEVO"] == "PRESELECCIONADOS DE 1ER AÑO CON RESTRICCIÓN CFT/IP (CORTE 1)") &
            (df_cruce["GLOSA_SUPERIOR"] != "PRESELECCIONADOS DE CURSO SUPERIOR (CORTE 1)")
        )
        condicion_2 = (
            (df_cruce["GLOSA_NUEVO"] == "ELIMINADO POR NO ELEGIBLE ACADÉMICAMENTE PARA 1ER AÑO") &
            (df_cruce["GLOSA_SUPERIOR"] != "PRESELECCIONADOS DE CURSO SUPERIOR (CORTE 1)")
        )
        cond_gnew = ~(condicion_1 | condicion_2)

        # -- B) Condiciones sobre GLOSA_SUPERIOR --

        # (1) "Preseleccionados de Primer año (corte 1)"
        cond_primer_anio = (
            df_cruce['GLOSA_SUPERIOR'] == "Preseleccionados de Curso Superior (corte 1)"  # Ejemplo de texto con "1"
        )

        # (2) "Preseleccionados de Curso Superior (corte 1)"
        #     => Se conserva si (IES_RESPALDO == 13) O ((AÑO_INGRESO_CARRERA < 2025) Y (PORCENTAJE_AVANCE >= 70))
        year_ref = self.current_year  
        cond_curso_superior = (
            (df_cruce['GLOSA_SUPERIOR'] == "Preseleccionados de Curso Superior (corte 1)") &
            (
               # (df_cruce['IES_RESPALDO'] == 13) |
                (
                    (df_cruce['AÑO_INGRESO_CARRERA'] < year_ref ) &
                    (df_cruce['PORCENTAJE_AVANCE'] >= 70)
                )
            )
            
            
        )

        # (3) "Eliminado por incumplimiento del avance académico curso superior"
        #     => Se conserva SOLO si IES_RESPALDO == 13, AÑO_INGRESO_CARRERA < 2025 y PORCENTAJE_AVANCE >= 70
        year_ref = self.current_year 
        cond_eliminado_avance = (
            (df_cruce['GLOSA_SUPERIOR'] == "Eliminado por no respaldo para curso superior") &
            #(df_cruce['IES_RESPALDO'] == 13) &
            (df_cruce['AÑO_INGRESO_CARRERA'] < year_ref) &
            (df_cruce['PORCENTAJE_AVANCE'] >= 70)
        )

        # Unificamos todas las condiciones de GLOSA_SUPERIOR (O lógico entre ellas)
        cond_gsup = cond_primer_anio | cond_curso_superior | cond_eliminado_avance

        # La máscara final exige:
        # - Cumplir la condición sobre GLOSA_NUEVO (cond_gnew)
        # - Cumplir la condición sobre GLOSA_SUPERIOR (cond_gsup)
        mask_final = cond_gnew & cond_gsup

        # =============================
        # 3) Separar en 2 DataFrames
        # =============================
        df_cruce["RUT"] = df_cruce["RUT"].str.zfill(8)
        #
        self.df_resultado_cruce_2 = df_cruce[mask_final].copy()
        self.df_resultado_no_cruce_2 = df_cruce[~mask_final].copy()
        self.df_resultado_2 = df_cruce

        # =========================
        # 4) Crear la columna OBSERVACIONES en df_resultado_cruce_2
        # =========================
        def generar_observacion(row):
            observaciones = []

            if row.get('NO_VIDENTE', 0) == 1:
                observaciones.append("no vidente")

            if row.get('ESTUDIOS_EXTRANJEROS', 0) == 1:
                observaciones.append("estudios extranjeros")

            # Tanto EXTRANJERO como ACREDITACION_EXTRANJEROS_PDI dan la observación "extranjeros PDI"
            extranjero_flag = (row.get('EXTRANJERO', 0) == 1) or (row.get('ACREDITACION_EXTRANJEROS_PDI', 0) == 1)
            if extranjero_flag:
                observaciones.append("extranjeros PDI")

            if row.get('INFORMADO_CON_BEA', 0) == 1:
                observaciones.append("BEA")

            # PSU_USADA >= 485 => "cumple PSU"
            psu_val = row.get('PSU_USADA', 0)
            if pd.notnull(psu_val) and (psu_val/100) >= 485:
                observaciones.append("cumple PSU")

            # Moroso: si el código es 1 dejar observación “moroso”
            if row.get('MOROSO', 0) == 1:
                observaciones.append("Morosos")

            # Evitar duplicados (si se repite "extranjeros PDI", etc.)
            observaciones_unicas = list(dict.fromkeys(observaciones))
            return ", ".join(observaciones_unicas)

        self.df_resultado_cruce_2['OBSERVACIONES'] = self.df_resultado_cruce_2.apply(generar_observacion, axis=1)

        # =================================
        # 5) Resultado final
        # =================================
        self.df_resultado_2 = self.df_resultado_2.reset_index(drop=True)

        self.btn_export_2.config(bg="#107FFD")
        self.btn_export_2_b.config(bg="#107FFD")
        self.btn_export_2_c.config(bg="#107FFD")
        self.enable_extra_buttons()


    def export_licitados_2(self):
        if self.df_resultado_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#2).")
            return
        self._save_df_to_excel(self.df_resultado_2, "Licitados_Preseleccionados_2")

    # Botones extra de ejemplo (sub-proceso #2)
    def export_licitados_2_b(self):
        if self.df_resultado_cruce_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#2-B).")
            return
        self._save_df_to_excel(self.df_resultado_cruce_2, "Licitados_2_B")

    def export_licitados_2_c(self):
        if self.df_resultado_no_cruce_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#2-C).")
            return
        self._save_df_to_excel(self.df_resultado_no_cruce_2, "Licitados_2_C")

    def operar_con_extra_2(self):
        if self.df_resultado_cruce_2 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "Sub-proceso #2 o archivo extra no cargados.")
            return
        df_out = pd.merge(self.df_resultado_2, self.df_extra, on="RUT", how="inner")
        messagebox.showinfo("Operación con Extra #2", f"Merged con df_extra. Filas: {len(df_out)}")
        self._save_df_to_excel(df_out, "Cruce_Extra_2")

# --------------------------------------------------------------------
    #       SUB-PROCESO #3
    # --------------------------------------------------------------------
    def load_file_licitados_3(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel Licitados #3")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #3 no contiene la columna 'RUT'.")
            return
        self.df_csv_3 = df_csv.copy()
        self.label_file_3.config(text=f"Archivo #3: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_3.config(state="normal", bg="#107FFD")
        self.btn_preview_3.config(bg="#107FFD")

    def run_licitados_3(self):
        global df_licitados
        if self.df_csv_3 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #3.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv = self.df_csv_3.copy()
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        self.df_resultado_3 = pd.merge(self.df_licitados_query, df_csv, on="RUT", how="inner")
        self.btn_export_3.config(bg="#107FFD")
        self.enable_extra_buttons()
        messagebox.showinfo("Procesado", f"Registros: {len(self.df_resultado_3)}")

    def export_licitados_3(self):
        if self.df_resultado_3 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#3).")
            return
        self._save_df_to_excel(self.df_resultado_3, "Licitados_NoSeleccionados_3")

    def operar_con_extra_3(self):
        """
        Luego de cruzar con df_extra, guardamos en self.df_resultado_3_extra
        y habilitamos el botón "Cargar 3b".
        """
        if self.df_resultado_3 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "Sub-proceso #3 o archivo extra no cargados.")
            return
        df_out = pd.merge(self.df_resultado_3, self.df_extra, on="RUT", how="inner")
        messagebox.showinfo("Operación con Extra #3", f"Merged con df_extra. Filas: {len(df_out)}")
        self.df_resultado_3_extra = df_out
        self._save_df_to_excel(df_out, "Cruce_Extra_3")

        # Habilitamos "Cargar 3b"
        self.btn_cargar_3b.config(state="normal", bg="#107FFD")

    def load_file_licitados_3b(self):
        """
        Se llama después de operar_con_extra_3.
        Toma self.df_resultado_3_extra y lo cruza con este nuevo archivo #3b.
        """
        if self.df_resultado_3_extra is None:
            messagebox.showwarning("Falta Data", "Primero operar con Extra #3 (no hay df_resultado_3_extra).")
            return

        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel para df_resultado_3_extra (3b)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #3b no contiene la columna 'RUT'.")
            return

        df_csv["RUT"] = df_csv["RUT"].astype(str)
        df_final_3b = pd.merge(self.df_resultado_3_extra, df_csv, on="RUT", how="inner")

        messagebox.showinfo(
            "Archivo #3b cargado",
            f"Merge con df_resultado_3_extra. Filas: {len(df_final_3b)}"
        )
        self._save_df_to_excel(df_final_3b, "Salida_Final_3b")


    # --------------------------------------------------------------------
    #       NUEVO SUB-PROCESO "RUT"
    # --------------------------------------------------------------------
    def load_file_rut(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel RUT (Adicional)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo RUT no contiene la columna 'RUT'.")
            return

        df_csv["RUT"] = df_csv["RUT"].astype(str)
        self.df_csv_rut = df_csv

        self.label_file_rut.config(text=f"Archivo RUT: {os.path.basename(file_path)}")
        messagebox.showinfo("Cargado", f"Archivo RUT con {len(df_csv)} filas.")

    def export_rut_matricula(self):
        if self.df_csv_rut is None:
            messagebox.showwarning("Falta archivo RUT", "Primero carga el archivo RUT (Adicional).")
            return
        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        self.df_csv_rut["RUT"] = self.df_csv_rut["RUT"].astype(str)

        df_rut_merge = pd.merge(df_licitados, self.df_csv_rut, on="RUT", how="inner")
        if df_rut_merge.empty:
            messagebox.showwarning("Cruce vacío", "No se encontraron coincidencias (RUT vs df_licitados).")
            return

        self.df_resultado_rut = df_rut_merge
        self._save_df_to_excel(df_rut_merge, "Cruce_Matricula_RUT")

        # Habilitamos "Exportar cruce c/Refinanciamiento (RUT)"
        self.btn_export_rut_refinanciamiento.config(state="normal", bg="#107FFD")
        messagebox.showinfo("Cruce con Matrícula (RUT)", "Exportado con éxito. Ya puedes cruzar con Refinanciamiento.")

    def export_rut_refinanciamiento(self):
        if self.df_resultado_rut is None or self.df_resultado_rut.empty:
            messagebox.showwarning("Sin datos", "Primero exporta el cruce con Matrícula (RUT).")
            return
        if self.df_extra is None or self.df_extra.empty:
            messagebox.showwarning("Sin datos", "No se ha cargado el Excel de refinanciamiento o está vacío.")
            return

        self.df_resultado_rut["RUT"] = self.df_resultado_rut["RUT"].astype(str)
        self.df_extra["RUT"] = self.df_extra["RUT"].astype(str)

        df_rut_ref = pd.merge(self.df_resultado_rut, self.df_extra, on="RUT", how="inner")
        if df_rut_ref.empty:
            messagebox.showwarning("Cruce vacío", "No hubo coincidencias con Refinanciamiento (RUT).")
            return

        self._save_df_to_excel(df_rut_ref, "Cruce_Refinanciamiento_RUT")
        messagebox.showinfo("Cruce con Refinanciamiento (RUT)", "Exportado con éxito.")

    # Botones extra de ejemplo (sub-proceso RUT)
    def export_rut_b(self):
        if self.df_csv_rut is None:
            messagebox.showwarning("Sin datos", "No hay datos de RUT para exportar (RUT-B).")
            return
        self._save_df_to_excel(self.df_csv_rut, "RUT_B")

    def export_rut_c(self):
        if self.df_csv_rut is None:
            messagebox.showwarning("Sin datos", "No hay datos de RUT para exportar (RUT-C).")
            return
        self._save_df_to_excel(self.df_csv_rut, "RUT_C")


    # --------------------------------------------------------------------
    #       FUNCIÓN AUX DE EXPORT A EXCEL
    # --------------------------------------------------------------------
    def _save_df_to_excel(self, df, default_name: str):
        file_path = filedialog.asksaveasfilename(
            title="Guardar archivo Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            initialfile=f"{default_name}.xlsx"
        )
        if file_path:
            try:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Se guardó en:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar:\n{e}")
        else:
            messagebox.showinfo("Cancelado", "No se exportó el archivo.")

    def _show_df(self, df, title):
        if df is None or df.empty:
            messagebox.showinfo("Sin datos", "No hay datos para mostrar.")
            return
        win = tk.Toplevel(self)
        win.title(title)
        frame = ttk.Frame(win)
        frame.pack(fill="both", expand=True)
        cols = list(df.columns)
        tree = ttk.Treeview(frame, columns=cols, show="headings")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        for c in cols:
            tree.heading(c, text=c)
            tree.column(c, width=120, anchor="w")
        for _, row in df.head(50).iterrows():
            tree.insert("", "end", values=[row[c] for c in cols])



class IngresaRenovantesFrame(tk.Frame):
    """
    Clase con:
      - Un botón para cargar un archivo 'extra' (self.df_extra).
      - 5 sub-procesos (#1, #2, #3, #4, #5) para cargar, exportar y operar con 'df_extra'.
    """

    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")
        
        # df_licitados se asume global
        global df_licitados
        self.df_licitados_query = df_licitados
        # filtro opcional CODIGO_IES
        self.codigo_ies_var = tk.StringVar(value="")

        # DataFrame donde se cargará el archivo extra
        self.df_extra = None

        # Resultados de cada sub-proceso
        self.df_resultado_1 = None
        self.df_resultado_2 = None
        self.df_resultado_3 = None
        self.df_resultado_4 = None
        self.df_resultado_5 = None

        # DataFrames "cumple" y "no cumple" para #2 y #3
        self.df_resultado_cruce_2 = None
        self.df_resultado_no_cruce_2 = None
        self.df_resultado_cruce_3 = None
        self.df_resultado_no_cruce_3 = None

        # CSV temporales
        self.df_csv_1 = None
        self.df_csv_2 = None
        self.df_csv_3 = None
        self.df_csv_4 = None
        self.df_csv_5 = None

        #
        # Layout base
        #
        for row_idx in range(15):
            self.rowconfigure(row_idx, weight=1)
        # Aumentamos a 8 columnas por nuevos controles
        for col_idx in range(8):
            self.columnconfigure(col_idx, weight=1)

        tk.Label(
            self, text="RENOVANTES", font=("Arial", 16, "bold"),
            bg="#FFFFFF"
        ).grid(row=0, column=0, padx=5)

        tk.Label(self, text="Código IES:", bg="#FFFFFF")\
            .grid(row=0, column=1, sticky="e")
        tk.Entry(self, textvariable=self.codigo_ies_var, width=6)\
            .grid(row=0, column=2, sticky="w")
        tk.Button(
            self, text="Filtrar", command=self.apply_filter,
            bg="#107FFD", fg="white", width=8
        ).grid(row=0, column=3, padx=5, sticky="w")

        #
        # 1) Botón para cargar el archivo EXTRA (df_extra)
        #
        btn_cargar_extra = tk.Button(
            self, text="Cargar Refinanciamiento", bg="#008000", fg="white",
            command=self.load_file_extra
        )
        btn_cargar_extra.grid(row=1, column=0, padx=5, pady=5, sticky="w")

        self.label_file_extra = tk.Label(self, text="Sin archivo de refinanciamiento", bg="#FFFFFF")
        self.label_file_extra.grid(row=1, column=1, columnspan=6, padx=5, pady=5, sticky="w")

        #
        # Sub-proceso #1
        #
        self.btn_cargar_1 = tk.Button(
            self, text="Cargar Reporte 5A Final", bg="#107FFD", fg="white",
            command=self.load_file_1
        )
        self.btn_cargar_1.grid(row=2, column=0, padx=5, pady=5)

        self.label_file_1 = tk.Label(self, text="Sin archivo (#1)", bg="#FFFFFF")
        self.label_file_1.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_1 = tk.Button(
            self, text="Exportar cruce con matricula #1", bg="#cccccc", fg="white",
            command=self.export_1
        )
        self.btn_export_1.grid(row=2, column=2, padx=5, pady=5)

        self.btn_run_1 = tk.Button(
            self, text="Run #1", bg="#cccccc", fg="white", state="disabled",
            command=self.run_1
        )
        self.btn_run_1.grid(row=2, column=3, padx=5, pady=5)

        self.btn_preview_1 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_1, "Preview #1")
        )
        self.btn_preview_1.grid(row=2, column=4, padx=5, pady=5)

        self.btn_usa_extra_1 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#1)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_1
        )
        self.btn_usa_extra_1.grid(row=3, column=0, columnspan=8, pady=5)


        #
        # Sub-proceso #2
        #
        self.btn_cargar_2 = tk.Button(
            self, text="Cargar Reporte 5B Final", bg="#107FFD", fg="white",
            command=self.load_file_2
        )
        self.btn_cargar_2.grid(row=4, column=0, padx=5, pady=5)

        self.label_file_2 = tk.Label(self, text="Sin archivo (#2)", bg="#FFFFFF")
        self.label_file_2.grid(row=4, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_2 = tk.Button(
            self, text="Exportar cruce con matricula #2", bg="#cccccc", fg="white",
            command=self.export_2
        )
        self.btn_export_2.grid(row=4, column=2, padx=5, pady=5)

        # NUEVOS BOTONES: Exportar Cumple #2 y Exportar No Cumple #2
        self.btn_export_2_cumple = tk.Button(
            self, text="Exportar Cumple #2", bg="#cccccc", fg="white",
            command=self.export_2_cumple
        )
        self.btn_export_2_cumple.grid(row=4, column=3, padx=5, pady=5)

        self.btn_export_2_no_cumple = tk.Button(
            self, text="Exportar No Cumple #2", bg="#cccccc", fg="white",
            command=self.export_2_no_cumple
        )
        self.btn_export_2_no_cumple.grid(row=4, column=4, padx=5, pady=5)

        self.btn_run_2 = tk.Button(
            self, text="Run #2", bg="#cccccc", fg="white", state="disabled",
            command=self.run_2
        )
        self.btn_run_2.grid(row=4, column=5, padx=5, pady=5)

        self.btn_preview_2 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_2, "Preview #2")
        )
        self.btn_preview_2.grid(row=4, column=6, padx=5, pady=5)

        self.btn_usa_extra_2 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#2)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_2
        )
        self.btn_usa_extra_2.grid(row=5, column=0, columnspan=8, pady=5)

        #
        # Sub-proceso #3
        #
        self.btn_cargar_3 = tk.Button(
            self, text="Cargar R. Proceso Anterior", bg="#107FFD", fg="white",
            command=self.load_file_3
        )
        self.btn_cargar_3.grid(row=6, column=0, padx=5, pady=5)

        self.label_file_3 = tk.Label(self, text="Sin archivo (#3)", bg="#FFFFFF")
        self.label_file_3.grid(row=6, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_3 = tk.Button(
            self, text="Exportar cruce con matricula #3", bg="#cccccc", fg="white",
            command=self.export_3
        )
        self.btn_export_3.grid(row=6, column=2, padx=5, pady=5)

        # NUEVOS BOTONES: Exportar Cumple #3 y Exportar No Cumple #3
        self.btn_export_3_cumple = tk.Button(
            self, text="Exportar Cumple #3", bg="#cccccc", fg="white",
            command=self.export_3_cumple
        )
        self.btn_export_3_cumple.grid(row=6, column=3, padx=5, pady=5)

        self.btn_export_3_no_cumple = tk.Button(
            self, text="Exportar No Cumple #3", bg="#cccccc", fg="white",
            command=self.export_3_no_cumple
        )
        self.btn_export_3_no_cumple.grid(row=6, column=4, padx=5, pady=5)

        self.btn_run_3 = tk.Button(
            self, text="Run #3", bg="#cccccc", fg="white", state="disabled",
            command=self.run_3
        )
        self.btn_run_3.grid(row=6, column=5, padx=5, pady=5)

        self.btn_preview_3 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_3, "Preview #3")
        )
        self.btn_preview_3.grid(row=6, column=6, padx=5, pady=5)

        self.btn_usa_extra_3 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#3)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_3
        )
        self.btn_usa_extra_3.grid(row=7, column=0, columnspan=8, pady=5)

        #
        # Sub-proceso #4
        #
        self.btn_cargar_4 = tk.Button(
            self, text="Cargar Varios", bg="#107FFD", fg="white",
            command=self.load_file_4
        )
        self.btn_cargar_4.grid(row=8, column=0, padx=5, pady=5)

        self.label_file_4 = tk.Label(self, text="Sin archivo (#4)", bg="#FFFFFF")
        self.label_file_4.grid(row=8, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_4 = tk.Button(
            self, text="Exportar cruce con matricula #4", bg="#cccccc", fg="white",
            command=self.export_4
        )
        self.btn_export_4.grid(row=8, column=2, padx=5, pady=5)

        self.btn_run_4 = tk.Button(
            self, text="Run #4", bg="#cccccc", fg="white", state="disabled",
            command=self.run_4
        )
        self.btn_run_4.grid(row=8, column=3, padx=5, pady=5)

        self.btn_preview_4 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_4, "Preview #4")
        )
        self.btn_preview_4.grid(row=8, column=4, padx=5, pady=5)

        self.btn_usa_extra_4 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#4)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_4
        )
        self.btn_usa_extra_4.grid(row=9, column=0, columnspan=8, pady=5)

        #
        # Sub-proceso #5
        #
        self.btn_cargar_5 = tk.Button(
            self, text="Cargar RUT", bg="#107FFD", fg="white",
            command=self.load_file_5
        )
        self.btn_cargar_5.grid(row=10, column=0, padx=5, pady=5)

        self.label_file_5 = tk.Label(self, text="Sin archivo (#5)", bg="#FFFFFF")
        self.label_file_5.grid(row=10, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_5 = tk.Button(
            self, text="Exportar cruce con matricula #5", bg="#cccccc", fg="white",
            command=self.export_5
        )
        self.btn_export_5.grid(row=10, column=2, padx=5, pady=5)

        self.btn_run_5 = tk.Button(
            self, text="Run #5", bg="#cccccc", fg="white", state="disabled",
            command=self.run_5
        )
        self.btn_run_5.grid(row=10, column=3, padx=5, pady=5)

        self.btn_preview_5 = tk.Button(
            self, text="Preview", bg="#cccccc", fg="white",
            command=lambda: self._show_df(self.df_csv_5, "Preview #5")
        )
        self.btn_preview_5.grid(row=10, column=4, padx=5, pady=5)

        self.btn_usa_extra_5 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#5)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_5
        )
        self.btn_usa_extra_5.grid(row=11, column=0, columnspan=8, pady=5)

        # (Opcional) Botón para volver a otro frame
        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=12, column=0, columnspan=8, pady=20)


    # ---------------------------------------------------------
    #   MÉTODO PARA CARGAR EL ARCHIVO EXTRA (self.df_extra)
    # ---------------------------------------------------------
    def load_file_extra(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel - Archivo EXTRA")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns and "RUTALU" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo EXTRA no contiene la columna 'RUT'.")
            return

        self.df_extra = df_csv.copy()
        self.df_extra['RUTALU'] = self.df_extra['RUTALU'].astype(str)
        self.df_extra = self.df_extra.rename(columns={'RUTALU': 'RUT'})
        #self.df_extra[["RUT","DV"]] = self.df_extra["RUTALU"].str.split("-", expand=True)
        self.label_file_extra.config(text=f"Archivo EXTRA: {os.path.basename(file_path)}")

        self.enable_extra_buttons()

    def apply_filter(self):
        """Filtra df_licitados por CODIGO_IES si se ingresó un valor."""
        global df_licitados
        codigo = self.codigo_ies_var.get().strip()
        if df_licitados is None:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        if codigo:
            self.df_licitados_query = df_licitados[df_licitados["CODIGO_IES"].astype(str) == codigo]
        else:
            self.df_licitados_query = df_licitados
        messagebox.showinfo("Filtrado", f"Registros: {len(self.df_licitados_query)}")

    # ---------------------------------------------------------
    #   FUNCIÓN QUE HABILITA/DESHABILITA LOS BOTONES EXTRA
    # ---------------------------------------------------------
    def enable_extra_buttons(self):
        """
        Habilita los botones 'Operar con EXTRA' para cada sub-proceso
        solo si ese sub-proceso se ha cargado y self.df_extra está cargado.
        """
        # Sub-proceso #1
        if self.df_resultado_1 is not None and self.df_extra is not None:
            self.btn_usa_extra_1.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_1.config(state="disabled", bg="#cccccc")

        # Sub-proceso #2
        if self.df_resultado_2 is not None and self.df_extra is not None:
            self.btn_usa_extra_2.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_2.config(state="disabled", bg="#cccccc")

        # Sub-proceso #3
        if self.df_resultado_3 is not None and self.df_extra is not None:
            self.btn_usa_extra_3.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_3.config(state="disabled", bg="#cccccc")

        # Sub-proceso #4
        if self.df_resultado_4 is not None and self.df_extra is not None:
            self.btn_usa_extra_4.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_4.config(state="disabled", bg="#cccccc")

        # Sub-proceso #5
        if self.df_resultado_5 is not None and self.df_extra is not None:
            self.btn_usa_extra_5.config(state="normal", bg="#107FFD")
        else:
            self.btn_usa_extra_5.config(state="disabled", bg="#cccccc")


    # =========================================================
    # =                   SUB-PROCESO #1                     =
    # =========================================================
    def load_file_1(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel #1")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #1 no contiene la columna 'RUT'.")
            return
        self.df_csv_1 = df_csv.copy()
        self.label_file_1.config(text=f"Archivo #1: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_1.config(state="normal", bg="#107FFD")
        self.btn_preview_1.config(bg="#107FFD")

    def run_1(self):
        global df_licitados
        if self.df_csv_1 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #1.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv = self.df_csv_1.copy()
        df_csv['RUT'] = df_csv['RUT'].astype(str)
        if 'IES' not in df_csv.columns:
            df_csv['IES'] = None
        self.df_resultado_1 = pd.merge(self.df_licitados_query, df_csv, on='RUT', how='inner')
        self.df_resultado_1 = self.df_resultado_1[self.df_resultado_1['IES'] == '013']
        self.btn_export_1.config(bg="#107FFD")
        self.enable_extra_buttons()
        messagebox.showinfo("Procesado", f"Registros: {len(self.df_resultado_1)}")

    def export_1(self):
        if self.df_resultado_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#1).")
            return
        self._save_df_to_excel(self.df_resultado_1, "Licitados_1")

    def operar_con_extra_1(self):
        if self.df_resultado_1 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "No se ha cargado sub-proceso #1 o el archivo extra.")
            return
        # Ejemplo de cruce con df_extra
        current_df_extra = self.df_extra[['RUTALU','DOCUMENTO', 'SALDO']] if 'DOCUMENTO' in self.df_extra.columns else self.df_extra[['RUTALU']]
        df_out = pd.merge(self.df_resultado_1, current_df_extra, on="RUT", how="inner")
        messagebox.showinfo(
            "Operación con Extra #1",
            f"Merged con df_extra. Filas: {len(df_out)}"
        )
        self._save_df_to_excel(df_out, "Cruce_Extra_1")


    # =========================================================
    # =                   SUB-PROCESO #2                     =
    # =========================================================
    def load_file_2(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel #2")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #2 no contiene la columna 'RUT'.")
            return
        self.df_csv_2 = df_csv.copy()
        self.label_file_2.config(text=f"Archivo #2: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_2.config(state="normal", bg="#107FFD")
        self.btn_preview_2.config(bg="#107FFD")

    def run_2(self):
        global df_licitados
        if self.df_csv_2 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #2.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return

        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv = self.df_csv_2.copy()
        df_csv['RUT'] = df_csv['RUT'].astype(str)
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_cruce_2 = pd.merge(self.df_licitados_query, df_csv, on='RUT', how='inner')

        estados_validos_5B = [4, 8, 12, 13, 18, 19, 21, 23, 24, 35]
        cond_cumple_2 = df_cruce_2['ESTADO_ACTUAL'].isin(estados_validos_5B)

        self.df_resultado_cruce_2 = df_cruce_2[cond_cumple_2].copy()
        self.df_resultado_no_cruce_2 = df_cruce_2[~cond_cumple_2].copy()
        self.df_resultado_2 = df_cruce_2

        self.btn_export_2.config(bg="#107FFD")
        self.btn_export_2_cumple.config(bg="#107FFD")
        self.btn_export_2_no_cumple.config(bg="#107FFD")

        self.enable_extra_buttons()
        messagebox.showinfo("Procesado", f"CUMPLE: {len(self.df_resultado_cruce_2)} | NO: {len(self.df_resultado_no_cruce_2)}")

    def export_2(self):
        if self.df_resultado_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#2).")
            return
        self._save_df_to_excel(self.df_resultado_2, "Licitados_2")

    # Métodos para exportar Cumple #2 y No Cumple #2
    def export_2_cumple(self):
        if self.df_resultado_cruce_2 is None or self.df_resultado_cruce_2.empty:
            messagebox.showwarning("Sin datos", "No hay datos 'cumple' para exportar (#2).")
            return
        self._save_df_to_excel(self.df_resultado_cruce_2, "Licitados_2_cumple")

    def export_2_no_cumple(self):
        if self.df_resultado_no_cruce_2 is None or self.df_resultado_no_cruce_2.empty:
            messagebox.showwarning("Sin datos", "No hay datos 'no cumple' para exportar (#2).")
            return
        self._save_df_to_excel(self.df_resultado_no_cruce_2, "Licitados_2_no_cumple")


    def operar_con_extra_2(self):
        if self.df_resultado_cruce_2 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "No se ha cargado sub-proceso #2 o el archivo extra.")
            return
        df_extra_2 = self.df_extra[['RUT','DOCUMENTO', 'SALDO']]
        df_out = pd.merge(self.df_resultado_cruce_2, df_extra_2, on="RUT", how="inner")
        df_out = df_out[self.df_resultado_cruce_2.columns]
        messagebox.showinfo(
            "Operación con Extra #2",
            f"Merged con df_extra. Filas: {len(df_out)}"
        )
        self._save_df_to_excel(df_out, "Cruce_Extra_2")


    # =========================================================
    # =                   SUB-PROCESO #3                     =
    # =========================================================
    def load_file_3(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel #3")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #3 no contiene la columna 'RUT'.")
            return
        self.df_csv_3 = df_csv.copy()
        self.label_file_3.config(text=f"Archivo #3: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_3.config(state="normal", bg="#107FFD")
        self.btn_preview_3.config(bg="#107FFD")

    def run_3(self):
        global df_licitados
        if self.df_csv_3 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #3.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv = self.df_csv_3.copy()
        df_csv['RUT'] = df_csv['RUT'].astype(str)
        df_csv = df_csv[['RUT','IESN_COD','ESTADO_RENOVANTE','CONTADOR_CAMBIOS']]
        df_cruce_3 = pd.merge(self.df_licitados_query, df_csv, on='RUT', how='inner')

        mask_iesn_13 = (df_cruce_3['IESN_COD'] == 13)
        mask_iesn_no_13 = (df_cruce_3['IESN_COD'] != 13)
        mask_estado_ok = ~df_cruce_3['ESTADO_RENOVANTE'].isin([7, 10, 11, 14, 15])
        mask_contador_ok = (df_cruce_3['CONTADOR_CAMBIOS'] == 0)

        mask_renovante_anterior = (
            mask_iesn_13 |
            (mask_iesn_no_13 & mask_estado_ok & mask_contador_ok)
        )

        df_cumple_renovante_anterior = df_cruce_3[mask_renovante_anterior].copy()
        df_no_cumple_renovante_anterior = df_cruce_3[~mask_renovante_anterior].copy()
        self.df_resultado_cruce_3 = df_cumple_renovante_anterior
        self.df_resultado_no_cruce_3 = df_no_cumple_renovante_anterior
        self.df_resultado_3 = df_cruce_3

        self.btn_export_3.config(bg="#107FFD")
        self.btn_export_3_cumple.config(bg="#107FFD")
        self.btn_export_3_no_cumple.config(bg="#107FFD")

        self.enable_extra_buttons()
        messagebox.showinfo("Procesado", f"CUMPLE: {len(df_cumple_renovante_anterior)} | NO: {len(df_no_cumple_renovante_anterior)}")

    def export_3(self):
        if self.df_resultado_3 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#3).")
            return
        self._save_df_to_excel(self.df_resultado_3, "Licitados_3")

    # Métodos para exportar Cumple #3 y No Cumple #3
    def export_3_cumple(self):
        if self.df_resultado_cruce_3 is None or self.df_resultado_cruce_3.empty:
            messagebox.showwarning("Sin datos", "No hay datos 'cumple' para exportar (#3).")
            return
        self._save_df_to_excel(self.df_resultado_cruce_3, "Licitados_3_cumple")

    def export_3_no_cumple(self):
        if self.df_resultado_no_cruce_3 is None or self.df_resultado_no_cruce_3.empty:
            messagebox.showwarning("Sin datos", "No hay datos 'no cumple' para exportar (#3).")
            return
        self._save_df_to_excel(self.df_resultado_no_cruce_3, "Licitados_3_no_cumple")


    def operar_con_extra_3(self):
        if self.df_resultado_cruce_3 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "No se ha cargado sub-proceso #3 o el archivo extra.")
            return
        current_extra = self.df_extra[['RUT','DOCUMENTO','SALDO']]
        df_out = pd.merge(self.df_resultado_cruce_3, current_extra, on="RUT", how="inner")
        messagebox.showinfo(
            "Operación con Extra #3",
            f"Merged con df_extra. Filas: {len(df_out)}"
        )
        self._save_df_to_excel(df_out, "Cruce_Extra_3")


    # =========================================================
    # =                   SUB-PROCESO #4                     =
    # =========================================================
    def load_file_4(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel #4")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #4 no contiene la columna 'RUT'.")
            return
        self.df_csv_4 = df_csv.copy()
        self.label_file_4.config(text=f"Archivo #4: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_4.config(state="normal", bg="#107FFD")
        self.btn_preview_4.config(bg="#107FFD")

    def run_4(self):
        global df_licitados
        if self.df_csv_4 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #4.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv = self.df_csv_4.copy()
        df_csv['RUT'] = df_csv['RUT'].astype(str)

        self.df_resultado_4 = pd.merge(self.df_licitados_query, df_csv, on='RUT', how='inner')
        self.btn_export_4.config(bg="#107FFD")

        self.enable_extra_buttons()
        messagebox.showinfo("Procesado", f"Registros: {len(self.df_resultado_4)}")

    def export_4(self):
        if self.df_resultado_4 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#4).")
            return
        self._save_df_to_excel(self.df_resultado_4, "Licitados_4")

    def operar_con_extra_4(self):
        if self.df_resultado_4 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "No se ha cargado sub-proceso #4 o el archivo extra.")
            return
        df_out = pd.merge(self.df_resultado_4, self.df_extra, on="RUT", how="inner")
        messagebox.showinfo(
            "Operación con Extra #4",
            f"Merged con df_extra. Filas: {len(df_out)}"
        )
        self._save_df_to_excel(df_out, "Cruce_Extra_4")


    # =========================================================
    # =                   SUB-PROCESO #5                     =
    # =========================================================
    def load_file_5(self):
        df_csv, file_path = read_any_file("Seleccionar CSV/TXT/Excel #5")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #5 no contiene la columna 'RUT'.")
            return
        self.df_csv_5 = df_csv.copy()
        self.label_file_5.config(text=f"Archivo #5: {os.path.basename(file_path)}")
        messagebox.showinfo("Archivo cargado", f"{len(df_csv)} filas cargadas")
        self.btn_run_5.config(state="normal", bg="#107FFD")
        self.btn_preview_5.config(bg="#107FFD")

    def run_5(self):
        global df_licitados
        if self.df_csv_5 is None:
            messagebox.showwarning("Sin archivo", "Primero carga el archivo #5.")
            return
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv = self.df_csv_5.copy()
        df_csv['RUT'] = df_csv['RUT'].astype(str)

        self.df_resultado_5 = pd.merge(self.df_licitados_query, df_csv, on='RUT', how='inner')
        self.btn_export_5.config(bg="#107FFD")

        self.enable_extra_buttons()
        messagebox.showinfo("Procesado", f"Registros: {len(self.df_resultado_5)}")

    def export_5(self):
        if self.df_resultado_5 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#5).")
            return
        self._save_df_to_excel(self.df_resultado_5, "Licitados_5")

    def operar_con_extra_5(self):
        if self.df_resultado_5 is None or self.df_extra is None:
            messagebox.showwarning("Falta Data", "No se ha cargado sub-proceso #5 o el archivo extra.")
            return
        df_out = pd.merge(self.df_resultado_5, self.df_extra, on="RUT", how="inner")
        messagebox.showinfo(
            "Operación con Extra #5",
            f"Merged con df_extra. Filas: {len(df_out)}"
        )
        self._save_df_to_excel(df_out, "Cruce_Extra_5")


    # ---------------------------------------------
    #  FUNCIÓN AUXILIAR PARA EXPORTAR A EXCEL
    # ---------------------------------------------
    def _save_df_to_excel(self, df, default_name: str):
        file_path = filedialog.asksaveasfilename(
            title="Guardar archivo Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            initialfile=f"{default_name}.xlsx"
        )
        if file_path:
            try:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Se guardó en:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar:\n{e}")
        else:
            messagebox.showinfo("Cancelado", "No se exportó el archivo.")




class SeguimientosFrame(tk.Frame):
    """
    Sub-proceso Licitados con 4 archivos (Firma en Banco, Firma en Certificación, 
    Reporte Licitados, Reporte con Categoría) que solo se cruzan con df_licitados.
    Se agrega un quinto proceso (RUT) que hace lo mismo (cruce con df_licitados).

    No incluye lógica de refinanciamiento ni "Operar con EXTRA".
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")
        
        global df_licitados
        self.df_licitados_query = df_licitados
        self.df_duplicados = df_licitados[df_licitados.duplicated(subset=["RUT"], keep=False)].copy()
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        
        # CSV temporales por sub‑proceso
        self.df_csv_1 = self.df_csv_2 = self.df_csv_3 = self.df_csv_4 = self.df_csv_rut = None
        # Resultados tras "Run"
        self.df_resultado_1 = self.df_resultado_2 = self.df_resultado_3 = self.df_resultado_4 = self.df_resultado_rut = None

        # -----------------------------------------------------------------
        #  Layout base 3×12
        cols = [1, 2, 3, 4, 5]  # (load, etiqueta, export OK, run, export NO)
        self.rowconfigure(list(range(12)), weight=1)
        for c in range(6):  # 0‑5
            self.columnconfigure(c, weight=1)

        # --- atributos dinámicos ---------------------------------------------------
        for idx in (1, 2, 3, 4, 5):
            setattr(self, f"df_csv_{idx}", None)
            setattr(self, f"df_ok_{idx}", None)
            setattr(self, f"df_nc_{idx}", None)

        # --- cabecera --------------------------------------------------------------
        tk.Label(self, text="Sub‑proceso: Seguimiento Firmas", font=("Arial", 16, "bold"), bg="#FFFFFF").grid(row=0, column=0, columnspan=6, pady=10)
        tk.Button(self, text="Exportar duplicados", bg="#FF8C00", fg="white", command=self.exportar_duplicados).grid(row=0, column=5, padx=5, pady=5, sticky="e")

        # --- helper para filas -----------------------------------------------------
        def _fila(idx, texto):
            row = idx
            # 0 Cargar
            tk.Button(self, text=texto, bg="#107FFD", fg="white", command=lambda i=idx: self.load_file(i)).grid(row=row, column=0, padx=5, pady=5)
            # 1 etiqueta
            setattr(self, f"lbl_{idx}", tk.Label(self, text=f"Sin archivo (#{idx})", bg="#FFFFFF"))
            getattr(self, f"lbl_{idx}").grid(row=row, column=1, sticky="w")
            # 2 export OK
            setattr(self, f"btn_exp_ok_{idx}", tk.Button(self, text=f"Export OK #{idx}", bg="#cccccc", fg="white", command=lambda i=idx: self.exportar(i, ok=True)))
            getattr(self, f"btn_exp_ok_{idx}").grid(row=row, column=2, padx=5, pady=5)
            # 3 run
            setattr(self, f"btn_run_{idx}", tk.Button(self, text=f"Run #{idx}", bg="#cccccc", fg="white", command=lambda i=idx: self.run_merge(i)))
            getattr(self, f"btn_run_{idx}").grid(row=row, column=3, padx=5, pady=5)
            # 4 export NO
            setattr(self, f"btn_exp_nc_{idx}", tk.Button(self, text=f"Sin Cruce #{idx}", bg="#cccccc", fg="white", command=lambda i=idx: self.exportar(i, ok=False)))
            getattr(self, f"btn_exp_nc_{idx}").grid(row=row, column=4, padx=5, pady=5)

        _fila(1, "Firma Banco (#1)")
        _fila(2, "Firma Certificación (#2)")
        _fila(3, "Reporte Licitados (#3)")
        _fila(4, "Reporte con Categoría (#4)")
        _fila(5, "Cargar RUT (Adicional)")

        # volver
        tk.Button(self, text="Volver", bg="#aaaaaa", fg="white", command=lambda: controller.show_frame("IngresaFrame")).grid(row=6, column=0, columnspan=6, pady=20)

    
    #============================================================================
   # ------------------------------------------------------------------
    #  Cargar archivo
    # ------------------------------------------------------------------
    def load_file(self, idx):
        nombres = {1: "Firma Banco (#1)", 2: "Firma Certificación (#2)", 3: "Reporte Licitados (#3)", 4: "Reporte con Categoría (#4)", 5: "Archivo RUT adicional"}
        df_csv, path = read_any_file(nombres[idx])
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", f"El archivo #{idx} no contiene 'RUT'.")
            return
        df_csv = df_csv[["RUT"]].copy()
        setattr(self, f"df_csv_{idx}", df_csv.copy())
        getattr(self, f"lbl_{idx}").config(text=f"Archivo #{idx}: {os.path.basename(path)}")
        getattr(self, f"btn_run_{idx}").config(bg="#107FFD")

    # ------------------------------------------------------------------
    #  Run merge
    # ------------------------------------------------------------------
    def run_merge(self, idx):
        global df_licitados
        if df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_csv = getattr(self, f"df_csv_{idx}")
        if df_csv is None:
            messagebox.showwarning("Sin archivo", f"Primero carga el archivo #{idx}.")
            return

        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        df_ok, df_nc = merge_and_clean(df_licitados, df_csv)
        setattr(self, f"df_ok_{idx}", df_ok)
        setattr(self, f"df_nc_{idx}", df_nc)

        # colores
        getattr(self, f"btn_exp_ok_{idx}").config(bg="#228B22" if not df_ok.empty else "#B22222")
        getattr(self, f"btn_exp_nc_{idx}").config(bg="#228B22" if not df_nc.empty else "#B22222")
        getattr(self, f"btn_run_{idx}").config(bg="#228B22")

        messagebox.showinfo("Cruce listo", f"#{idx}: {len(df_ok)} coincidentes, {len(df_nc)} sin cruce.")

    # ------------------------------------------------------------------
    #  Exportar
    # ------------------------------------------------------------------
    def exportar(self, idx, ok: bool = True):
        df_attr = f"df_ok_{idx}" if ok else f"df_nc_{idx}"
        df = getattr(self, df_attr)
        if df is None or df.empty:
            messagebox.showwarning("Sin datos", "No hay datos para exportar.")
            return
        tipo = "OK" if ok else "NoCruce"
        self._save_df_to_excel(df, f"Seguimiento_{tipo}_{idx}")

    # ------------------------------------------------------------------
    #  Exportar duplicados base
    # ------------------------------------------------------------------
    def exportar_duplicados(self):
        if self.df_duplicados.empty:
            messagebox.showinfo("Sin duplicados", "No se encontraron registros duplicados por RUT.")
            return
        self._save_df_to_excel(self.df_duplicados, "Duplicados_RUT")

    # ------------------------------------------------------------------
    #  Helper guardar Excel
    # ------------------------------------------------------------------
    def _save_df_to_excel(self, df, default_name):
        path = filedialog.asksaveasfilename(title="Guardar Excel", defaultextension=".xlsx", filetypes=[("Excel","*.xlsx")], initialfile=f"{default_name}.xlsx")
        if not path:
            return
        try:
            df.to_excel(path, index=False)
            messagebox.showinfo("Exportado", f"Archivo guardado en:\n{path}")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo exportar:\n{e}")





class EgresadosFrame(tk.Frame):
    """
     Sub-proceso EGRESADOS:
      - Query base (df_egresados) con formato final.
      - Cargar 5A, 5B, Deserotores -> Cruce / No Cruce por RUT.
      - Unificar cruces (sin eliminar duplicados) con columna ARCHIVO_ORIGEN.
      - Exportar/unificar/no_cruces y ver en pantalla.
      - Botón para quitar duplicados del Cruce Unificado.
    """
    COLUMN_ORDER = [
        "RUT","DV","PATERNO","MATERNO","NOMBRES","SEXO","FECHA_NACIMIENTO",
        "DIRECCION","NACIONALIDAD","COD_CIUDAD","COD_COMUNA","COD_REGION",
        "FONO FIJO","MAIL_INSTITUCIONAL","FECHA_EGRESO","ANO_COHORTE",
        "ANO_INGRESO_INSTITUCION","NOMBRE_CARRERA","CODIGO_TIPO_IES",
        "CODIGO_DE_IES","CODIGO_DE_SEDE","CODIGO_CARRERA","CODIGO_JORNADA",
        "JORNADA","ARANCEL_REAL_PESOS","ARANCEL_REFERENCIA","FECHA_ULTIMA_MATRICULA",
        # Columnas extra que quieras agregar…
    ]
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.connection = connection1
        self.config(bg="#FFFFFF")
        
        # --------------------------------------------------------
        # 1) Ejecutamos la query al iniciar y guardamos en df_egresados
        # --------------------------------------------------------
        # Ajusta la conexión a la que necesites (connection1 o connection2).
        # DataFrames
        self.df_egresados = pd.DataFrame()
        self.df_cruce_5a = None; self.df_nc_5a = None
        self.df_cruce_5b = None; self.df_nc_5b = None
        self.df_cruce_des = None; self.df_nc_des = None

        self.df_cruce_unificado = None
        self.df_cruce_unificado_sin_dup = None
        self.df_nc_union = None

        # Cargamos query al iniciar
        self.run_query_egresados()

        # ---------------- GRID ----------------
        for r in range(12):
            self.rowconfigure(r, weight=1)
        for c in range(6):
            self.columnconfigure(c, weight=1)

        # Logo
        #self._add_logo()

        tk.Label(self, text="Sub-proceso: Egresados", font=("Arial",16,"bold"),
                 bg="#FFFFFF").grid(row=1,column=0,columnspan=6,pady=(5,10))

        # ---- 5A ----
        self._make_file_section(row=2, tag="5A", load_cmd=self.load_5a)

        # ---- 5B ----
        self._make_file_section(row=3, tag="5B", load_cmd=self.load_5b)

        # ---- DESERTORES ----
        self._make_file_section(row=4, tag="DESERTORES", load_cmd=self.load_des)

        # ---- Botones Unificación / Duplicados / No Cruces ----
        self.btn_unificar = tk.Button(self, text="Unificar Cruces (5A+5B+DES)",
                                      bg="#cccccc", fg="white",
                                      command=self.unificar_cruces, state="disabled")
        self.btn_unificar.grid(row=5,column=0,padx=5,pady=5,sticky="we")

        self.btn_ver_unificado = tk.Button(self,text="Ver Cruce Unificado",
                                           bg="#cccccc",state="disabled",
                                           command=lambda:self._show_df(self.df_cruce_unificado,"Cruce Unificado"))
        self.btn_ver_unificado.grid(row=5,column=1,padx=5)

        self.btn_export_unificado = tk.Button(self,text="Exportar Unificado",
                                              bg="#cccccc",fg="white",state="disabled",
                                              command=lambda:self._save_df(self.df_cruce_unificado,"Cruce_Unificado"))
        self.btn_export_unificado.grid(row=5,column=2,padx=5)

        self.btn_quitar_dup = tk.Button(self,text="Quitar Duplicados (RUT)",
                                        bg="#cccccc",fg="white",state="disabled",
                                        command=self.quitar_duplicados_unificado)
        self.btn_quitar_dup.grid(row=5,column=3,padx=5)

        self.btn_export_unificado_sin = tk.Button(self,text="Exportar Unificado SIN Dup",
                                                  bg="#cccccc",fg="white",state="disabled",
                                                  command=lambda:self._save_df(self.df_cruce_unificado_sin_dup,"Cruce_Unificado_SinDup"))
        self.btn_export_unificado_sin.grid(row=5,column=4,padx=5)

        self.btn_export_nc_union = tk.Button(self,text="Exportar Unión NO Cruces",
                                             bg="#cccccc",fg="white",state="disabled",
                                             command=lambda:self._save_df(self.df_nc_union,"Union_NoCruces"))
        self.btn_export_nc_union.grid(row=5,column=5,padx=5)

        # Status
        self.lbl_status = tk.Label(self,text="",bg="#FFFFFF",anchor="w",justify="left")
        self.lbl_status.grid(row=6,column=0,columnspan=6,sticky="we")

        tk.Button(self,text="Volver",bg="#aaaaaa",fg="white",
                  command=lambda: controller.show_frame("IngresaFrame")).grid(row=11,column=0,columnspan=6,pady=(15,10))


    def _make_file_section(self, row:int, tag:str, load_cmd):
        """
        Crea fila con:
           Cargar <tag> | Ver Cruce | Export Cruce | Export No Cruce | Ver No Cruce
        """
        btn = tk.Button(self,text=f"Cargar {tag}",bg="#107FFD",fg="white",command=load_cmd)
        btn.grid(row=row,column=0,padx=5,pady=3,sticky="we")

        setattr(self,f"lbl_{tag}", tk.Label(self,text=f"Sin {tag}",bg="#FFFFFF"))
        getattr(self,f"lbl_{tag}").grid(row=row,column=1,sticky="w")

        setattr(self,f"btn_ver_cruce_{tag}",
                tk.Button(self,text="Ver Cruce",bg="#cccccc",state="disabled",
                          command=lambda t=tag: self._show_df(getattr(self,f'df_cruce_{self._tag_key(tag)}'),f"Cruce {t}")))
        getattr(self,f"btn_ver_cruce_{tag}").grid(row=row,column=2)

        setattr(self,f"btn_export_cruce_{tag}",
                tk.Button(self,text="Exportar Cruce",bg="#cccccc",fg="white",state="disabled",
                          command=lambda t=tag: self._save_df(getattr(self,f'df_cruce_{self._tag_key(tag)}'),f"Cruce_{t}")))
        getattr(self,f"btn_export_cruce_{tag}").grid(row=row,column=3)

        setattr(self,f"btn_ver_nc_{tag}",
                tk.Button(self,text="Ver NO Cruce",bg="#cccccc",state="disabled",
                          command=lambda t=tag: self._show_df(getattr(self,f'df_nc_{self._tag_key(tag)}'),f"NoCruce_{t}")))
        getattr(self,f"btn_ver_nc_{tag}").grid(row=row,column=4)

        setattr(self,f"btn_export_nc_{tag}",
                tk.Button(self,text="Exportar NO Cruce",bg="#cccccc",fg="white",state="disabled",
                          command=lambda t=tag: self._save_df(getattr(self,f'df_nc_{self._tag_key(tag)}'),f"NoCruce_{t}")))
        getattr(self,f"btn_export_nc_{tag}").grid(row=row,column=5)

    def _tag_key(self, tag:str)->str:
        return "des" if tag=="DESERTORES" else tag.lower()

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------
    def run_query_egresados(self):
        """
        Ejecuta la query y deja df_egresados con columnas en formato final.
        Evitamos post-procesar demasiado fuera: renombramos aquí.
        """
        query = text("""
            SELECT
                b.RUT,
                c.DV,
                PATERNO,
                MATERNO,
                NOMBRES,
                CASE WHEN GENERO='F' THEN 'F' ELSE 'M' END AS SEXO,
                c.FECH_NAC AS FECHA_NACIMIENTO,
                DIRECCION,
                NACIONALIDAD,
                (SELECT TOP 1 X.CODIGO_CIUDAD FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_CIUDAD,
                (SELECT TOP 1 X.CODIGO_COMUNA FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_COMUNA, 
                (SELECT TOP 1 X.CODIGO_REGION FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_REGION,
                c.TELEFONO AS [FONO FIJO],
                c.MAIL_UNIACC AS MAIL_INSTITUCIONAL,
                FECHA_EGRESO,
                pm.PERIODO AS ANO_COHORTE,
                pm.PERIODO AS ANO_INGRESO_INSTITUCION,
                d.NOMBRE_CARRERA,
                1 AS CODIGO_TIPO_IES,
                13 AS CODIGO_DE_IES,
                j.SEDEN_COD AS CODIGO_DE_SEDE,
                j.CARRN_COD AS CODIGO_CARRERA,
                j.JORNN_COD AS CODIGO_JORNADA,
                CASE WHEN j.JORNN_COD=1 THEN 'Diurno' ELSE 'Vespertino/Semipresencial/Online' END AS JORNADA,
                F.ARANCEL_ANUAL AS ARANCEL_REAL_PESOS,
                0 AS ARANCEL_REFERENCIA,
                fm.FECHA_MAT AS FECHA_ULTIMA_MATRICULA
            FROM ft_egreso a
              LEFT JOIN (SELECT DISTINCT CODCLI, PERIODO FROM ft_matricula WHERE MAT_N=1) pm ON a.codcli = pm.codcli
              INNER JOIN dim_matricula b ON a.CODCLI = b.CODCLI
              INNER JOIN dim_alumno c ON b.RUT = c.RUT
              INNER JOIN dim_plan_academico d ON b.CODPLAN = d.LLAVE_MALLA
              INNER JOIN (
                    SELECT CODIGO_SIES, ARANCEL_ANUAL,
                           ROW_NUMBER() over (partition by CODIGO_SIES order by periodo desc ) numero
                    FROM dim_oferta_academica
              ) f ON d.CODIGO_SIES_COMPLETO = f.CODIGO_SIES AND numero=1
              LEFT JOIN dim_territorio i ON c.COMUNA=i.COMUNA
              LEFT JOIN (
                    SELECT DISTINCT [CODIGO SIES SIN VERSION], SEDEN_COD, CARRN_COD, JORNN_COD, NOMBRE_CARRERA,
                           ROW_NUMBER() OVER (PARTITION BY [CODIGO SIES SIN VERSION] ORDER BY [CODIGO SIES SIN VERSION]) numero
                    FROM oferta_academica_ingresa
                    WHERE carrera_discontinua='NO'
              ) j ON LEFT(d.CODIGO_SIES_COMPLETO, LEN(d.CODIGO_SIES_COMPLETO)-2)=j.[CODIGO SIES SIN VERSION]
              INNER JOIN (
                    SELECT CODCLI, FECHA_MAT,
                           ROW_NUMBER() OVER (PARTITION BY CODCLI ORDER BY FECHA_MAT DESC) AS numero
                    FROM ft_matricula
              ) fm ON a.CODCLI = fm.CODCLI AND fm.numero=1
            WHERE d.NIVEL_GLOBAL='PREGRADO'
              AND d.CODIGO_SIES_COMPLETO <> '0'
        """)
        try:
            if self.connection is not None:
                df = pd.read_sql_query(query, self.connection)
                # Limpieza y orden
                df["RUT"] = _clean_rut(df["RUT"])
                # Aplica validaciones reutilizadas
                df = validate_minud(_ensure(df))
                # Forzamos orden de columnas (las que existan)
                cols = [c for c in self.COLUMN_ORDER if c in df.columns]
                self.df_egresados = df[cols].copy()
                print(f"Query egresados OK: {len(self.df_egresados)} filas.")
            else:
                messagebox.showerror("Error","No hay conexión para ejecutar la query.")
        except Exception as e:
            messagebox.showerror("Error", f"Fallo al ejecutar query: {e}")

    # ------------------------------------------------------------------
    # LOADERS
    # ------------------------------------------------------------------
    def load_5a(self):  self._load_generic(tag="5A")
    def load_5b(self):  self._load_generic(tag="5B")
    def load_des(self): self._load_generic(tag="DESERTORES")

    def _load_generic(self, tag:str):
        df, path = read_any_file(f"Archivo {tag}")
        if df is None: return
        if "RUT" not in df.columns:
            messagebox.showerror("Error", f"{tag} sin columna RUT"); return

        df["RUT"] = _clean_rut(df["RUT"])
        self.df_egresados["RUT"] = _clean_rut(self.df_egresados["RUT"])

        # Cruce
        cruce = pd.merge(df, self.df_egresados, on="RUT", how="inner")  # mantiene columnas archivo + query
        cruce.insert(0,"ARCHIVO_ORIGEN",tag)
        # No Cruce
        nc = df[~df["RUT"].isin(cruce["RUT"])].copy()

        # Guardamos
        key = self._tag_key(tag)
        setattr(self, f"df_cruce_{key}", cruce)
        setattr(self, f"df_nc_{key}", nc)

        getattr(self, f"lbl_{tag}").config(text=os.path.basename(path))
        getattr(self, f"btn_ver_cruce_{tag}").config(state="normal",bg="#107FFD")
        getattr(self, f"btn_export_cruce_{tag}").config(state="normal",bg="#107FFD")
        getattr(self, f"btn_ver_nc_{tag}").config(state="normal",bg="#107FFD")
        getattr(self, f"btn_export_nc_{tag}").config(state="normal",bg="#107FFD")

        messagebox.showinfo("Cargado",
                            f"{tag} cargado.\nCruce: {len(cruce)} filas\nNo Cruce: {len(nc)} filas.")

        # Habilitamos unificación si existen al menos uno
        if any(getattr(self,f"df_cruce_{k}") is not None for k in ["5a","5b","des"]):
            self.btn_unificar.config(state="normal",bg="#107FFD")

        self._refresh_status()

    # ------------------------------------------------------------------
    # Unificación / Duplicados
    # ------------------------------------------------------------------
    def unificar_cruces(self):
        frames = []
        for k in ["5a","5b","des"]:
            dfc = getattr(self,f"df_cruce_{k}")
            if dfc is not None and not dfc.empty:
                frames.append(dfc)
        if not frames:
            messagebox.showinfo("Sin datos","No hay cruces para unificar."); return

        self.df_cruce_unificado = pd.concat(frames, ignore_index=True)
        # Unión NO cruces (por si la quieres exportar)
        nc_frames = []
        for k in ["5a","5b","des"]:
            dfnc = getattr(self,f"df_nc_{k}")
            if dfnc is not None and not dfnc.empty:
                nc_frames.append(dfnc[["RUT"]])
        self.df_nc_union = pd.concat(nc_frames, ignore_index=True).drop_duplicates() if nc_frames else pd.DataFrame(columns=["RUT"])

        self.btn_ver_unificado.config(state="normal",bg="#107FFD")
        self.btn_export_unificado.config(state="normal",bg="#107FFD")
        self.btn_quitar_dup.config(state="normal",bg="#107FFD")
        if not self.df_nc_union.empty:
            self.btn_export_nc_union.config(state="normal",bg="#107FFD")

        messagebox.showinfo("Unificado", f"Cruce unificado creado: {len(self.df_cruce_unificado)} filas (con duplicados).")
        self._refresh_status()

    def quitar_duplicados_unificado(self):
        if self.df_cruce_unificado is None:
            return
        # Conserva la primera aparición por RUT
        self.df_cruce_unificado_sin_dup = self.df_cruce_unificado.sort_index().drop_duplicates(subset="RUT", keep="first")
        self.btn_export_unificado_sin.config(state="normal",bg="#107FFD")
        self._show_df(self.df_cruce_unificado_sin_dup,"Cruce Unificado SIN Duplicados")
        self._refresh_status()

    # ------------------------------------------------------------------
    # Estado
    # ------------------------------------------------------------------
    def _refresh_status(self):
        parts=[]
        for tag,k in [("5A","5a"),("5B","5b"),("DES","des")]:
            cr = getattr(self,f"df_cruce_{k}")
            nc = getattr(self,f"df_nc_{k}")
            if cr is not None:
                parts.append(f"{tag}: Cruce {len(cr)} / NoCruce {len(nc) if nc is not None else 0}")
        if self.df_cruce_unificado is not None:
            parts.append(f"Unificado: {len(self.df_cruce_unificado)} filas")
        if self.df_cruce_unificado_sin_dup is not None:
            parts.append(f"Unificado SIN Dup: {len(self.df_cruce_unificado_sin_dup)} filas")
        if self.df_nc_union is not None and not self.df_nc_union.empty:
            parts.append(f"Unión NO Cruces: {len(self.df_nc_union)} RUT únicos")
        self.lbl_status.config(text=" | ".join(parts))

    # ------------------------------------------------------------------
    # Helpers ver/exportar
    # ------------------------------------------------------------------
    def _show_df(self, df, title):
        if df is None or df.empty:
            messagebox.showinfo("Sin datos","No hay datos para mostrar."); return
        win = tk.Toplevel(self); win.title(title)
        frame = ttk.Frame(win); frame.pack(fill="both",expand=True)
        cols=list(df.columns)
        tree=ttk.Treeview(frame,columns=cols,show="headings")
        vsb=ttk.Scrollbar(frame,orient="vertical",command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left",fill="both",expand=True); vsb.pack(side="right",fill="y")
        for c in cols:
            tree.heading(c,text=c); tree.column(c,width=120,anchor="w")
        # limit rows for speed (opcional)
        for _,row in df.iterrows():
            tree.insert("", "end", values=[row[c] for c in cols])

    def _save_df(self, df, default_name):
        if df is None or df.empty:
            messagebox.showinfo("Sin datos","No hay datos para exportar."); return
        path=filedialog.asksaveasfilename(
            title="Guardar Excel",defaultextension=".xlsx",
            filetypes=[("Excel","*.xlsx")],
            initialfile=f"{default_name}.xlsx"
        )
        if not path: return
        try:
            df.to_excel(path,index=False)
            messagebox.showinfo("Exportado",f"Guardado en:\n{path}")
        except Exception as e:
            messagebox.showerror("Error",str(e))

#====================================================================================
#====================================================================================

class BecasFrame(tk.Frame):
    """
    Pantalla principal de Becas, con posibilidad de abrir “BecasRenovantesFrame”
    (que contiene todo tu código grande).
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        tk.Label(
            self, text="Pantalla: Becas", bg="#FFFFFF", fg="#107FFD",
            font=("Arial", 20, "bold")
        ).pack(pady=20)

        tk.Button(
            self, text="Renovantes (Becas)", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("BecasRenovantesFrame"),
            width=20
        ).pack(pady=5)
        
        tk.Button(
            self, text="Matrícula y Validaciones", bg="#107FFD", fg="white",
            command=lambda: controller.show_frame("MatriculayValidaciones"),
            width=20
        ).pack(pady=5)

        tk.Button(
            self, text="Volver Menú", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("MainMenuFrame"),
            width=20
        ).pack(pady=20)


#=====================================================
# --------------- MatriculayValidaciones ---------------
#=====================================================



# Ejemplo de variables/funciones globales que se asumen existentes:
# - connection1, connection2 (conexiones a tu BD)
# - clean_text (función para limpiar texto)
# - df_resultado_11 (DataFrame global)
# Asegúrate de que estén definidas antes de usar esta clase.

class MatriculayValidaciones(tk.Frame): 

    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        # Configurar grid
        for row_idx in range(12):
            self.rowconfigure(row_idx, weight=1)
        for col_idx in range(3):
            self.columnconfigure(col_idx, weight=1)
            
        # Título principal
        tk.Label(
            self,
            text="Matrícula y Validaciones",
            bg="#FFFFFF",
            fg="#107FFD",
            font=("Arial", 20, "bold")
        ).grid(row=0, column=0, columnspan=3, pady=10)


        # Botón "Volver" (usar grid en lugar de pack)
        tk.Button(
            self,
            text="Volver",
            bg="#aaaaaa",
            fg="white",
            command=lambda: controller.show_frame("BecasFrame")
        ).grid(row=8, column=0, columnspan=3, pady=10)

        # ─────────────────────────────
        # Sección 2: Procesar Datos
        # ─────────────────────────────
        self.btn_ejecutar_sql = tk.Button(
            self, text="Ejecutar SQL MATRÍCULA", bg="#107FFD", fg="white",
            command=self.execute_query
        )
        self.btn_ejecutar_sql.grid(row=2, column=0, padx=5, pady=5)
        
        self.btn_exportar_sql = tk.Button(
            self,text="Descargar MATRÍCULA Total",bg="#107FFD", fg="white",
            command=lambda: self.export_result(1))
        self.btn_exportar_sql.grid(row=2, column=1, padx=5, pady=5)


        # Botón para descargar duplicados
        self.btn_exportar_duplicados = tk.Button(
            self, text="Descargar Duplicados", bg="#107FFD", fg="white",
            command=self.export_duplicated
        )
        self.btn_exportar_duplicados.grid(row=2, column=2, padx=5, pady=5)
        
        
        self.btn_ejecutar_sql_cc = tk.Button(
            self, text="Ejecutar SQL Cambio Carrera", bg="#107FFD", fg="white",
            command=self.execute_query2
        )
        self.btn_ejecutar_sql_cc.grid(row=3, column=0, padx=5, pady=5)
        
                # Botón para descargar CAMBIO CARRERA
        self.btn_exportar_cc = tk.Button(
            self, text="Descargar Cambio Carrera", bg="#107FFD", fg="white",
            command=self.export_cc
        )
        self.btn_exportar_cc.grid(row=3, column=1, padx=5, pady=5)
        
        
        
        # Label para mostrar mensajes o resultados
        self.resultado_label = tk.Label(
            self, text="", bg="#FFFFFF", fg="#107FFD",
            font=("Arial", 12, "bold")
        )
        self.resultado_label.grid(row=4, column=0, columnspan=3, pady=5)

        # Progressbar (opcional, si deseas mostrar progreso en tareas largas)
        self.progress_bar = ttk.Progressbar(
            self, orient="horizontal", mode="indeterminate", length=200
        )
        self.progress_bar.grid(row=5, column=0, columnspan=3, pady=10)
        
        # Variable local donde guardaremos el DataFrame de duplicados
        self.df_duplicated = pd.DataFrame()
        self.df_cc = pd.DataFrame()

    # -------------------------------------------------------------------------
    # LÓGICA DE EJECUCIÓN EN SEGUNDO PLANO
    # -------------------------------------------------------------------------
    def _run_in_thread(self, button, target):
        """
        Helper para iniciar un thread, cambiar color de botón y manejar la progress bar.
        """
        self.progress_bar.start()
        button.config(bg="orange")

        def wrapper():
            try:
                target()
                button.config(bg="green")
            except Exception as e:
                messagebox.showerror("Error", f"Ha ocurrido un error:\n{e}")
                button.config(bg="#107FFD")
            finally:
                self.progress_bar.stop()

        threading.Thread(target=wrapper).start()

    # -------------------------------------------------------------------------
    # MÉTODOS RELACIONADOS A SQL (QUERY) Y RESULTADO_11
    # -------------------------------------------------------------------------

    def execute_query2(self):
        # Llama a run_query() en un thread, usando el botón actual (btn_ejecutar_sql)
        self._run_in_thread(self.btn_ejecutar_sql_cc, self.run_query2)
    def run_query2(self):
        global df_cc 
        query = """
           SELECT *  
           FROM dbo.vw_duplicados_beneficios A
        """
        # Se asume que connection2 está definido en otro módulo o globalmente
        df_cc = pd.read_sql_query(query, connection1) 
        self.df_cc = df_cc
        return df_cc   
    def execute_query(self):
        # Llama a run_query() en un thread, usando el botón actual (btn_ejecutar_sql)
        self._run_in_thread(self.btn_ejecutar_sql, self.run_query)

    def run_query(self):
        global df_resultado_11
        query = """
            SELECT
                CODIGO_PLAN,
                CASE 
                    WHEN B.SEXO = 'F' THEN 'M'
                    WHEN B.SEXO = 'M' THEN 'H'
                    ELSE 'NB'
                END AS SEXO,
                CASE WHEN B.MODALIDAD = 'PRESENCIAL' THEN 1
                     WHEN B.MODALIDAD = 'SEMIPRESENCIAL' THEN 2 
                     WHEN B.MODALIDAD = 'A DISTANCIA ON LINE' THEN 3 
                     ELSE 0 END AS MODALIDAD,
                CASE WHEN B.JORNADA = 'DIURNO' THEN 1
                     WHEN B.JORNADA = 'VESPERTINO' THEN 2
                     WHEN B.JORNADA = 'SEMIPRESENCIAL' THEN 3
                     WHEN B.JORNADA = 'A DISTANCIA' THEN 4
                     ELSE 0 END AS JOR,
                FORMAT(B.FECHA_MATRICULA, 'dd/MM/yyyy') AS FECHA_MATRICULA,
                ' ' AS REINCORPORACION, 
                B.ESTADO_ACADEMICO AS VIG,
                B.CODCLI
            FROM
                [dbo].[PR_MATRICULA] B
            WHERE
                ano >= 2024
                AND ESTADO_ACADEMICO = 'VIGENTE'
                AND TIPO_CARRERA = 'pregrado'
        """
        # Se asume que connection2 está definido en otro módulo o globalmente
        df_pr_matricula = pd.read_sql_query(query, connection2)

        query_dim_plan_academico = """
            SELECT
                LLAVE_MALLA,
                CODIGO_CARRERA_SIES AS COD_CAR,
                RIGHT(CODIGO_SIES_COMPLETO,1) AS VERSION
            FROM dbo.dim_plan_academico
        """
        df_dim_plan_academico = pd.read_sql_query(query_dim_plan_academico, connection1)

        df_joined = pd.merge(
            df_pr_matricula, df_dim_plan_academico,
            left_on="CODIGO_PLAN", right_on="LLAVE_MALLA",
            how="inner"
        )

        query_pr_sies = """
            SELECT
                CASE WHEN A.Tipo_documento = 'RUT' THEN 'R' ELSE 'P' END AS TIPO_DOC,
                A.nro_documento AS N_DOC,
                A.DV,
                A.PATERNO AS PRIMER_APELLIDO,
                A.MATERNO AS SEGUNDO_APELLIDO,
                A.NOMBRE,
                A.fecha_nacimiento AS FECH_NAC,
                A.Nacionalidad AS NAC,
                A.pais_estudios_sec AS PAIS_EST_SEC,
                A.SEDE AS COD_SED,
                A.FORMA_INGRESO AS FOR_ING_ACT,
                A.ANO_INGRESO AS ANIO_ING_ACT,
                A.SEM_INGRESO AS SEM_ING_ACT,
                ISNULL(A.ANO_ING_ORIGEN,A.ANO_INGRESO) AS ANIO_ING_ORI,
                ISNULL(A.SEM_ING_ORIGEN,A.SEM_INGRESO) AS SEM_ING_ORI,
                A.ASIG_INSCRITAS AS ASI_INS_ANT,
                A.asig_aprobadas AS ASI_APR_ANT,
                ISNULL(A.PROM_PRI_SEM,0) AS PROM_PRI_SEM,
                ISNULL(A.PROM_SEG_SEM,0) AS PROM_SEG_SEM,
                A.ASIG_INSCRITAS_HIST AS ASI_INS_HIS,
                A.asig_aprobadas_hist AS ASI_APR_HIS,
                CASE    
                    WHEN asig_aprobadas_hist < 5 THEN 1 
                    WHEN asig_aprobadas_hist < 10 THEN 2
                    WHEN asig_aprobadas_hist < 14 THEN 3
                    WHEN asig_aprobadas_hist < 18 THEN 4
                    WHEN asig_aprobadas_hist < 23 THEN 5
                    WHEN asig_aprobadas_hist < 28 THEN 6
                    WHEN asig_aprobadas_hist < 33 THEN 7
                    WHEN asig_aprobadas_hist < 38 THEN 8
                    WHEN asig_aprobadas_hist < 43 THEN 9
                    WHEN asig_aprobadas_hist >= 43 THEN 10 
                END  AS NIV_ACA,
                A.SITU_FONDO_SOLIDARIO AS SIT_FON_SOL,
                A.SUSP_PREVIAS AS SUS_PRE,
                A.CODCLI
            FROM dbo.PR_SIES A
        """
        df_pr_sies = pd.read_sql_query(query_pr_sies, connection2)

        df_final_joined = pd.merge(
            df_pr_sies, df_joined,
            on="CODCLI",
            how="inner"
        )

        df_paises = self.create_paises_dataframe()

        df_result = pd.merge(
            df_final_joined, df_paises,
            how='left',
            left_on='NAC',
            right_on='NACIONALIDAD'
        )
        df_result['NAC'] = df_result['COD_PAIS'].combine_first(df_result['NAC'])
        print(df_result)
        df_result['NAC'] = df_result['NAC'].replace('SIN INFORMACION', 38).astype(int)
        df_result['NAC'] = df_result['NAC'].infer_objects(copy=False).astype(int)
        df_result = df_result.drop(columns=['NACIONALIDAD','COD_PAIS','NOMBRE_PAIS'])

        # Se asume la función clean_text existe globalmente
        for col in ['PRIMER_APELLIDO', 'SEGUNDO_APELLIDO', 'NOMBRE']:
            df_result[col] = df_result[col].apply(clean_text)

        df_resultado_11 = df_result
        # ──────────────────────────────────────────────
        # OBTENER SOLO REGISTROS DUPLICADOS (POR N_DOC)
        # ──────────────────────────────────────────────
        # keep=False => devuelve todas las filas que tengan duplicado,
        # en vez de 1 sola. Cambia 'N_DOC' por otras columnas si lo requieres.
        df_duplicados = df_result[df_result.duplicated(subset=['N_DOC'], keep=False)]

        # Asignamos a una variable de la clase para uso posterior en export_duplicated
        self.df_duplicated = df_duplicados
        
        messagebox.showinfo("Success", "SQL Query ejecutada con éxito!")
        #self.resultado_label.config(text="Query Result Loaded")

    @staticmethod
    def create_paises_dataframe():
        """
        Retorna un DataFrame con códigos y nombres de país,
        junto con la columna 'NACIONALIDAD'. 
        """
        paises_data = [
            (0,   "SIN INFORMACION",            "Desconocida"),
            (1,   "AFGANISTÁN",                 "Afgana"),
            (2,   "ALBANIA",                    "Albanesa"),
            (3,   "ALEMANIA",                   "Alemana"),
            (4,   "ANDORRA",                    "Andorrana"),
            (5,   "ANGOLA",                     "Angoleña"),
            (6,   "ANTIGUA Y BARBUDA",          "Antiguana"),
            (7,   "ARABIA SAUDITA",             "Saudí"),
            (8,   "ARGELIA",                    "Argelina"),
            (9,   "ARGENTINA",                  "Argentina"),
            (10,  "ARMENIA",                    "Armenia"),
            (11,  "AUSTRALIA",                  "Australiana"),
            (12,  "AUSTRIA",                    "Austríaca"),
            (13,  "AZERBAIYÁN",                 "Azerbaiyana"),
            (14,  "BAHAMAS",                    "Bahameña"),
            (15,  "BANGLADÉS",                  "Bangladesí"),
            (16,  "BARBADOS",                   "Barbadense"),
            (17,  "BARÉIN",                     "Bareiní"),
            (18,  "BÉLGICA",                    "Belga"),
            (19,  "BELICE",                     "Beliceña"),
            (20,  "BENÍN",                      "Beninesa"),
            (21,  "BIELORRUSIA",                "Bielorrusa"),
            (22,  "BIRMANIA",                   "Birmana"),
            (23,  "BOLIVIA",                    "Boliviana"),
            (24,  "BOSNIA- HERZEGOVINA",        "Bosnia"),
            (25,  "BOTSUANA",                   "Botsuanesa"),
            (26,  "BRASIL",                     "Brasileña"),
            (27,  "BRUNÉI",                     "Bruneana"),
            (28,  "BULGARIA",                   "Búlgara"),
            (29,  "BURKINA FASO",               "Burkinesa"),
            (30,  "BURUNDI",                    "Burundesa"),
            (31,  "BUTÁN",                      "Butanesa"),
            (32,  "CABO VERDE",                 "Caboverdiana"),
            (33,  "CAMBOYA",                    "Camboyana"),
            (34,  "CAMERÚN",                    "Camerunesa"),
            (35,  "CANADÁ",                     "Canadiense"),
            (36,  "CATAR",                      "Catarí"),
            (37,  "CHAD",                       "Chadiana"),
            (38,  "CHILE",                      "Chilena"),  # Reutilizamos 38 para "CHILE"
            (39,  "CHINA",                      "China"),
            (40,  "CHIPRE",                     "Chipriota"),
            (41,  "COLOMBIA",                   "Colombiana"),
            (42,  "COMORAS",                    "Comorense"),
            (43,  "CONGO",                      "Congoleña"),
            (44,  "COREA DEL NORTE",            "Koreana"),
            (45,  "COREA DEL SUR",              "norcoreana"),
            (46,  "COSTA DE MARFIL",            "Marfileña"),
            (47,  "COSTA RICA",                 "Costarricense"),
            (48,  "CROACIA",                    "Croata"),
            (49,  "CUBA",                       "Cubana"),
            (50,  "DINAMARCA",                  "Danesa"),
            (51,  "DOMINICA",                   "Dominiquesa"),
            (52,  "ECUADOR",                    "Ecuatoriana"),
            (53,  "EGIPTO",                     "Egipcia"),
            (54,  "EL SALVADOR",                "Salvadoreña"),
            (55,  "EMIRATOS ÁRABES UNIDOS",     "Emiratí"),
            (56,  "ERITREA",                    "Eritrea"),  
            (57,  "ESLOVAQUIA",                 "Eslovaca"),
            (58,  "ESLOVENIA",                  "Eslovena"),
            (59,  "ESPAÑA",                     "Española"),
            (60,  "ESTADOS UNIDOS",             "Estadounidense"),
            (61,  "ESTONIA",                    "Estonia"),
            (62,  "ETIOPÍA",                    "Etíope"),
            (63,  "FILIPINAS",                  "Filipina"),
            (64,  "FINLANDIA",                  "Finlandesa"),
            (65,  "FIYI",                       "Fiyiana"),
            (66,  "FRANCIA",                    "Francesa"),
            (67,  "GABÓN",                      "Gabonense"),
            (68,  "GAMBIA",                     "Gambiana"),
            (69,  "GEORGIA",                    "Georgiana"),
            (70,  "GHANA",                      "Ghanesa"),
            (71,  "GRANADA",                    "Granadina"),
            (72,  "GRECIA",                     "Griega"),
            (73,  "GUATEMALA",                  "Guatemalteca"),
            (74,  "GUINEA",                     "Guineana"),
            (75,  "GUINEA ECUATORIAL",          "Ecuatoguineana"),
            (76,  "GUINEA-BISÁU",               "Guineana"),
            (77,  "GUYANA",                     "Guyanesa"),
            (78,  "HAITÍ",                      "Haitiana"),
            (79,  "HONDURAS",                   "Hondureña"),
            (80,  "HUNGRÍA",                    "Húngara"),
            (81,  "INDIA",                      "India"),
            (82,  "INDONESIA",                  "Indonesia"),
            (83,  "IRAK",                       "Iraquí"),
            (84,  "IRÁN",                       "Iraní"),
            (85,  "IRLANDA",                    "Irlandesa"),
            (86,  "ISLANDIA",                   "Islandesa"),
            (87,  "ISLAS MARSHALL",             "Marshallesa"),
            (88,  "ISLAS SALOMÓN",              "Salomonense"),
            (89,  "ISRAEL",                     "Israelí"),
            (90,  "ITALIA",                     "Italiana"),
            (91,  "JAMAICA",                    "Jamaicana"),
            (92,  "JAPÓN",                      "Japonesa"),
            (93,  "JORDANIA",                   "Jordana"),
            (94,  "KAZAJISTÁN",                 "Kazaja"),
            (95,  "KENIA",                      "Keniana"),
            (96,  "KIRGUISTÁN",                 "Kirguisa"),
            (97,  "KIRIBATI",                   "Kiribatiana"),
            (98,  "KOSOVO",                     "Kosovar"),
            (99,  "KUWAIT",                     "Kuwaití"),
            (100, "LAOS",                       "Laosiana"),
            (101, "LESOTO",                     "Lesotense"),
            (102, "LETONIA",                    "Letona"),
            (103, "LÍBANO",                     "Libanesa"),
            (104, "LIBERIA",                    "Liberiana"),
            (105, "LIBIA",                      "Libia"),
            (106, "LIECHTENSTEIN",              "Liechtensteiniana"),
            (107, "LITUANIA",                   "Lituana"),
            (108, "LUXEMBURGO",                 "Luxemburguesa"),
            (109, "MACEDONIA",                  "Macedonia"),
            (110, "MADAGASCAR",                 "Malgache"),
            (111, "MALASIA",                    "Malasia"),
            (112, "MALAUI",                     "Malauí"),
            (113, "MALDIVAS",                   "Maldiva"),
            (114, "MALÍ",                       "Maliense"),
            (115, "MALTA",                      "Maltesa"),
            (116, "MARRUECOS",                  "Marroquí"),
            (117, "MAURICIO",                   "Mauriciana"),
            (118, "MAURITANIA",                 "Mauritana"),
            (119, "MÉXICO",                     "Mexicana"),
            (120, "MICRONESIA",                 "Micronesia"),
            (121, "MOLDAVIA",                   "Moldava"),
            (122, "MÓNACO",                     "Monegasca"),
            (123, "MONGOLIA",                   "Mongola"),
            (124, "MONTENEGRO",                 "Montenegrina"),
            (125, "MOZAMBIQUE",                 "Mozambiqueña"),
            (126, "NAMIBIA",                    "Namibia"),
            (127, "NAURU",                      "Nauruana"),
            (128, "NEPAL",                      "Nepalí"),
            (129, "NICARAGUA",                  "Nicaragüense"),
            (130, "NÍGER",                      "Nigerina"),
            (131, "NIGERIA",                    "Nigeriana"),
            (132, "NORUEGA",                    "Noruega"),
            (133, "NUEVA ZELANDA",              "Neozelandesa"),
            (134, "OMÁN",                       "Omaní"),
            (135, "PAÍSES BAJOS",               "Neerlandesa"),
            (136, "PAKISTÁN",                   "Paquistaní"),
            (137, "PALAOS",                     "Palauana"),
            (138, "PALESTINA",                  "Palestina"),
            (139, "PANAMÁ",                     "Panameña"),
            (140, "PAPÚA NUEVA GUINEA",         "Papú"),
            (141, "PARAGUAY",                   "Paraguaya"),
            (142, "PERÚ",                       "Peruana"),
            (143, "POLONIA",                    "Polaca"),
            (144, "PORTUGAL",                   "Portuguesa"),
            (145, "REINO UNIDO",                "Británica"),
            (146, "REPÚBLICA CENTROAFRICANA",   "Centroafricana"),
            (147, "REPÚBLICA CHECA",            "Checa"),
            (148, "REPÚBLICA DEMOCRÁTICA DEL CONGO", "Congoleña"),
            (149, "REPÚBLICA DOMINICANA",       "Dominicana"),
            (150, "RUANDA",                     "Ruandesa"),
            (151, "RUMANIA",                    "Rumana"),
            (152, "RUSIA",                      "Rusa"),
            (153, "SAMOA",                      "Samoana"),
            (154, "SAN CRISTÓBAL Y NIEVES",     "Sancristobaleña"),
            (155, "SAN MARINO",                 "Sanmarinense"),
            (156, "SAN VICENTE Y LAS GRANADINAS","Sanvicentina"),
            (157, "SANTA LUCÍA",                "Santalucense"),
            (158, "SANTO TOMÉ Y PRÍNCIPE",      "Saotomeña"),
            (159, "SENEGAL",                    "Senegalesa"),
            (160, "SERBIA",                     "Serbia"),
            (161, "SEYCHELLES",                 "Seychellense"),
            (162, "SIERRA LEONA",               "Sierraleonesa"),
            (163, "SINGAPUR",                   "Singapurense"),
            (164, "SIRIA",                      "Siria"),
            (165, "SOMALIA",                    "Somalí"),
            (166, "SRI LANKA",                  "Esrilanquesa"),
            (167, "SUAZILANDIA",                "Suazi"),
            (168, "SUDÁFRICA",                  "Sudafricana"),
            (169, "SUDÁN",                      "Sudanesa"),
            (170, "SUDÁN DEL SUR",              "Sursudanesa"),
            (171, "SUECIA",                     "Sueca"),
            (172, "SUIZA",                      "Suiza"),
            (173, "SURINAM",                    "Surinamesa"),
            (174, "TAILANDIA",                  "Tailandesa"),
            (175, "TAIWÁN",                     "Taiwanesa"),
            (176, "TANZANIA",                   "Tanzana"),
            (177, "TAYIKISTÁN",                 "Tayika"),
            (178, "TIMOR ORIENTAL",             "Timorense"),
            (179, "TOGO",                       "Togolesa"),
            (180, "TONGA",                      "Tongana"),
            (181, "TRINIDAD Y TOBAGO",          "Trinitense"),
            (182, "TÚNEZ",                      "Tunecina"),
            (183, "TURKMENISTÁN",               "Turkmena"),
            (184, "TURQUÍA",                    "Turca"),
            (185, "TUVALU",                     "Tuvaluana"),
            (186, "UCRANIA",                    "Ucraniana"),
            (187, "UGANDA",                     "Ugandesa"),
            (188, "URUGUAY",                    "Uruguaya"),
            (189, "UZBEKISTÁN",                 "Uzbeka"),
            (190, "VANUATU",                    "Vanuatuense"),
            (191, "VATICANO",                   "Vaticana"),
            (192, "VENEZUELA",                  "Venezolana"),
            (193, "VIETNAM",                    "Vietnamita"),
            (194, "YEMEN",                      "Yemení"),
            (195, "YIBUTI",                     "Yibutiana"),
            (196, "ZAMBIA",                     "Zambiana"),
            (197, "ZIMBABUE",                   "Zimbabuense")
        ]
        df_paises = pd.DataFrame(paises_data, columns=["COD_PAIS", "NOMBRE_PAIS", "NACIONALIDAD"])
        return df_paises

    def create_paises_dataframe2(self):
        """
        Ejemplo de método que consulta otra tabla en tu BD.
        No se está usando en el código actual.
        """
        query = """
            SELECT
               codigo as COD_PAIS,
               nombre as NOMBRE_PAIS,
               nacionalidad as NACIONALIDAD
            FROM
                dbo.dim_territorio_sies
        """
        df_paises = pd.read_sql_query(query, connection1)
        # Aquí podrías retornar el df o hacer merges con él.
        return df_paises
 # -------------------------------------------------------------------------
    # EXPORTACIÓN DE RESULTADOS (Matrícula Completa o Parcial)
    # -------------------------------------------------------------------------
    def export_result(self, result_number):
        """
        Exporta el DataFrame df_resultado_11 (u otros), troceado si excede 1.040.000 filas.
        """
        global df_resultado_11, df_resultado_3_non_nan, df_resultado_4_non_nan, df_preseleccion_updated
        df_to_export = None

        if result_number == 1:
            df_to_export = df_resultado_11
        elif result_number == 2:
            df_to_export = df_resultado_3_non_nan
        elif result_number == 3:
            df_to_export = df_resultado_4_non_nan
        elif result_number == 4:
            df_to_export = df_preseleccion_updated
        else:
            messagebox.showerror("Error", "Invalid result number.")
            return

        if df_to_export is None or df_to_export.empty:
            messagebox.showwarning("No Data", f"No data for Result {result_number}.")
            return

        self._export_in_chunks(df_to_export, f"Result_{result_number}")

    def export_duplicated(self):
        """
        Exporta el DataFrame de duplicados (self.df_duplicated),
        troceado si excede 1.040.000 filas.
        """
        if self.df_duplicated is None or self.df_duplicated.empty:
            messagebox.showwarning("Sin duplicados", "No hay registros duplicados para exportar.")
            return

        self._export_in_chunks(self.df_duplicated, "Duplicados")

    def export_cc(self):
        """
        Exporta el DataFrame de duplicados (self.df_duplicated),
        troceado si excede 1.040.000 filas.
        """
        if self.df_cc is None or self.df_cc.empty:
            messagebox.showwarning("Sin datos", "No hay registros para exportar.")
            return

        self._export_in_chunks(self.df_cc, "Cambio Carrera")
        
    def _export_in_chunks(self, df_to_export, default_name):
        """
        Lógica compartida para exportar a Excel en uno o más archivos
        (si excede 1.040.000 filas).
        """
        limit_excel_rows = 1040000
        num_rows = df_to_export.shape[0]

        if num_rows <= limit_excel_rows:
            # Caso: Exportar todo en un solo archivo Excel
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel Files", "*.xlsx")],
                title=f"Guardar {default_name}"
            )
            if not file_path:
                return  # Usuario canceló
            try:
                df_to_export.to_excel(file_path, index=False)
                messagebox.showinfo("Success", f"{default_name} guardado en:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"Error al guardar Excel.\n{e}")
        else:
            # Se trocea en varias partes (2 x 500.000 + resto)
            messagebox.showinfo(
                "Advertencia tamaño de archivo",
                (
                    f"El DataFrame tiene {num_rows} filas, "
                    "más de 1.040.000. Se crearán varios archivos Excel:\n"
                    " - 2 archivos de 500.000 filas\n"
                    " - 1 archivo con las filas restantes"
                )
            )
            base_file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel Files", "*.xlsx")],
                title=f"Guardar {default_name} - se crearán varios archivos"
            )
            if not base_file_path:
                return  # Usuario canceló

            base, ext = os.path.splitext(base_file_path)
            if not ext:
                ext = ".xlsx"

            df_part1 = df_to_export.iloc[:500000, :]
            df_part2 = df_to_export.iloc[500000:1000000, :]
            df_part3 = df_to_export.iloc[1000000:, :]

            part1_path = f"{base}_part1{ext}"
            part2_path = f"{base}_part2{ext}"
            part3_path = f"{base}_part3{ext}"

            try:
                df_part1.to_excel(part1_path, index=False)
                df_part2.to_excel(part2_path, index=False)

                if not df_part3.empty:
                    df_part3.to_excel(part3_path, index=False)
                    messagebox.showinfo(
                        "Success",
                        f"Archivos creados:\n\n"
                        f"- {part1_path}\n- {part2_path}\n- {part3_path}"
                    )
                else:
                    messagebox.showinfo(
                        "Success",
                        f"Archivos creados:\n\n"
                        f"- {part1_path}\n- {part2_path}\n\n"
                        "Part3 vacío, no se generó."
                    )
            except Exception as e:
                messagebox.showerror("Error", f"No se pudieron guardar todas las partes.\n{e}")

#=====================================================
# --------------- BECASRENOVANTESFRAME ---------------
#=====================================================


class BecasRenovantesFrame(tk.Frame):
    """
    Contiene toda la lógica y botones de 'Renovantes' dentro de Becas,
    con carga de archivos, ejecución de queries, merges, etc.
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        # Título principal
        tk.Label(
            self,
            text="Renovantes (Becas)",
            bg="#FFFFFF",
            fg="#107FFD",
            font=("Arial", 20, "bold")
        ).pack(pady=5)

        # Se construyen secciones (frames) de manera ordenada
        self._create_upload_section().pack(pady=10)
        self._create_process_section().pack(pady=10)
        self._create_download_section().pack(pady=10)

        # Label de resultado
        self.resultado_label = tk.Label(
            self, text="", bg="#FFFFFF",
            font=("Arial", 14, "bold"), fg="#107FFD"
        )
        self.resultado_label.pack(pady=5)

        # Botón "Volver"
        tk.Button(
            self,
            text="Volver",
            bg="#aaaaaa",
            fg="white",
            command=lambda: controller.show_frame("BecasFrame")
        ).pack(pady=10)

    # -------------------------------------------------------------------------
    # SECCIONES DE LA UI
    # -------------------------------------------------------------------------
    def _create_upload_section(self):
        frame_upload = tk.Frame(self, bg="#FFFFFF")

        # Título Upload
        upload_label = tk.Label(
            frame_upload,
            text="Subir Archivos CSV (BecasRenovantes)",
            bg="#FFFFFF",
            font=("Arial", 14, "bold"),
            fg="#107FFD"
        )
        upload_label.grid(row=0, column=0, columnspan=2, pady=5)

        # Glosas
        self.label_glosas = tk.Label(frame_upload, text="No file loaded for Glosas", bg="#FFFFFF")
        self.label_glosas.grid(row=1, column=0, sticky="w")

        btn_load_glosas = tk.Button(
            frame_upload, text="Load Glosas File",
            command=lambda: self.load_file(self.label_glosas, 'GLOSAS'),
            relief="flat", bg="#107FFD", fg="white", font=("Arial", 10, "bold")
        )
        btn_load_glosas.grid(row=1, column=1, padx=5, pady=5)

        # Renovantes
        self.label_renovantes = tk.Label(frame_upload, text="No file loaded for Renovantes", bg="#FFFFFF")
        self.label_renovantes.grid(row=2, column=0, sticky="w")

        btn_load_renovantes = tk.Button(
            frame_upload, text="Load Renovantes File",
            command=lambda: self.load_file(self.label_renovantes, 'RENOVANTES'),
            relief="flat", bg="#107FFD", fg="white", font=("Arial", 10, "bold")
        )
        btn_load_renovantes.grid(row=2, column=1, padx=5, pady=5)



        # Potenciales Renovantes
        self.label_potenciales_renovantes = tk.Label(frame_upload, text="No file loaded for Potenciales Renovantes", bg="#FFFFFF")
        self.label_potenciales_renovantes.grid(row=3, column=0, sticky="w")

        btn_load_potenciales_renovantes = tk.Button(
            frame_upload, text="Load Potenciales Renovantes File",
            command=lambda: self.load_file(self.label_potenciales_renovantes, 'POTENCIALES_RENOVANTES'),
            relief="flat", bg="#107FFD", fg="white", font=("Arial", 10, "bold")
        )
        btn_load_potenciales_renovantes.grid(row=3, column=1, padx=5, pady=5)
        
        # Preseleccion
        self.label_preseleccion = tk.Label(frame_upload, text="No file loaded for Preseleccion", bg="#FFFFFF")
        self.label_preseleccion.grid(row=4, column=0, sticky="w")

        btn_load_preseleccion = tk.Button(
            frame_upload, text="Load Preseleccion File",
            command=lambda: self.load_file(self.label_preseleccion, 'PRESELECCION'),
            relief="flat", bg="#107FFD", fg="white", font=("Arial", 10, "bold")
        )
        btn_load_preseleccion.grid(row=4, column=1, padx=5, pady=5)

        return frame_upload

    def _create_process_section(self):
        frame_process = tk.Frame(self, bg="#FFFFFF")

        process_label = tk.Label(
            frame_process,
            text="Procesar Datos",
            bg="#FFFFFF",
            font=("Arial", 14, "bold"),
            fg="#107FFD"
        )
        process_label.grid(row=0, column=0, columnspan=2, pady=5)

        self.button_execute_query = tk.Button(
            frame_process,
            text="Ejecutar SQL MATRÍCULA",
            command=self.execute_query,
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        )
        self.button_execute_query.grid(row=1, column=0, padx=5, pady=5)

        self.button_generate_result_2 = tk.Button(
            frame_process,
            text="Generar PRESELECCIÓN",
            command=self.generate_result_2,
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        )
        self.button_generate_result_2.grid(row=1, column=1, padx=5, pady=5)

        self.button_generate_result_3 = tk.Button(
            frame_process,
            text="Generar RENOVANTES",
            command=self.generate_result_3,
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        )
        self.button_generate_result_3.grid(row=2, column=0, padx=5, pady=5)

        self.button_generate_result_4 = tk.Button(
            frame_process,
            text="Generar POTENCIALES RENOVANTES",
            command=self.generate_result_4,
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        )
        self.button_generate_result_4.grid(row=2, column=1, padx=5, pady=5)

        self.progress_bar = ttk.Progressbar(
            frame_process,
            orient="horizontal",
            mode="indeterminate",
            length=200
        )
        self.progress_bar.grid(row=3, column=0, columnspan=2, pady=10)

        return frame_process

    def _create_download_section(self):
        frame_download = tk.Frame(self, bg="#FFFFFF")

        download_label = tk.Label(
            frame_download,
            text="Descargar Resultados",
            bg="#FFFFFF",
            font=("Arial", 14, "bold"),
            fg="#107FFD"
        )
        download_label.grid(row=0, column=0, columnspan=2, pady=5)

        tk.Button(
            frame_download,
            text="Download MATRÍCULA",
            command=lambda: self.export_result(1),
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        ).grid(row=1, column=0, padx=5, pady=5)

        tk.Button(
            frame_download,
            text="Download PRESELECCIÓN",
            command=lambda: self.export_result(2),
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        ).grid(row=1, column=1, padx=5, pady=5)

        tk.Button(
            frame_download,
            text="Download RENOVANTES",
            command=lambda: self.export_result(3),
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        ).grid(row=2, column=0, padx=5, pady=5)

        tk.Button(
            frame_download,
            text="Download POTENCIALES RENOVANTES",
            command=lambda: self.export_result(4),
            relief="flat",
            bg="#107FFD",
            fg="white",
            font=("Arial", 12, "bold")
        ).grid(row=2, column=1, padx=5, pady=5)

        return frame_download

    # -------------------------------------------------------------------------
    # LÓGICA DE CARGA DE ARCHIVOS
    # -------------------------------------------------------------------------
    def load_file(self, label, file_type):
        global df_glosas, df_renovantes, df_preseleccion, df_pot_renovantes
        df, file_path = read_any_file(title=f"Select {file_type} file")
        if df is not None and file_path is not None:
            label.config(text=f"Loaded file: {Path(file_path).name}")
            messagebox.showinfo("Success", f"{file_type} file loaded successfully!")
            if file_type == 'GLOSAS':
                df_glosas = df
            elif file_type == 'RENOVANTES':
                df_renovantes = df
            elif file_type == 'PRESELECCION':
                df_preseleccion = df
            elif file_type == 'POTENCIALES_RENOVANTES':
                df_pot_renovantes = df
        else:
            messagebox.showwarning("No File Selected", f"Please select a {file_type} file.")

    # -------------------------------------------------------------------------
    # LÓGICA DE EJECUCIÓN EN SEGUNDO PLANO
    # -------------------------------------------------------------------------
    def _run_in_thread(self, button, target):
        """
        Helper para iniciar un thread, cambiar color de botón y manejar la progress bar.
        """
        self.progress_bar.start()
        button.config(bg="orange")

        def wrapper():
            try:
                target()
                button.config(bg="green")
            except Exception as e:
                messagebox.showerror("Error", f"Ha ocurrido un error:\n{e}")
                button.config(bg="#107FFD")
            finally:
                self.progress_bar.stop()

        threading.Thread(target=wrapper).start()

    # -------------------------------------------------------------------------
    # MÉTODOS RELACIONADOS A SQL (QUERY) Y RESULTADO_11
    # -------------------------------------------------------------------------
    def execute_query(self):
        # Llama a run_query() en un thread
        self._run_in_thread(self.button_execute_query, self.run_query)

    def run_query(self):
        global df_resultado_11
        global df_cc 
        query = """
            SELECT
                CODIGO_PLAN,
                CASE 
                    WHEN B.SEXO = 'F' THEN 'M'
                    WHEN B.SEXO = 'M' THEN 'H'
                    ELSE 'NB'
                END AS SEXO,
                CASE WHEN B.MODALIDAD = 'PRESENCIAL' THEN 1
                     WHEN B.MODALIDAD = 'SEMIPRESENCIAL' THEN 2 
                     WHEN B.MODALIDAD = 'A DISTANCIA ON LINE' THEN 3 
                     ELSE 0 END AS MODALIDAD,
                CASE WHEN B.JORNADA = 'DIURNO' THEN 1
                     WHEN B.JORNADA = 'VESPERTINO' THEN 2
                     WHEN B.JORNADA = 'SEMIPRESENCIAL' THEN 3
                     WHEN B.JORNADA = 'A DISTANCIA' THEN 4
                     ELSE 0 END AS JOR,
                FORMAT(B.FECHA_MATRICULA, 'dd/MM/yyyy') AS FECHA_MATRICULA,
                ' ' AS REINCORPORACION, 
                B.ESTADO_ACADEMICO AS VIG,
                B.CODCLI
            FROM
                [dbo].[PR_MATRICULA] B
            WHERE
                ano >= 2024
                AND ESTADO_ACADEMICO = 'VIGENTE'
                AND TIPO_CARRERA = 'pregrado'
        """
        df_pr_matricula = pd.read_sql_query(query, connection2)

        query_dim_plan_academico = """
            SELECT
                LLAVE_MALLA,
                CODIGO_CARRERA_SIES AS COD_CAR,
                RIGHT(CODIGO_SIES_COMPLETO,1) AS VERSION
            FROM dbo.dim_plan_academico
        """
        df_dim_plan_academico = pd.read_sql_query(query_dim_plan_academico, connection1)

        df_joined = pd.merge(
            df_pr_matricula, df_dim_plan_academico,
            left_on="CODIGO_PLAN", right_on="LLAVE_MALLA",
            how="inner"
        )

        query_pr_sies = """
            SELECT
                CASE WHEN A.Tipo_documento = 'RUT' THEN 'R' ELSE 'P' END AS TIPO_DOC,
                A.nro_documento AS N_DOC,
                A.DV,
                A.PATERNO AS PRIMER_APELLIDO,
                A.MATERNO AS SEGUNDO_APELLIDO,
                A.NOMBRE,
                A.fecha_nacimiento AS FECH_NAC,
                A.Nacionalidad AS NAC,
                A.pais_estudios_sec AS PAIS_EST_SEC,
                A.SEDE AS COD_SED,
                A.FORMA_INGRESO AS FOR_ING_ACT,
                A.ANO_INGRESO AS ANIO_ING_ACT,
                A.SEM_INGRESO AS SEM_ING_ACT,
                ISNULL(A.ANO_ING_ORIGEN,A.ANO_INGRESO) AS ANIO_ING_ORI,
                ISNULL(A.SEM_ING_ORIGEN,A.SEM_INGRESO) AS SEM_ING_ORI,
                A.ASIG_INSCRITAS AS ASI_INS_ANT,
                A.asig_aprobadas AS ASI_APR_ANT,
                ISNULL(A.PROM_PRI_SEM,0) AS PROM_PRI_SEM,
                ISNULL(A.PROM_SEG_SEM,0) AS PROM_SEG_SEM,
                A.ASIG_INSCRITAS_HIST AS ASI_INS_HIS,
                A.asig_aprobadas_hist AS ASI_APR_HIS,
                CASE    
                    WHEN asig_aprobadas_hist < 5 THEN 1 
                    WHEN asig_aprobadas_hist < 10 THEN 2
                    WHEN asig_aprobadas_hist < 14 THEN 3
                    WHEN asig_aprobadas_hist < 18 THEN 4
                    WHEN asig_aprobadas_hist < 23 THEN 5
                    WHEN asig_aprobadas_hist < 28 THEN 6
                    WHEN asig_aprobadas_hist < 33 THEN 7
                    WHEN asig_aprobadas_hist < 38 THEN 8
                    WHEN asig_aprobadas_hist < 43 THEN 9
                    WHEN asig_aprobadas_hist >= 43 THEN 10 
                END  AS NIV_ACA,
                A.SITU_FONDO_SOLIDARIO AS SIT_FON_SOL,
                A.SUSP_PREVIAS AS SUS_PRE,
                A.CODCLI
            FROM dbo.PR_SIES A
        """
        df_pr_sies = pd.read_sql_query(query_pr_sies, connection2)

        df_final_joined = pd.merge(
            df_pr_sies, df_joined,
            on="CODCLI",
            how="inner"
        )

        df_paises = self.create_paises_dataframe()

        df_result = pd.merge(
            df_final_joined, df_paises,
            how='left',
            left_on='NAC',
            right_on='NACIONALIDAD'
        )
        df_result['NAC'] = df_result['COD_PAIS'].combine_first(df_result['NAC'])
        df_result['NAC'] = df_result['NAC'].replace('SIN INFORMACION', 38)
        df_result['NAC'] = df_result['NAC'].infer_objects(copy=False).astype(int)
        #df_result['NAC'] = df_result['NAC'].replace('SIN INFORMACION', 38).astype(int)
        df_result = df_result.drop(columns=['NACIONALIDAD','COD_PAIS','NOMBRE_PAIS'])

        for col in ['PRIMER_APELLIDO', 'SEGUNDO_APELLIDO', 'NOMBRE']:
            df_result[col] = df_result[col].apply(clean_text)
        
        df_resultado_11 = df_result
        df_duplicados = df_result[df_result.duplicated(subset=['N_DOC'], keep=False)]
        df_resultado_11 = df_resultado_11[~df_resultado_11['N_DOC'].isin(df_duplicados['N_DOC'].unique())].copy()
        
        if df_cc is None: 
            messagebox.showinfo("Debes ejecutar el proceso de Validaciones Previas")
        df_cc['RUT'] = df_cc['RUT'].astype(str).str.strip()
        df_resultado_11 = df_resultado_11[~df_resultado_11['N_DOC'].isin(df_cc['RUT'].unique())].copy() 
        messagebox.showinfo("Success", "SQL Query ejecutada con éxito!")
        self.resultado_label.config(text="Query Result Loaded")

    @staticmethod
    def create_paises_dataframe():
    # Tabla unificada: cada fila es (COD_PAIS, NOMBRE_PAIS, NACIONALIDAD)
        paises_data = [
            (0,   "SIN INFORMACION",            "Desconocida"),
            (1,   "AFGANISTÁN",                 "Afgana"),
            (2,   "ALBANIA",                    "Albanesa"),
            (3,   "ALEMANIA",                   "Alemana"),
            (4,   "ANDORRA",                    "Andorrana"),
            (5,   "ANGOLA",                     "Angoleña"),
            (6,   "ANTIGUA Y BARBUDA",          "Antiguana"),
            (7,   "ARABIA SAUDITA",             "Saudí"),
            (8,   "ARGELIA",                    "Argelina"),
            (9,   "ARGENTINA",                  "Argentina"),
            (10,  "ARMENIA",                    "Armenia"),
            (11,  "AUSTRALIA",                  "Australiana"),
            (12,  "AUSTRIA",                    "Austríaca"),
            (13,  "AZERBAIYÁN",                 "Azerbaiyana"),
            (14,  "BAHAMAS",                    "Bahameña"),
            (15,  "BANGLADÉS",                  "Bangladesí"),
            (16,  "BARBADOS",                   "Barbadense"),
            (17,  "BARÉIN",                     "Bareiní"),
            (18,  "BÉLGICA",                    "Belga"),
            (19,  "BELICE",                     "Beliceña"),
            (20,  "BENÍN",                      "Beninesa"),
            (21,  "BIELORRUSIA",                "Bielorrusa"),
            (22,  "BIRMANIA",                   "Birmana"),
            (23,  "BOLIVIA",                    "Boliviana"),
            (24,  "BOSNIA- HERZEGOVINA",        "Bosnia"),
            (25,  "BOTSUANA",                   "Botsuanesa"),
            (26,  "BRASIL",                     "Brasileña"),
            (27,  "BRUNÉI",                     "Bruneana"),
            (28,  "BULGARIA",                   "Búlgara"),
            (29,  "BURKINA FASO",               "Burkinesa"),
            (30,  "BURUNDI",                    "Burundesa"),
            (31,  "BUTÁN",                      "Butanesa"),
            (32,  "CABO VERDE",                 "Caboverdiana"),
            (33,  "CAMBOYA",                    "Camboyana"),
            (34,  "CAMERÚN",                    "Camerunesa"),
            (35,  "CANADÁ",                     "Canadiense"),
            (36,  "CATAR",                      "Catarí"),
            (37,  "CHAD",                       "Chadiana"),
            (38,  "CHILE",                      "Chilena"),  # Reutilizamos 38 para "CHILE"
            (39,  "CHINA",                      "China"),
            (40,  "CHIPRE",                     "Chipriota"),
            (41,  "COLOMBIA",                   "Colombiana"),
            (42,  "COMORAS",                    "Comorense"),
            (43,  "CONGO",                      "Congoleña"),
            (44,  "COREA DEL NORTE",            "Koreana"),
            (45,  "COREA DEL SUR",              "norcoreana"),
            (46,  "COSTA DE MARFIL",            "Marfileña"),
            (47,  "COSTA RICA",                 "Costarricense"),
            (48,  "CROACIA",                    "Croata"),
            (49,  "CUBA",                       "Cubana"),
            (50,  "DINAMARCA",                  "Danesa"),
            (51,  "DOMINICA",                   "Dominiquesa"),
            (52,  "ECUADOR",                    "Ecuatoriana"),
            (53,  "EGIPTO",                     "Egipcia"),
            (54,  "EL SALVADOR",                "Salvadoreña"),
            (55,  "EMIRATOS ÁRABES UNIDOS",     "Emiratí"),
            (56,  "ERITREA",                    "Eritrea"),  
            (57,  "ESLOVAQUIA",                 "Eslovaca"),
            (58,  "ESLOVENIA",                  "Eslovena"),
            (59,  "ESPAÑA",                     "Española"),
            (60,  "ESTADOS UNIDOS",             "Estadounidense"),
            (61,  "ESTONIA",                    "Estonia"),
            (62,  "ETIOPÍA",                    "Etíope"),
            (63,  "FILIPINAS",                  "Filipina"),
            (64,  "FINLANDIA",                  "Finlandesa"),
            (65,  "FIYI",                       "Fiyiana"),
            (66,  "FRANCIA",                    "Francesa"),
            (67,  "GABÓN",                      "Gabonense"),
            (68,  "GAMBIA",                     "Gambiana"),
            (69,  "GEORGIA",                    "Georgiana"),
            (70,  "GHANA",                      "Ghanesa"),
            (71,  "GRANADA",                    "Granadina"),
            (72,  "GRECIA",                     "Griega"),
            (73,  "GUATEMALA",                  "Guatemalteca"),
            (74,  "GUINEA",                     "Guineana"),
            (75,  "GUINEA ECUATORIAL",          "Ecuatoguineana"),
            (76,  "GUINEA-BISÁU",               "Guineana"),
            (77,  "GUYANA",                     "Guyanesa"),
            (78,  "HAITÍ",                      "Haitiana"),
            (79,  "HONDURAS",                   "Hondureña"),
            (80,  "HUNGRÍA",                    "Húngara"),
            (81,  "INDIA",                      "India"),
            (82,  "INDONESIA",                  "Indonesia"),
            (83,  "IRAK",                       "Iraquí"),
            (84,  "IRÁN",                       "Iraní"),
            (85,  "IRLANDA",                    "Irlandesa"),
            (86,  "ISLANDIA",                   "Islandesa"),
            (87,  "ISLAS MARSHALL",             "Marshallesa"),
            (88,  "ISLAS SALOMÓN",              "Salomonense"),
            (89,  "ISRAEL",                     "Israelí"),
            (90,  "ITALIA",                     "Italiana"),
            (91,  "JAMAICA",                    "Jamaicana"),
            (92,  "JAPÓN",                      "Japonesa"),
            (93,  "JORDANIA",                   "Jordana"),
            (94,  "KAZAJISTÁN",                 "Kazaja"),
            (95,  "KENIA",                      "Keniana"),
            (96,  "KIRGUISTÁN",                 "Kirguisa"),
            (97,  "KIRIBATI",                   "Kiribatiana"),
            (98,  "KOSOVO",                     "Kosovar"),
            (99,  "KUWAIT",                     "Kuwaití"),
            (100, "LAOS",                       "Laosiana"),
            (101, "LESOTO",                     "Lesotense"),
            (102, "LETONIA",                    "Letona"),
            (103, "LÍBANO",                     "Libanesa"),
            (104, "LIBERIA",                    "Liberiana"),
            (105, "LIBIA",                      "Libia"),
            (106, "LIECHTENSTEIN",              "Liechtensteiniana"),
            (107, "LITUANIA",                   "Lituana"),
            (108, "LUXEMBURGO",                 "Luxemburguesa"),
            (109, "MACEDONIA",                  "Macedonia"),
            (110, "MADAGASCAR",                 "Malgache"),
            (111, "MALASIA",                    "Malasia"),
            (112, "MALAUI",                     "Malauí"),
            (113, "MALDIVAS",                   "Maldiva"),
            (114, "MALÍ",                       "Maliense"),
            (115, "MALTA",                      "Maltesa"),
            (116, "MARRUECOS",                  "Marroquí"),
            (117, "MAURICIO",                   "Mauriciana"),
            (118, "MAURITANIA",                 "Mauritana"),
            (119, "MÉXICO",                     "Mexicana"),
            (120, "MICRONESIA",                 "Micronesia"),
            (121, "MOLDAVIA",                   "Moldava"),
            (122, "MÓNACO",                     "Monegasca"),
            (123, "MONGOLIA",                   "Mongola"),
            (124, "MONTENEGRO",                 "Montenegrina"),
            (125, "MOZAMBIQUE",                 "Mozambiqueña"),
            (126, "NAMIBIA",                    "Namibia"),
            (127, "NAURU",                      "Nauruana"),
            (128, "NEPAL",                      "Nepalí"),
            (129, "NICARAGUA",                  "Nicaragüense"),
            (130, "NÍGER",                      "Nigerina"),
            (131, "NIGERIA",                    "Nigeriana"),
            (132, "NORUEGA",                    "Noruega"),
            (133, "NUEVA ZELANDA",              "Neozelandesa"),
            (134, "OMÁN",                       "Omaní"),
            (135, "PAÍSES BAJOS",               "Neerlandesa"),
            (136, "PAKISTÁN",                   "Paquistaní"),
            (137, "PALAOS",                     "Palauana"),
            (138, "PALESTINA",                  "Palestina"),
            (139, "PANAMÁ",                     "Panameña"),
            (140, "PAPÚA NUEVA GUINEA",         "Papú"),
            (141, "PARAGUAY",                   "Paraguaya"),
            (142, "PERÚ",                       "Peruana"),
            (143, "POLONIA",                    "Polaca"),
            (144, "PORTUGAL",                   "Portuguesa"),
            (145, "REINO UNIDO",                "Británica"),
            (146, "REPÚBLICA CENTROAFRICANA",   "Centroafricana"),
            (147, "REPÚBLICA CHECA",            "Checa"),
            (148, "REPÚBLICA DEMOCRÁTICA DEL CONGO", "Congoleña"),
            (149, "REPÚBLICA DOMINICANA",       "Dominicana"),
            (150, "RUANDA",                     "Ruandesa"),
            (151, "RUMANIA",                    "Rumana"),
            (152, "RUSIA",                      "Rusa"),
            (153, "SAMOA",                      "Samoana"),
            (154, "SAN CRISTÓBAL Y NIEVES",     "Sancristobaleña"),
            (155, "SAN MARINO",                 "Sanmarinense"),
            (156, "SAN VICENTE Y LAS GRANADINAS","Sanvicentina"),
            (157, "SANTA LUCÍA",                "Santalucense"),
            (158, "SANTO TOMÉ Y PRÍNCIPE",      "Saotomeña"),
            (159, "SENEGAL",                    "Senegalesa"),
            (160, "SERBIA",                     "Serbia"),
            (161, "SEYCHELLES",                 "Seychellense"),
            (162, "SIERRA LEONA",               "Sierraleonesa"),
            (163, "SINGAPUR",                   "Singapurense"),
            (164, "SIRIA",                      "Siria"),
            (165, "SOMALIA",                    "Somalí"),
            (166, "SRI LANKA",                  "Esrilanquesa"),
            (167, "SUAZILANDIA",                "Suazi"),
            (168, "SUDÁFRICA",                  "Sudafricana"),
            (169, "SUDÁN",                      "Sudanesa"),
            (170, "SUDÁN DEL SUR",              "Sursudanesa"),
            (171, "SUECIA",                     "Sueca"),
            (172, "SUIZA",                      "Suiza"),
            (173, "SURINAM",                    "Surinamesa"),
            (174, "TAILANDIA",                  "Tailandesa"),
            (175, "TAIWÁN",                     "Taiwanesa"),
            (176, "TANZANIA",                   "Tanzana"),
            (177, "TAYIKISTÁN",                 "Tayika"),
            (178, "TIMOR ORIENTAL",             "Timorense"),
            (179, "TOGO",                       "Togolesa"),
            (180, "TONGA",                      "Tongana"),
            (181, "TRINIDAD Y TOBAGO",          "Trinitense"),
            (182, "TÚNEZ",                      "Tunecina"),
            (183, "TURKMENISTÁN",               "Turkmena"),
            (184, "TURQUÍA",                    "Turca"),
            (185, "TUVALU",                     "Tuvaluana"),
            (186, "UCRANIA",                    "Ucraniana"),
            (187, "UGANDA",                     "Ugandesa"),
            (188, "URUGUAY",                    "Uruguaya"),
            (189, "UZBEKISTÁN",                 "Uzbeka"),
            (190, "VANUATU",                    "Vanuatuense"),
            (191, "VATICANO",                   "Vaticana"),
            (192, "VENEZUELA",                  "Venezolana"),
            (193, "VIETNAM",                    "Vietnamita"),
            (194, "YEMEN",                      "Yemení"),
            (195, "YIBUTI",                     "Yibutiana"),
            (196, "ZAMBIA",                     "Zambiana"),
            (197, "ZIMBABUE",                   "Zimbabuense")
        ]

    # Creamos el DataFrame directamente desde la lista de tuplas
        df_paises = pd.DataFrame(paises_data, columns=["COD_PAIS", "NOMBRE_PAIS", "NACIONALIDAD"])
        return df_paises

    # -------------------------------------------------------------------------
    # GENERACIÓN DE RESULT_2 (PRESELECCIÓN) Y SU PROCESADO
    # -------------------------------------------------------------------------
    def generate_result_2(self):
        self._run_in_thread(self.button_generate_result_2, self.process_result_2)

    def process_result_2(self):
        global df_preseleccion, df_glosas, df_resultado_11, df_resultado_3_non_nan, df_preseleccion_updated
        if df_preseleccion is None or df_glosas is None or df_resultado_11 is None:
            messagebox.showwarning(
                "Missing Data",
                "Cargar Preseleccion, Glosas y ejecutar Query primero."
            )
            return

        df = df_preseleccion.copy()
        print(df)
        columns_to_unpivot = [
            'GLOSA_GRATUIDAD', 'GLOSA_BVP', 'GLOSA_BB', 'GLOSA_BEA',
            'GLOSA_BDTE', 'GLOSA_BJGM', 'GLOSA_BNM', 'GLOSA_BHPE','GLOSA_FSCU'
        ]
        df['max_splits'] = df[columns_to_unpivot].apply(
            lambda row: max([len(str(value).split('@')) for value in row]),
            axis=1
        )

        def split_and_fill(value, max_length):
            parts = value.split('@') if isinstance(value, str) and value else ['0']
            return parts + ['0'] * (max_length - len(parts))

        for col in columns_to_unpivot:
            df[col] = df.apply(lambda row: split_and_fill(row[col], row['max_splits']), axis=1)

        # Expandir
        df_expanded = pd.DataFrame({
            col: df.apply(
                lambda row: [row[col]] * row['max_splits'],
                axis=1
            ).explode().reset_index(drop=True)
            for col in df.columns if col != 'max_splits'
        })

        for col in columns_to_unpivot:
            df_expanded[col] = df_expanded[col].explode().reset_index(drop=True)

        df_expanded = df_expanded.astype(str).apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df_preseleccion_updated = self.add_descriptions(df_expanded, df_glosas, columns_to_unpivot)

        # Merge con df_resultado_11
        df_preseleccion_updated['RUT'] = df_preseleccion_updated['RUT'].astype(str).str.strip()
        df_resultado_11['N_DOC'] = df_resultado_11['N_DOC'].astype(str).str.strip()

        df_resultado_3 = df_preseleccion_updated.merge(
            df_resultado_11,
            how='left',
            left_on='RUT',
            right_on='N_DOC'
        )
        df_resultado_3_non_nan = df_resultado_3.dropna(subset=['N_DOC'])

        messagebox.showinfo("Success", "Result 2 (Preselección) generado!")

    def add_descriptions(self, df_preseleccion, df_glosas, columns_to_unpivot):
        df_glosas['GLO_Id_GLOSA_NUM'] = df_glosas['GLO_Id_GLOSA_NUM'].astype(str)
        for col in columns_to_unpivot:
            df_preseleccion[col] = df_preseleccion[col].astype(str)
            df_preseleccion = df_preseleccion.merge(
                df_glosas[['GLO_Id_GLOSA_NUM', 'PGA_DESCRIPCION_TXT']],
                how='left',
                left_on=col,
                right_on='GLO_Id_GLOSA_NUM'
            )
            desc_col_name = f"{col}_DESCRIPCION"
            df_preseleccion.rename(columns={'PGA_DESCRIPCION_TXT': desc_col_name}, inplace=True)
            df_preseleccion[desc_col_name] = df_preseleccion[desc_col_name].fillna('No encontrado').where(
                df_preseleccion[col] != '0', 'Código nulo'
            )
            df_preseleccion.drop(columns=['GLO_Id_GLOSA_NUM'], inplace=True)
        return df_preseleccion

    # -------------------------------------------------------------------------
    # GENERACIÓN DE RESULT_3 (RENOVANTES)
    # -------------------------------------------------------------------------
    def generate_result_3(self):
        self._run_in_thread(self.button_generate_result_3, self.process_result_3)

    def process_result_3(self):
        global df_renovantes, df_resultado_11, df_resultado_4_non_nan
        if df_renovantes is None or df_resultado_11 is None:
            messagebox.showwarning("Missing Data", "Cargar Renovantes y ejecutar SQL primero.")
            return

        # Convertir a string y quitar espacios
        df_renovantes['RUT'] = df_renovantes['RUT'].astype(str).str.strip()
        df_resultado_11['N_DOC'] = df_resultado_11['N_DOC'].astype(str).str.strip()

        # Definimos las llaves de cruce base
        left_on = ['RUT']
        right_on = ['N_DOC']

        # Si ambas columnas de carrera existen, las usamos en el merge
        if 'CODIGO_CARRERA' in df_renovantes.columns and 'COD_CAR' in df_resultado_11.columns:
            # Convertir a string y eliminar espacios
            df_renovantes['CODIGO_CARRERA'] = df_renovantes['CODIGO_CARRERA'].astype(str).str.strip()
            df_resultado_11['COD_CAR'] = df_resultado_11['COD_CAR'].astype(str).str.strip()

            left_on.append('CODIGO_CARRERA')
            right_on.append('COD_CAR')

        # Realizamos el merge
        df_resultado_4 = df_renovantes.merge(
            df_resultado_11,
            how='inner',
            left_on=left_on,
            right_on=right_on
        )

        # Eliminamos filas con NaN en la columna 'N_DOC' (o en la que corresponda)
        df_resultado_4_non_nan = df_resultado_4.dropna(subset=['N_DOC'])

        messagebox.showinfo("Success", "Result 3 (Renovantes) generado!")
        self.button_generate_result_4.config(bg="green")

    # -------------------------------------------------------------------------
    # GENERACIÓN DE RESULT_4 (POTENCIALES RENOVANTES)
    # -------------------------------------------------------------------------
    def generate_result_4(self):
        self._run_in_thread(self.button_generate_result_4, self.process_result_4)
        
    def process_result_4(self):
        global df_pot_renovantes, df_resultado_11, df_resultado_4_non_nan
        if df_pot_renovantes is None or df_resultado_11 is None:
            messagebox.showwarning("Missing Data", "Cargar Potenciales Renovantes y ejecutar SQL primero.")
            return

        df_pot_renovantes['RUT'] = df_pot_renovantes['RUT'].astype(str).str.strip()
        df_resultado_11['N_DOC'] = df_resultado_11['N_DOC'].astype(str).str.strip()

        df_resultado_4 = df_pot_renovantes.merge(
            df_resultado_11,
            how='inner',
            left_on='RUT',
            right_on='N_DOC'
        )
        df_resultado_4_non_nan = df_resultado_4.dropna(subset=['N_DOC'])

        messagebox.showinfo("Success", "Result 4 (Potenciales Renovantes) generado!")
        self.button_generate_result_4.config(bg="green")

        

    # -------------------------------------------------------------------------
    # EXPORTACIÓN DE RESULTADOS
    # -------------------------------------------------------------------------
    def export_result(self, result_number):
        global df_resultado_11, df_resultado_3_non_nan, df_resultado_4_non_nan, df_preseleccion_updated
        df_to_export = None

        if result_number == 1:
            df_to_export = df_resultado_11
        elif result_number == 2:
            df_to_export = df_resultado_3_non_nan
        elif result_number == 3:
            df_to_export = df_resultado_4_non_nan
        elif result_number == 4:
            df_to_export = df_preseleccion_updated
        else:
            messagebox.showerror("Error", "Invalid result number.")
            return

        if df_to_export is None:
            messagebox.showwarning("No Data", f"No data for Result {result_number}.")
            return
        
        num_rows = df_to_export.shape[0]
        if num_rows > 1040000:
            messagebox.showinfo(
                "Exportación CSV",
                (
                    f"El DataFrame tiene {num_rows} filas, "
                    "más de 1.040.000. Excel no soporta tantas filas.\n"
                    "Se exportará automáticamente en formato CSV (;)."
                )
            )
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
                title=f"Save CSV (Result {result_number})"
            )
            if not file_path:
                return
            
            try:
                df_to_export.to_csv(file_path, index=False, sep=';')
                messagebox.showinfo("Success", f"Result {result_number} saved as CSV:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"Error al guardar CSV.\n{e}")
        else:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel Files", "*.xlsx")],
                title=f"Save Result {result_number}"
            )
            if not file_path:
                return
            
            try:
                df_to_export.to_excel(file_path, index=False)
                messagebox.showinfo("Success", f"Result {result_number} saved as Excel:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"Error al guardar Excel.\n{e}")


#=================================FIN CLASS=========================================

# ============================================================== 
#                   CLASE PRINCIPAL (App)
# ============================================================== 

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Uniacc Proceso Becas (Ejemplo Multi-Frame)")
        self.center_window(900, 700)
        self.config(bg="#FFFFFF")
        self.resizable(False, False)

        container = tk.Frame(self, bg="#FFFFFF")
        container.pack(fill="both", expand=True)

        self.frames = {}
        frame_classes = [
            LoginFrame,
            MainMenuFrame,
            IngresaFrame,
            FUASFrame,
            LicitadosFrame,
            SeguimientosFrame,
            IngresaRenovantesFrame,
            SolicitudMontoFrame,
            EgresadosFrame,
            #ValidacionesFrame,
            BecasFrame,
            BecasRenovantesFrame,
            MatriculayValidaciones
        ]

        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)

        for F in frame_classes:
            frame_name = F.__name__
            frame = F(container, self)
            self.frames[frame_name] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame("LoginFrame")

    def center_window(self, width, height):
        self.update_idletasks()
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        self.geometry(f"{width}x{height}+{x}+{y}")

    def show_frame(self, frame_name):
        frame = self.frames[frame_name]
        frame.tkraise()



if __name__ == "__main__":
    app = App()
    app.mainloop()