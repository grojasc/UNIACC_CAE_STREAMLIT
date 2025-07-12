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
    RIGHT(REPLICATE('0',10) + CAST(ARANCEL_SOLICITADO AS varchar(50)),10)  AS ARANCEL_SOLICITADO,
    RIGHT(REPLICATE('0',10) + CAST(ARANCEL_REAL       AS varchar(50)),10)  AS ARANCEL_REAL,

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



def clean_text(text):
    if isinstance(text, str):
        text = unicodedata.normalize('NFKD', text).encode('ASCII','ignore').decode('utf-8')
        text = text.upper()
        text = re.sub(r'[^A-ZÜ\s-]', '', text)
    return text



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

        #
        # Layout base
        #
        # Aumentamos a 12 filas y 5 columnas para acomodar los botones extra
        for row_idx in range(12):
            self.rowconfigure(row_idx, weight=1)
        for col_idx in range(5):
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
        self.btn_exportar_duplicados.grid(row=0, column=5, padx=5, pady=5, sticky="e")

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

        self.btn_usa_extra_1 = tk.Button(
            self, text="Cruzar con Refinanciamiento", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_1
        )
        ####
        self.btn_usa_extra_1.grid(row=5, column=0, columnspan=5, pady=5)

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

        self.btn_usa_extra_2 = tk.Button(
            self, text="Cruzar con refinanciamiento", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_2
        )
        self.btn_usa_extra_2.grid(row=3, column=0, columnspan=5, pady=5)

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

        self.btn_usa_extra_3 = tk.Button(
            self, text="Cruzar con refinanciamiento", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_3
        )
        self.btn_usa_extra_3.grid(row=7, column=0, columnspan=3, pady=5)

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
        self.label_file_1.config(text=f"Archivo #1: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        
        # 2️⃣ Rellena con ceros a la izquierda hasta 8 dígitos
        df_csv["RUT"] = df_csv["RUT"].astype(str)
        #df_csv["RUT"] = df_csv["RUT"].str.zfill(8)

        # Aquí seleccionas las columnas que necesites. Como ejemplo:
        # df_licitados = df_licitados[["RUT", "NOMBRE_IES_RESPALDO", ...]]
        
        df_csv = df_csv[['RUT', 'IES_RESPALDO', 'NOMBRE_IES_RESPALDO','GLOSA_NUEVO','GLOSA_SUPERIOR','NO_VIDENTE','ESTUDIOS_EXTRANJEROS','EXTRANJERO','INFORMADO_CON_BEA','PSU_USADA','ACREDITACION_EXTRANJEROS_PDI','MOROSOS']]

        self.df_resultado_1 = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
        self.df_resultado_1["RUT"] = self.df_resultado_1["RUT"].str.zfill(8)
        cond_gnew = (self.df_resultado_1['GLOSA_NUEVO'] == "Seleccionado Normal ESTADO_SELECCION = 1 - 2 - 3") ## cumple una o la otra
        cond_gsup = (self.df_resultado_1['GLOSA_SUPERIOR'] == "Seleccionado Normal ESTADO_SELECCION = 1 - 2 - 3")

        # IES_RESPALDO debe ser 13
        cond_ies = (self.df_resultado_1['CODIGO_IES'] == '013')

        # Máscara final: Todas las condiciones se deben cumplir
        mask_final = (cond_gnew | cond_gsup) & cond_ies

        # =======================================
        # 3) SEPARAR DATAFRAMES (CUMPLE / NO)
        # =======================================
        df_cumple = self.df_resultado_1[mask_final].copy()
        df_no_cumple = self.df_resultado_1[~mask_final].copy()
        self.df_resultado_no_cruce_1 = df_no_cumple
        # =============================================
        # 4) CREAR COLUMNA "OBSERVACIONES" EN df_cumple
        # =============================================
        def generar_observacion(row):
            observaciones = []

            # 1) no vidente
            if row.get('NO_VIDENTE', 0) == 1:
                observaciones.append("no vidente")

            # 2) estudios extranjeros
            if row.get('ESTUDIOS_EXTRANJEROS', 0) == 1:
                observaciones.append("estudios extranjeros")

            # 3) extranjeros PDI (si EXTRANJERO == 1 o ACREDITACION_EXTRANJEROS_PDI == 1)
            extranjero_flag = (row.get('EXTRANJERO', 0) == 1) or (row.get('ACREDITACION_EXTRANJEROS_PDI', 0) == 1)
            if extranjero_flag:
                observaciones.append("extranjeros PDI")

            # 4) BEA
            if row.get('INFORMADO_CON_BEA', 0) == 1:
                observaciones.append("BEA")

            # 5) cumple PSU
            psu_val = row.get('PSU_USADA', 0)
            if pd.notnull(psu_val/100) and psu_val >= 485: # revisar!!
                observaciones.append("cumple PSU")

           # # 6) moroso
            if row.get('MOROSO', 0) == 1:
               observaciones.append("morosos")

            # Para evitar duplicados si EXTRANJERO y ACREDITACION_EXTRANJEROS_PDI son ambos 1
            observaciones_unicas = list(dict.fromkeys(observaciones))
            return ", ".join(observaciones_unicas)

        df_cumple['OBSERVACIONES'] = df_cumple.apply(generar_observacion, axis=1)
        self.df_resultado_cruce_1 = df_cumple
        # ===========================
        # 5) RESULTADO FINAL
        # ===========================
        print("Registros que CUMPLEN condiciones:", len(df_cumple))
        print("Registros que NO CUMPLEN condiciones:", len(df_no_cumple))

        # df_cumple => Contiene los casos válidos con observaciones
        # df_no_cumple => Contiene los casos que no cumplieron

        df_seleccionados = df_cumple.reset_index(drop=True)
        df_seleccionados


        self.btn_export_1.config(bg="#107FFD")
        self.btn_export_1_b.config(bg="#107FFD")
        self.btn_export_1_c.config(bg="#107FFD")
        self.enable_extra_buttons()

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

        self.label_file_2.config(text=f"Archivo #2: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        # 2️⃣ Rellena con ceros a la izquierda hasta 8 dígitos
        
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        
        # Aquí seleccionas las columnas que necesites. Como ejemplo:
        # df_licitados = df_licitados[["RUT", "NOMBRE_IES_RESPALDO", ...]]
        df_csv["RUT"] = df_csv["RUT"].astype(str)
        df_csv = df_csv[['RUT', 'IES_RESPALDO', 'NOMBRE_IES_RESPALDO','GLOSA_NUEVO','GLOSA_SUPERIOR','NO_VIDENTE','ESTUDIOS_EXTRANJEROS','EXTRANJERO','INFORMADO_CON_BEA','PSU_USADA','ACREDITACION_EXTRANJEROS_PDI','MOROSO']]

        df_cruce = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
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

        self.label_file_3.config(text=f"Archivo #3: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        self.df_resultado_3 = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
        self.btn_export_3.config(bg="#107FFD")
        self.enable_extra_buttons()

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

        #
        # Layout base
        #
        for row_idx in range(15):
            self.rowconfigure(row_idx, weight=1)
        # Ampliamos las columnas de 3 a 5 (tal como en la clase Licitados) 
        for col_idx in range(5):
            self.columnconfigure(col_idx, weight=1)

        tk.Label(
            self, text="RENOVANTES", font=("Arial", 16, "bold"),
            bg="#FFFFFF"
        ).grid(row=0, column=0, columnspan=5, pady=10)

        #
        # 1) Botón para cargar el archivo EXTRA (df_extra)
        #
        btn_cargar_extra = tk.Button(
            self, text="Cargar Refinanciamiento", bg="#008000", fg="white",
            command=self.load_file_extra
        )
        btn_cargar_extra.grid(row=1, column=0, padx=5, pady=5, sticky="w")

        self.label_file_extra = tk.Label(self, text="Sin archivo de refinanciamiento", bg="#FFFFFF")
        self.label_file_extra.grid(row=1, column=1, columnspan=4, padx=5, pady=5, sticky="w")

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

        self.btn_usa_extra_1 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#1)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_1
        )
        self.btn_usa_extra_1.grid(row=3, column=0, columnspan=5, pady=5)


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

        self.btn_usa_extra_2 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#2)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_2
        )
        self.btn_usa_extra_2.grid(row=5, column=0, columnspan=5, pady=5)

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

        self.btn_usa_extra_3 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#3)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_3
        )
        self.btn_usa_extra_3.grid(row=7, column=0, columnspan=5, pady=5)

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

        self.btn_usa_extra_4 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#4)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_4
        )
        self.btn_usa_extra_4.grid(row=9, column=0, columnspan=5, pady=5)

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

        self.btn_usa_extra_5 = tk.Button(
            self, text="Cruzar con Refinanciamiento (#5)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.operar_con_extra_5
        )
        self.btn_usa_extra_5.grid(row=11, column=0, columnspan=5, pady=5)

        # (Opcional) Botón para volver a otro frame
        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=12, column=0, columnspan=5, pady=20)


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

        self.label_file_1.config(text=f"Archivo #1: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv['RUT'] = df_csv['RUT'].astype(str)
        # Ajustar columnas según corresponda
        if 'IES' not in df_csv.columns:
            df_csv['IES'] = None  # Ejemplo, si no existe

        self.df_resultado_1 = pd.merge(df_licitados, df_csv, on='RUT', how='inner')
        # Filtra a IES=13, por ejemplo
        self.df_resultado_1 = self.df_resultado_1[self.df_resultado_1['IES'] == '013']
        self.btn_export_1.config(bg="#107FFD")

        self.enable_extra_buttons()

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

        self.label_file_2.config(text=f"Archivo #2: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return

        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv['RUT'] = df_csv['RUT'].astype(str)
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        # Merge
        df_cruce_2 = pd.merge(df_licitados, df_csv, on='RUT', how='inner')

        # =============================
        # EJEMPLO de condición para separar 'cumple' vs. 'no cumple'
        # Ajusta la lógica a tus necesidades reales
        # =============================
        # Supongamos que en df_csv hay una columna "ESTADO_ACTUAL"
        # y consideramos "cumple" si ESTADO_ACTUAL == "APROBADO"
        estados_validos_5B = [4, 8, 12, 13, 18, 19, 21, 23, 24, 35]
        cond_cumple_2 = df_cruce_2['ESTADO_ACTUAL'].isin(estados_validos_5B)

        self.df_resultado_cruce_2 = df_cruce_2[cond_cumple_2].copy()
        self.df_resultado_no_cruce_2 = df_cruce_2[~cond_cumple_2].copy()
        self.df_resultado_2 = df_cruce_2  # Este mantiene todos

        # Activa botón de exportar
        self.btn_export_2.config(bg="#107FFD")

        # También podrías cambiar a azul (bg="#107FFD") los botones de cumple/no cumple
        # para indicar que hay datos
        self.btn_export_2_cumple.config(bg="#107FFD")
        self.btn_export_2_no_cumple.config(bg="#107FFD")

        self.enable_extra_buttons()

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

        self.label_file_3.config(text=f"Archivo #3: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv['RUT'] = df_csv['RUT'].astype(str)
        df_csv = df_csv[['RUT','IESN_COD','ESTADO_RENOVANTE','CONTADOR_CAMBIOS']]
        df_cruce_3 = pd.merge(df_licitados, df_csv, on='RUT', how='inner')

        # =============================
        # EJEMPLO de condición para separar 'cumple' vs. 'no cumple'
        # =============================
        # Supongamos que la columna "ULTIMO_NIVEL" indica si cumple


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

        self.label_file_4.config(text=f"Archivo #4: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv['RUT'] = df_csv['RUT'].astype(str)

        self.df_resultado_4 = pd.merge(df_licitados, df_csv, on='RUT', how='inner')
        self.btn_export_4.config(bg="#107FFD")

        self.enable_extra_buttons()

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

        self.label_file_5.config(text=f"Archivo #5: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados['RUT'] = df_licitados['RUT'].astype(str)
        df_csv['RUT'] = df_csv['RUT'].astype(str)

        self.df_resultado_5 = pd.merge(df_licitados, df_csv, on='RUT', how='inner')
        self.btn_export_5.config(bg="#107FFD")

        self.enable_extra_buttons()

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
        
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        
        # Sub-procesos #1, #2, #3, #4
        self.df_resultado_1 = None
        self.df_resultado_2 = None
        self.df_resultado_3 = None
        self.df_resultado_4 = None

        # NUEVO: Para el sub-proceso "RUT"
        self.df_csv_rut = None
        self.df_resultado_rut = None

        #
        # Layout base
        #
        for row_idx in range(12):
            self.rowconfigure(row_idx, weight=1)
        for col_idx in range(3):
            self.columnconfigure(col_idx, weight=1)

        tk.Label(
            self, text="Sub-proceso: Licitados", font=("Arial", 16, "bold"),
            bg="#FFFFFF"
        ).grid(row=0, column=0, columnspan=3, pady=10)

        #
        # SUB-PROCESO #1 (Firma en Banco)
        #
        self.btn_cargar_1 = tk.Button(
            self, text="Firma en Banco (#1)", bg="#107FFD", fg="white",
            command=self.load_file_licitados_1
        )
        self.btn_cargar_1.grid(row=1, column=0, padx=5, pady=5)

        self.label_file_1 = tk.Label(self, text="Sin archivo (#1)", bg="#FFFFFF")
        self.label_file_1.grid(row=1, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_1 = tk.Button(
            self, text="Exportar #1 (Cruzado c/Matrícula)", bg="#cccccc", fg="white",
            command=self.export_licitados_1
        )
        self.btn_export_1.grid(row=1, column=2, padx=5, pady=5)

        #
        # SUB-PROCESO #2 (Firma en Certificación)
        #
        self.btn_cargar_2 = tk.Button(
            self, text="Firma en Certificación (#2)", bg="#107FFD", fg="white",
            command=self.load_file_licitados_2
        )
        self.btn_cargar_2.grid(row=2, column=0, padx=5, pady=5)

        self.label_file_2 = tk.Label(self, text="Sin archivo (#2)", bg="#FFFFFF")
        self.label_file_2.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_2 = tk.Button(
            self, text="Exportar #2 (Cruzado c/Matrícula)", bg="#cccccc", fg="white",
            command=self.export_licitados_2
        )
        self.btn_export_2.grid(row=2, column=2, padx=5, pady=5)

        #
        # SUB-PROCESO #3 (Reporte Licitados)
        #
        self.btn_cargar_3 = tk.Button(
            self, text="Reporte Licitados (#3)", bg="#107FFD", fg="white",
            command=self.load_file_licitados_3
        )
        self.btn_cargar_3.grid(row=3, column=0, padx=5, pady=5)

        self.label_file_3 = tk.Label(self, text="Sin archivo (#3)", bg="#FFFFFF")
        self.label_file_3.grid(row=3, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_3 = tk.Button(
            self, text="Exportar #3 (Cruzado c/Matrícula)", bg="#cccccc", fg="white",
            command=self.export_licitados_3
        )
        self.btn_export_3.grid(row=3, column=2, padx=5, pady=5)

        #
        # SUB-PROCESO #4 (Reporte con Categoría)
        #
        self.btn_cargar_4 = tk.Button(
            self, text="Reporte con Categoría (#4)", bg="#107FFD", fg="white",
            command=self.load_file_licitados_4
        )
        self.btn_cargar_4.grid(row=4, column=0, padx=5, pady=5)

        self.label_file_4 = tk.Label(self, text="Sin archivo (#4)", bg="#FFFFFF")
        self.label_file_4.grid(row=4, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_4 = tk.Button(
            self, text="Exportar #4 (Cruzado c/Matrícula)", bg="#cccccc", fg="white",
            command=self.export_licitados_4
        )
        self.btn_export_4.grid(row=4, column=2, padx=5, pady=5)

        #
        # SUB-PROCESO "RUT" (NUEVO)
        # Repite la misma lógica: se cruza con df_licitados, sin refinanciamiento
        #
        self.btn_cargar_rut = tk.Button(
            self, text="Cargar RUT (Adicional)", bg="#107FFD", fg="white",
            command=self.load_file_rut
        )
        self.btn_cargar_rut.grid(row=5, column=0, padx=5, pady=5)

        self.label_file_rut = tk.Label(self, text="Sin archivo (RUT)", bg="#FFFFFF")
        self.label_file_rut.grid(row=5, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_rut = tk.Button(
            self, text="Exportar RUT (Cruzado c/Matrícula)", bg="#cccccc", fg="white",
            command=self.export_rut
        )
        self.btn_export_rut.grid(row=5, column=2, padx=5, pady=5)

        #
        # Botón para volver
        #
        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=6, column=0, columnspan=3, pady=20)

    # ------------------------------------
    #       SUB-PROCESO #1
    # ------------------------------------
    def load_file_licitados_1(self):
        df_csv, file_path = read_any_file("Firma Banco (#1)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #1 no contiene 'RUT'.")
            return

        self.label_file_1.config(text=f"Archivo #1: {os.path.basename(file_path)}")

        # Cruzamos con df_licitados
        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        self.df_resultado_1 = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
        self.btn_export_1.config(bg="#107FFD")

    def export_licitados_1(self):
        if self.df_resultado_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#1).")
            return
        self._save_df_to_excel(self.df_resultado_1, "Licitados_Seleccionados_1")


    # ------------------------------------
    #       SUB-PROCESO #2
    # ------------------------------------
    def load_file_licitados_2(self):
        df_csv, file_path = read_any_file("Firma Certificación (#2)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #2 no contiene la columna 'RUT'.")
            return

        self.label_file_2.config(text=f"Archivo #2: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        self.df_resultado_2 = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
        self.btn_export_2.config(bg="#107FFD")

    def export_licitados_2(self):
        if self.df_resultado_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#2).")
            return
        self._save_df_to_excel(self.df_resultado_2, "Licitados_Preseleccionados_2")


    # ------------------------------------
    #       SUB-PROCESO #3
    # ------------------------------------
    def load_file_licitados_3(self):
        df_csv, file_path = read_any_file("Reporte Licitados (#3)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #3 no contiene la columna 'RUT'.")
            return

        self.label_file_3.config(text=f"Archivo #3: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        self.df_resultado_3 = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
        self.btn_export_3.config(bg="#107FFD")

    def export_licitados_3(self):
        if self.df_resultado_3 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#3).")
            return
        self._save_df_to_excel(self.df_resultado_3, "Licitados_NoSeleccionados_3")


    # ------------------------------------
    #       SUB-PROCESO #4
    # ------------------------------------
    def load_file_licitados_4(self):
        df_csv, file_path = read_any_file("Reporte con Categoría (#4)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo #4 no contiene la columna 'RUT'.")
            return

        self.label_file_4.config(text=f"Archivo #4: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        self.df_resultado_4 = pd.merge(df_licitados, df_csv, on="RUT", how="inner")
        self.btn_export_4.config(bg="#107FFD")

    def export_licitados_4(self):
        if self.df_resultado_4 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#4).")
            return
        self._save_df_to_excel(self.df_resultado_4, "Licitados_Subproceso_4")


    # ------------------------------------
    #       NUEVO SUB-PROCESO (RUT)
    # ------------------------------------
    def load_file_rut(self):
        """
        Carga un archivo RUT adicional y lo cruza solo con df_licitados,
        igual que los demás sub-procesos.
        """
        df_csv, file_path = read_any_file("Archivo RUT adicional")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo RUT no contiene la columna 'RUT'.")
            return

        self.label_file_rut.config(text=f"Archivo RUT: {os.path.basename(file_path)}")

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str)
        df_csv["RUT"] = df_csv["RUT"].astype(str)

        # Lo guardamos en una variable "df_resultado_rut" (análoga a #1..#4)
        self.df_resultado_rut = pd.merge(df_licitados, df_csv, on='RUT', how='inner')

        # Podríamos crear un botón "Exportar RUT" si lo deseas,
        # pero en este ejemplo, usaremos el ya definido self.btn_export_rut
        self.btn_export_rut.config(bg="#107FFD")

    def export_rut(self):
        if self.df_resultado_rut is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (RUT).")
            return
        self._save_df_to_excel(self.df_resultado_rut, "Licitados_RUT")


    # ------------------------------------
    #  FUNCIÓN AUXILIAR PARA EXPORTAR
    # ------------------------------------
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








class EgresadosFrame(tk.Frame):
    """
    Sub-proceso Egresados.
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")
        
        # --------------------------------------------------------
        # 1) Ejecutamos la query al iniciar y guardamos en df_egresados
        # --------------------------------------------------------
        # Ajusta la conexión a la que necesites (connection1 o connection2).
        
        self.connection = connection1  
        self.df_egresados = None
        self.run_query_egresados()
        # DataFrames resultantes de cada “no cruce”
        self.df_egresados_not_found_1 = None
        self.df_egresados_not_found_2 = None
        self.df_egresados_not_found_3 = None
        self.df_egresados_not_found_4 = None
        # --------------------------------------------------------
        # 1.1) Configuramos el grid del propio Frame
        # --------------------------------------------------------
        # Ajusta la cantidad de filas/columnas según tu diseño
        for row_idx in range(8):  # 0..6
            self.rowconfigure(row_idx, weight=1)
        for col_idx in range(3):  # 0..2
            self.columnconfigure(col_idx, weight=1)
        # --------------------------------------------------------
        # 2) agregamos logo
        # --------------------------------------------------------
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
            tk.Label(self, image=self.logo, bg="#FFFFFF").grid(row=0, column=0, columnspan=3, pady=(10,10))
        except Exception as e:
            print(f"No se pudo cargar la imagen del logo: {e}")
            tk.Label(self, text="[Logo Aquí]", bg="#FFFFFF", fg="#107FFD",
                     font=("Arial", 12, "bold")).grid(row=0, column=0, columnspan=3, pady=(10,10))
        # --------------------------------------------------------
        # 3) Interfaz de Tkinter (usando grid)
        # --------------------------------------------------------

        # Título (fila 1)
        tk.Label(
            self,
            text="Sub-proceso: Egresados",
            font=("Arial", 16, "bold"),
            bg="#FFFFFF"
        ).grid(row=1, column=0, columnspan=3, pady=(10,10))

        # --------------------------------------------------------
        # 4) Fila 2: Primer par de Cargar / Label / Export
        # --------------------------------------------------------
        # Botón "Cargar CSV #1"
        self.btn_cargar_1 = tk.Button(
            self, text="Cargar CSV Egresados 5A #1", bg="#107FFD", fg="white",
            command=self.load_file_egresados_1
        )
        self.btn_cargar_1.grid(row=2, column=0, padx=5, pady=5)

        # Label para mostrar info del archivo
        self.label_file_1 = tk.Label(
            self, text="Sin archivo (#1)", bg="#FFFFFF"
        )
        self.label_file_1.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        # Botón "Exportar #1"
        self.btn_export_1 = tk.Button(
            self, text="Exportar NO Cruce #1", bg="#cccccc", fg="white",
            command=self.export_egresados_1
        )
        self.btn_export_1.grid(row=2, column=2, padx=5, pady=5)

        # --------------------------------------------------------
        # 5) Fila 3: Segundo par de Cargar / Label / Export
        # --------------------------------------------------------
        self.btn_cargar_2 = tk.Button(
            self, text="Cargar CSV Egresados 5B #2", bg="#107FFD", fg="white",
            command=self.load_file_egresados_2
        )
        self.btn_cargar_2.grid(row=3, column=0, padx=5, pady=5)

        self.label_file_2 = tk.Label(
            self, text="Sin archivo (#2)", bg="#FFFFFF"
        )
        self.label_file_2.grid(row=3, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_2 = tk.Button(
            self, text="Exportar NO Cruce #2", bg="#cccccc", fg="white",
            command=self.export_egresados_2
        )
        self.btn_export_2.grid(row=3, column=2, padx=5, pady=5)

        # --------------------------------------------------------
        # 6) Fila 4: Tercer par de Cargar / Label / Export
        # --------------------------------------------------------
        self.btn_cargar_3 = tk.Button(
            self, text="Cargar CSV Egresados DESERTORES #3", bg="#107FFD", fg="white",
            command=self.load_file_egresados_3
        )
        self.btn_cargar_3.grid(row=4, column=0, padx=5, pady=5)

        self.label_file_3 = tk.Label(
            self, text="Sin archivo (#3)", bg="#FFFFFF"
        )
        self.label_file_3.grid(row=4, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_3 = tk.Button(
            self, text="Exportar NO Cruce #3", bg="#cccccc", fg="white",
            command=self.export_egresados_3
        )
        self.btn_export_3.grid(row=4, column=2, padx=5, pady=5)

        # --------------------------------------------------------
        # 7) Botón Volver (fila 5)
        # --------------------------------------------------------
        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=6, column=0, columnspan=3, pady=(20,10))

        # --------------------------------------------------------
        # Fila para el 4to botón
        # --------------------------------------------------------
        self.btn_cargar_4 = tk.Button(
            self,
            text="Cargar CSV/TXT Egresados",
            bg="#107FFD", fg="white",
            command=self.load_file_vs_no_cruces
        )
        self.btn_cargar_4.grid(row=5, column=0, padx=5, pady=5)

        self.label_file_4 = tk.Label(
            self, text="Sin archivo (#4)", bg="#FFFFFF"
        )
        self.label_file_4.grid(row=5, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_4 = tk.Button(
            self,
            text="Exportar NO Cruce #4",
            bg="#cccccc", fg="white",
            command=self.export_vs_no_cruces
        )
        self.btn_export_4.grid(row=5, column=2, padx=5, pady=5)



    def run_query_egresados(self):
        query = text("""
                        select
                            b.RUT,
                            c.DV, 
                            a.CODCLI,
                            CODIGO_SIES_COMPLETO,
                            PATERNO, 
                            MATERNO, 
                            NOMBRES, 
                            GENERO, 
                            c.FECH_NAC,
                            DIRECCION,
                            NACIONALIDAD,
                            (SELECT TOP 1 X.CODIGO_CIUDAD FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_CIUDAD,
                            (SELECT TOP 1 X.CODIGO_COMUNA FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_COMUNA, 
                            (SELECT TOP 1 X.CODIGO_REGION FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_REGION,
                            c.TELEFONO, 
                            c.MAIL_UNIACC, 
                            FECHA_EGRESO,
                            pm.PERIODO as ANO_COHORTE,
                            pm.PERIODO as ANO_INGRESO_INSTITUCION,
                            d.NOMBRE_CARRERA,
                                1 as 'CODIGO_TIPO_IES',
                                13  as 'CODIGO_DE_IES',
                                j.SEDEN_COD  as 'CODIGO_DE_SEDE',
                                j.CARRN_COD  as 'CODIGO_CARRERA',
                                j.JORNN_COD  as 'CODIGO_JORNADA', 
                                    CASE WHEN j.JORNN_COD=1 THEN 'Diurno' ELSE 'Vespertino/Semipresencial/Online'END AS 'JORNADA',
                            F.ARANCEL_ANUAL AS 'ARANCEL_REAL_ANUAL',
                            0 AS 'ARANCEL DE REFERENCIA',FECHA_MAT -- SE CARGA A MANO POSTERIOR A LA GENERACIÓN DEL ARCHIVO EXCEL-SE INCORPORA FECHA MAT
                        from ft_egreso a
                        left join (select distinct CODCLI, PERIODO from ft_matricula where MAT_N = 1 ) pm on a.codcli = pm.codcli
                        inner join dim_matricula b on a.CODCLI = b.CODCLI
                        inner join dim_alumno c on b.RUT = c.RUT
                        inner join dim_plan_academico d on b.CODPLAN = d.LLAVE_MALLA
                        inner join (select  CODIGO_SIES, ARANCEL_ANUAL,
                                            ROW_NUMBER() over (partition by CODIGO_SIES order by periodo desc )	as numero
                                            from dim_oferta_academica) f  on d.CODIGO_SIES_COMPLETO = f.CODIGO_SIES and numero = 1
                        left join dim_territorio i on c.COMUNA=i.COMUNA
                        left join (select distinct [CODIGO SIES SIN VERSION], SEDEN_COD, CARRN_COD, JORNN_COD, NOMBRE_CARRERA,
                                ROW_NUMBER() over (partition by [CODIGO SIES SIN VERSION] order by[CODIGO SIES SIN VERSION] )	as numero
                                    from oferta_academica_ingresa where carrera_discontinua = 'NO'	) j
                        on left (d.CODIGO_SIES_COMPLETO,LEN (d.CODIGO_SIES_COMPLETO)-2)=j.[CODIGO SIES SIN VERSION]
                        inner join (select CODCLI, FECHA_MAT,    
                                ROW_NUMBER() OVER (partition by CODCLI ORDER BY FECHA_MAT DESC) AS numero
                                from ft_matricula) fm on a.CODCLI = fm.CODCLI and fm.numero = 1
                        where 1 = 1
                        and d.NIVEL_GLOBAL = 'PREGRADO'
                        and CODIGO_SIES_COMPLETO <> '0'		
        """)
        try:
            if self.connection is not None:
                self.df_egresados = pd.read_sql_query(query, self.connection)
                print("Query ejecutada y df_egresados cargado correctamente.")
            else:
                print("No se ejecutó la query: conexión no proporcionada.")
        except Exception as e:
            print(f"Error al ejecutar query: {e}")

    
    # ----------------------------------------------------------------
    # 2) LOAD / MERGE / FILTRO NO CRUCE (por RUT)
    # ----------------------------------------------------------------
    def load_file_egresados_1(self):
        """
        Carga un archivo CSV Egresados #1, cruza por RUT contra df_egresados_query
        y deja en self.df_egresados_not_found_1 lo que NO cruza.
        """
        file_path = filedialog.askopenfilename(
            title="Seleccionar CSV Egresados #1",
            filetypes=[("CSV Files", "*.csv"), ("TXT Files", "*.txt"), ("All", "*.*")]
        )
        if file_path:
            df_loaded = read_any_file(file_path)
            if df_loaded is not None:
                # Nos aseguramos de que exista la columna "RUT" (ajusta a tu columna real)
                if "RUT" not in df_loaded.columns:
                    messagebox.showerror("Error", "El CSV no contiene la columna 'RUT'.")
                    return

                self.label_file_1.config(text=f"Archivo #1: {os.path.basename(file_path)}")
                  # Convertir a str
                df_loaded['RUT'] = df_loaded['RUT'].astype(str)
                self.df_egresados['RUT'] = self.df_egresados['RUT'].astype(str)
                # Hacemos el cruce "left" y filtramos los que no matchean
                df_result = pd.merge(
                    df_loaded,
                    self.df_egresados[["RUT"]] if self.df_egresados is not None else pd.DataFrame(columns=["RUT"]),
                    on="RUT",
                    how="left",
                    indicator=True
                )
                # Nos quedamos solo con left_only (RUT que NO existen en df_egresados_query)
                df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

                self.df_egresados_not_found_1 = df_result
                # Cambiamos color del botón "Exportar" a azul (ahora hay algo que exportar)
                self.btn_export_1.config(bg="#107FFD")

                messagebox.showinfo("Cargado", f"Archivo #1 cargado. {len(df_result)} filas no cruzan.")
        else:
            messagebox.showinfo("Cancelado", "No se seleccionó archivo.")

    def load_file_egresados_2(self):
        file_path = filedialog.askopenfilename(
            title="Seleccionar CSV Egresados #2",
            filetypes=[("CSV Files", "*.csv"), ("TXT Files", "*.txt"), ("All", "*.*")]
        )
        if file_path:
            df_loaded = read_any_file(file_path)
            if df_loaded is not None:
                if "RUT" not in df_loaded.columns:
                    messagebox.showerror("Error", "El CSV #2 no contiene la columna 'RUT'.")
                    return

                self.label_file_2.config(text=f"Archivo #2: {os.path.basename(file_path)}")
                df_loaded['RUT'] = df_loaded['RUT'].astype(str)
                self.df_egresados['RUT'] = self.df_egresados['RUT'].astype(str)
                df_result = pd.merge(
                    df_loaded,
                    self.df_egresados[["RUT"]] if self.df_egresados is not None else pd.DataFrame(columns=["RUT"]),
                    on="RUT",
                    how="left",
                    indicator=True
                )
                df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

                self.df_egresados_not_found_2 = df_result
                self.btn_export_2.config(bg="#107FFD")

                messagebox.showinfo("Cargado", f"Archivo #2 cargado. {len(df_result)} filas no cruzan.")
        else:
            messagebox.showinfo("Cancelado", "No se seleccionó archivo.")

    def load_file_egresados_3(self):
        file_path = filedialog.askopenfilename(
            title="Seleccionar CSV Egresados #3",
            filetypes=[("CSV Files", "*.csv"), ("TXT Files", "*.txt"), ("All", "*.*")]
        )
        if file_path:
            df_loaded = read_any_file(file_path)
            if df_loaded is not None:
                if "RUT" not in df_loaded.columns:
                    messagebox.showerror("Error", "El CSV #3 no contiene la columna 'RUT'.")
                    return

                self.label_file_3.config(text=f"Archivo #3: {os.path.basename(file_path)}")
                df_loaded['RUT'] = df_loaded['RUT'].astype(str)
                self.df_egresados['RUT'] = self.df_egresados['RUT'].astype(str)
                
                df_result = pd.merge(
                    df_loaded,
                    self.df_egresados[["RUT"]] if self.df_egresados is not None else pd.DataFrame(columns=["RUT"]),
                    on="RUT",
                    how="left",
                    indicator=True
                )
                df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

                self.df_egresados_not_found_3 = df_result
                self.btn_export_3.config(bg="#107FFD")

                messagebox.showinfo("Cargado", f"Archivo #3 cargado. {len(df_result)} filas no cruzan.")
        else:
            messagebox.showinfo("Cancelado", "No se seleccionó archivo.")

    # ----------------------------------------------------------------
    # 3) EXPORTAR: Guarda el DF "no cruzado" en Excel
    # ----------------------------------------------------------------
    def export_egresados_1(self):
        if self.df_egresados_not_found_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#1).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_1, "Egresados_NO_Cruce_1")

    def export_egresados_2(self):
        if self.df_egresados_not_found_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#2).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_2, "Egresados_NO_Cruce_2")

    def export_egresados_3(self):
        if self.df_egresados_not_found_3 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#3).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_3, "Egresados_NO_Cruce_3")


        # ------------------------------------------------------------
    # [NUEVO] Cargar 4to archivo vs no-cruces #1,#2,#3
    # ------------------------------------------------------------
    def load_file_vs_no_cruces(self):
        file_path = filedialog.askopenfilename(
            title="Seleccionar CSV/TXT para cruzar vs. NO Cruce #1,#2,#3",
            filetypes=[
                ("CSV Files", "*.csv"),
                ("TXT Files", "*.txt"),
                ("All", "*.*")
            ]
        )
        if not file_path:
            messagebox.showinfo("Cancelado", "No se seleccionó archivo.")
            return

        # Leemos CSV/TXT con la misma función que usas antes:
        df_loaded = read_any_file(file_path)
        if df_loaded is None:
            messagebox.showerror("Error", "No se pudo leer el archivo CSV/TXT.")
            return

        if "RUT" not in df_loaded.columns:
            messagebox.showerror("Error", "El archivo no contiene la columna 'RUT'.")
            return

        self.label_file_4.config(text=f"Archivo #4: {os.path.basename(file_path)}")
        df_loaded["RUT"] = df_loaded["RUT"].astype(str)

        # Unimos las 3 salidas de no-cruce (si alguna es None, la ignoramos).
        frames_no_cruce = []
        if self.df_egresados_not_found_1 is not None:
            frames_no_cruce.append(self.df_egresados_not_found_1[["RUT"]])
        if self.df_egresados_not_found_2 is not None:
            frames_no_cruce.append(self.df_egresados_not_found_2[["RUT"]])
        if self.df_egresados_not_found_3 is not None:
            frames_no_cruce.append(self.df_egresados_not_found_3[["RUT"]])

        if len(frames_no_cruce) == 0:
            messagebox.showinfo(
                "Sin cruces previos",
                "No hay datos de no-cruce (#1,#2,#3), no se puede comparar."
            )
            return

        # Concatenamos y quitamos duplicados de RUT
        df_no_cruces_union = pd.concat(frames_no_cruce, ignore_index=True).drop_duplicates()

        # Merge para quedarnos con los RUT que no estén en df_no_cruces_union
        df_result = pd.merge(
            df_loaded,
            df_no_cruces_union,
            on="RUT",
            how="left",
            indicator=True
        )
        # Nos quedamos con los que no matchean (left_only)
        df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

        self.df_egresados_not_found_4 = df_result
        self.btn_export_4.config(bg="#107FFD")

        messagebox.showinfo(
            "Cargado",
            f"Archivo #4 cargado. {len(df_result)} filas NO se cruzan "
            "con las salidas de #1,#2,#3."
        )

    def export_vs_no_cruces(self):
        if self.df_egresados_not_found_4 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#4).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_4, "Egresados_NO_Cruce_4")
    def _save_df_to_excel(self, df: pd.DataFrame, default_name: str):
        """
        Abre un diálogo para guardar el DataFrame en un Excel (.xlsx).
        """
        file_path = filedialog.asksaveasfilename(
            title="Guardar archivo Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            initialfile=f"{default_name}.xlsx"
        )
        if file_path:
            try:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Se guardó el archivo en:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar:\n{e}")
        else:
            messagebox.showinfo("Cancelado", "No se exportó el archivo.")
""" DUPLICADO?
class ValidacionesFrame(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")
        tk.Label(
            self, text="Validaciones Previas", bg="#FFFFFF", fg="#107FFD",
            font=("Arial", 20, "bold")
        ).pack(pady=20)
        tk.Button(
            self, text="Volver Menú", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("MainMenuFrame")
        ).pack(pady=20)
"""

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

# ============================================================== 
#       CLASE SOLICITUD DE MONTO (ADAPTADA) 
# ============================================================== 


class SolicitudMontoFrame(tk.Frame):
    """
    Flujo:
      1) Cargar Excel de refinanciamiento (df_extra).
      2) Cargar archivos 1A y 1B.
      3) "Exportar cruce con Matrícula" -> (1A + 1B) se cruzan con df_licitados => self.df_result
      4) Se habilita "Exportar cruce con refinanciamiento" (para 1A+1B)
         -> self.df_result se cruza con df_extra.
      5) NUEVO: Botón para cargar RUT (1C) en otra fila.
         -> "Exportar cruce con Matrícula (RUT)" => self.df_result_rut
         -> "Exportar cruce con Refinanciamiento (RUT)" => se habilita tras el cruce anterior.
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")

        global df_licitados  # Se asume que existe en tu código principal

        # DataFrames
        self.df_extra = None         # Excel de refinanciamiento
        self.df_csv_1 = None         # Archivo 1A
        self.df_csv_2 = None         # Archivo 1B
        self.df_result = None        # Cruce (1A+1B) con df_licitados

        self.df_csv_rut = None       # NUEVO: Archivo "RUT" (1C)
        self.df_result_rut = None    # Resultado de cruce (RUT con df_licitados)

        #
        # Layout base
        #
        for row_idx in range(10):
            self.rowconfigure(row_idx, weight=1)
        for col_idx in range(3):
            self.columnconfigure(col_idx, weight=1)

        tk.Label(
            self, text="SOLICITUD DE MONTO",
            font=("Arial", 16, "bold"), bg="#FFFFFF"
        ).grid(row=0, column=0, columnspan=3, pady=10)

        #
        # 1) Cargar Refinanciamiento (df_extra)
        #
        self.btn_cargar_ref = tk.Button(
            self, text="Cargar Excel Refinanciamiento", bg="#008000", fg="white",
            command=self.load_file_refinanciamiento
        )
        self.btn_cargar_ref.grid(row=1, column=0, padx=5, pady=5)

        self.label_file_ref = tk.Label(
            self, text="Sin archivo refinanciamiento", bg="#FFFFFF"
        )
        self.label_file_ref.grid(row=1, column=1, columnspan=2, padx=5, pady=5, sticky="w")

        #
        # 2) Botones para cargar 1A y 1B
        #
        self.btn_cargar_1a = tk.Button(
            self, text="Cargar Reporte 5A (1A)", bg="#107FFD", fg="white",
            command=self.load_file_1a
        )
        self.btn_cargar_1a.grid(row=2, column=0, padx=5, pady=5)

        self.label_file_1a = tk.Label(self, text="Sin archivo (1A)", bg="#FFFFFF")
        self.label_file_1a.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        self.btn_cargar_1b = tk.Button(
            self, text="Cargar Solicitud de Monto (1B)", bg="#107FFD", fg="white",
            command=self.load_file_1b
        )
        self.btn_cargar_1b.grid(row=3, column=0, padx=5, pady=5)

        self.label_file_1b = tk.Label(self, text="Sin archivo (1B)", bg="#FFFFFF")
        self.label_file_1b.grid(row=3, column=1, padx=5, pady=5, sticky="w")

        #
        # 3) Exportar cruce con Matrícula (1A+1B con df_licitados)
        #
        self.btn_export_cruce_matricula = tk.Button(
            self, text="Exportar cruce con Matrícula", bg="#cccccc", fg="white",
            command=self.export_cruce_con_matricula
        )
        self.btn_export_cruce_matricula.grid(row=4, column=0, columnspan=2, pady=10)

        #
        # 4) Exportar cruce con Refinanciamiento (df_extra),
        #    inicialmente deshabilitado hasta que se haga el cruce con Matrícula.
        #
        self.btn_export_cruce_refinanciamiento = tk.Button(
            self, text="Exportar cruce c/Refinanciamiento", bg="#cccccc", fg="white",
            state="disabled",
            command=self.export_cruce_con_refinanciamiento
        )
        self.btn_export_cruce_refinanciamiento.grid(row=4, column=2, padx=5, pady=5)


        # ─────────────────────────────────────────────────────────────────
        # NUEVO: 5) BOTÓN PARA CARGAR ARCHIVO "RUT" (1C)
        # ─────────────────────────────────────────────────────────────────
        self.btn_cargar_rut = tk.Button(
            self, text="Cargar RUT (1C)", bg="#107FFD", fg="white",
            command=self.load_file_rut
        )
        self.btn_cargar_rut.grid(row=5, column=0, padx=5, pady=5)

        self.label_file_rut = tk.Label(self, text="Sin archivo (RUT)", bg="#FFFFFF")
        self.label_file_rut.grid(row=5, column=1, padx=5, pady=5, sticky="w")

        # Botón para "Exportar cruce con Matrícula (RUT)"
        self.btn_export_cruce_matricula_rut = tk.Button(
            self, text="Exportar cruce con Matrícula (RUT)", bg="#cccccc", fg="white",
            command=self.export_cruce_con_matricula_rut
        )
        self.btn_export_cruce_matricula_rut.grid(row=6, column=0, columnspan=2, pady=10)

        # Botón para "Exportar cruce con Refinanciamiento (RUT)",
        # inicialmente deshabilitado
        self.btn_export_cruce_refinanciamiento_rut = tk.Button(
            self, text="Exportar cruce c/Refinanciamiento (RUT)", bg="#cccccc", fg="white",
            state="disabled",
            command=self.export_cruce_con_refinanciamiento_rut
        )
        self.btn_export_cruce_refinanciamiento_rut.grid(row=6, column=2, padx=5, pady=5)


        #
        # Botón para volver
        #
        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=9, column=0, columnspan=3, pady=20)


    # ======================================================
    #   MÉTODO: Cargar Excel Refinanciamiento (df_extra)
    # ======================================================
    def load_file_refinanciamiento(self):
        df_csv, file_path = read_any_file("Seleccionar Excel Refinanciamiento")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns and "RUTALU" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo de refinanciamiento no contiene RUT/RUTALU.")
            return

        # Ajusta según tus columnas
        df_csv["RUTALU"] = df_csv["RUTALU"].astype(str).str.strip()
       # df_csv[["RUT","DV"]] = df_csv["RUTALU"].str.split("-", expand=True)
        df_csv = df_csv.rename(columns={'RUTALU': 'RUT'})
        self.df_extra = df_csv
        self.label_file_ref.config(text=f"Refinanciamiento: {os.path.basename(file_path)}")
        messagebox.showinfo("Cargado", f"Refinanciamiento con {len(df_csv)} filas.")


    # ======================================================
    #   MÉTODO: Cargar archivos 1A y 1B
    # ======================================================
    def load_file_1a(self):
        df_csv, file_path = read_any_file("Seleccionar archivo 1A (Reporte 5A)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo 1A no contiene 'RUT'.")
            return
        df_csv["RUT"] = df_csv["RUT"].astype(str).str.strip()
        self.df_csv_1 = df_csv
        self.label_file_1a.config(text=f"Archivo 1A: {os.path.basename(file_path)}")

    def load_file_1b(self):
        df_csv, file_path = read_any_file("Seleccionar archivo 1B (Reporte Solicitud de Monto)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo 1B no contiene 'RUT'.")
            return
        df_csv["RUT"] = df_csv["RUT"].astype(str).str.strip()
        self.df_csv_2 = df_csv
        self.label_file_1b.config(text=f"Archivo 1B: {os.path.basename(file_path)}")


    # ======================================================
    #   MÉTODO: Exportar cruce con Matrícula (1A + 1B)
    # ======================================================
    def export_cruce_con_matricula(self):
        if self.df_csv_1 is None or self.df_csv_2 is None:
            messagebox.showwarning("Faltan archivos", "Por favor, carga 1A y 1B antes de exportar.")
            return

        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return
        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        # Concatena y ajusta RUT
        self.df_csv_1 = self.df_csv_1[self.df_csv_1['IES'] == '013']
        df_concat = pd.merge(self.df_csv_1, self.df_csv_2, how="inner", on='RUT').drop_duplicates()
        df_concat.drop_duplicates(subset='RUT', keep='first', inplace=True)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str).str.strip()
        df_concat["RUT"] = df_concat["RUT"].astype(str).str.strip()
        df_concat = df_concat[['RUT']]
        # Merge con df_licitados
        df_cruce = pd.merge(df_concat, df_licitados, on="RUT", how="inner")
        if df_cruce.empty:
            messagebox.showwarning("Cruce vacío", "No se encontraron coincidencias en el cruce con Matrícula.")
            return

        self.df_result = df_cruce  # Guardamos el resultado
        self._save_df_to_excel(df_cruce, "Cruce_Matricula_1A_1B")

        # Habilitamos el botón "Exportar cruce c/Refinanciamiento"
        self.btn_export_cruce_refinanciamiento.config(
            state="normal", bg="#107FFD"
        )
        messagebox.showinfo(
            "Cruce con Matrícula",
            "Exportado con éxito. Ahora puedes cruzar con Refinanciamiento (1A+1B)."
        )


    # ======================================================
    #   MÉTODO: Exportar cruce con Refinanciamiento (1A+1B)
    # ======================================================
    def export_cruce_con_refinanciamiento(self):
        if self.df_result is None or self.df_result.empty:
            messagebox.showwarning("Sin datos", "Primero haz el cruce con Matrícula (1A+1B).")
            return
        if self.df_extra is None or self.df_extra.empty:
            messagebox.showwarning("Sin datos", "No se ha cargado el Excel de refinanciamiento o está vacío.")
            return
        self.df_result["RUT"] = self.df_result["RUT"].astype(str).str.strip()
        self.df_extra["RUT"] = self.df_extra["RUT"].astype(str).str.strip()
        current_extra = self.df_extra[['RUT', 'DOCUMENTO','SALDO']]
        df_cruce_ref = pd.merge(self.df_result, current_extra, on="RUT", how="inner")
        if df_cruce_ref.empty:
            messagebox.showwarning("Cruce vacío", "No hubo coincidencias con Refinanciamiento.")
            return

        self._save_df_to_excel(df_cruce_ref, "Cruce_Refinanciamiento")
        messagebox.showinfo("Cruce con Refinanciamiento", "Exportado con éxito (1A+1B).")


    # ─────────────────────────────────────────────────────────────────
    #   NUEVO: Cargar RUT (1C) y sus cruces
    # ─────────────────────────────────────────────────────────────────

    def load_file_rut(self):
        """
        Carga el archivo RUT (1C).
        """
        df_csv, file_path = read_any_file("Seleccionar archivo RUT (1C)")
        if df_csv is None:
            return
        if "RUT" not in df_csv.columns:
            messagebox.showerror("Error", "El archivo RUT no contiene la columna 'RUT'.")
            return

        df_csv["RUT"] = df_csv["RUT"].astype(str).str.strip()
        self.df_csv_rut = df_csv

        self.label_file_rut.config(text=f"Archivo RUT: {os.path.basename(file_path)}")
        messagebox.showinfo("Cargado", f"Archivo RUT con {len(df_csv)} filas.")

    def export_cruce_con_matricula_rut(self):
        """
        Cruza el archivo RUT (1C) con df_licitados.
        Luego habilita el botón de refinanciamiento para RUT.
        """
        if self.df_csv_rut is None:
            messagebox.showwarning("Falta archivo RUT", "Primero carga el archivo RUT (1C).")
            return
        global df_licitados
        if df_licitados is None or df_licitados.empty:
            messagebox.showwarning("Sin datos", "df_licitados está vacío.")
            return

        df_licitados["PORCENTAJE_AVANCE"] = df_licitados['PORCENTAJE_AVANCE'].round(0)
        df_licitados["RUT"] = df_licitados["RUT"].astype(str).str.strip()
        self.df_csv_rut["RUT"] = self.df_csv_rut["RUT"].astype(str).str.strip()

        df_cruce_rut = pd.merge(self.df_csv_rut, df_licitados, on="RUT", how="inner")
        if df_cruce_rut.empty:
            messagebox.showwarning("Cruce vacío", "No se encontraron coincidencias (RUT vs df_licitados).")
            return

        self.df_result_rut = df_cruce_rut  # Guardamos el resultado
        self._save_df_to_excel(df_cruce_rut, "Cruce_Matricula_RUT")

        # Habilitamos el botón de refinanciamiento para RUT
        self.btn_export_cruce_refinanciamiento_rut.config(
            state="normal", bg="#107FFD"
        )
        messagebox.showinfo(
            "Cruce con Matrícula (RUT)",
            "Exportado con éxito. Ahora puedes cruzar con Refinanciamiento (RUT)."
        )

    def export_cruce_con_refinanciamiento_rut(self):
        """
        Cruza self.df_result_rut con self.df_extra.
        """
        if self.df_result_rut is None or self.df_result_rut.empty:
            messagebox.showwarning("Sin datos", "Primero haz el cruce con Matrícula (RUT).")
            return
        if self.df_extra is None or self.df_extra.empty:
            messagebox.showwarning("Sin datos", "No se ha cargado el Excel de refinanciamiento o está vacío.")
            return

        self.df_result_rut["RUT"] = self.df_result_rut["RUT"].astype(str).str.strip()
        self.df_extra["RUT"] = self.df_extra["RUT"].astype(str).str.strip()

        df_cruce_rut_ref = pd.merge(self.df_result_rut, self.df_extra, on="RUT", how="inner")
        if df_cruce_rut_ref.empty:
            messagebox.showwarning("Cruce vacío", "No hubo coincidencias con Refinanciamiento (RUT).")
            return

        self._save_df_to_excel(df_cruce_rut_ref, "Cruce_Refinanciamiento_RUT")
        messagebox.showinfo(
            "Cruce con Refinanciamiento (RUT)",
            "Exportado con éxito (RUT)."
        )


    # ======================================================
    #   FUNCIÓN AUXILIAR PARA EXPORTAR A EXCEL
    # ======================================================
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
                messagebox.showinfo("Exportado", f"Guardado en:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar:\n{e}")
        else:
            messagebox.showinfo("Cancelado", "No se exportó el archivo.")


class EgresadosFrame(tk.Frame):
    """
    Sub-proceso Egresados, con soporte para leer CSV/TXT/Excel
    igual que en IngresaRenovantesFrame.
    """
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.config(bg="#FFFFFF")
        
        # Conexión BD y DataFrame principal
        self.connection = connection1  
        self.df_egresados = None
        self.run_query_egresados()

        # DataFrames resultantes de cada “no cruce”
        self.df_egresados_not_found_1 = None
        self.df_egresados_not_found_2 = None
        self.df_egresados_not_found_3 = None
        self.df_egresados_not_found_4 = None

        # Configurar grid
        for row_idx in range(8):
            self.rowconfigure(row_idx, weight=1)
        for col_idx in range(3):
            self.columnconfigure(col_idx, weight=1)

        # Logo (opcional)
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
            tk.Label(self, image=self.logo, bg="#FFFFFF").grid(row=0, column=0, columnspan=3, pady=(10,10))
        except Exception as e:
            print(f"No se pudo cargar la imagen del logo: {e}")
            tk.Label(self, text="[Logo Aquí]", bg="#FFFFFF", fg="#107FFD",
                     font=("Arial", 12, "bold")).grid(row=0, column=0, columnspan=3, pady=(10,10))

        # Título
        tk.Label(
            self, text="Sub-proceso: Egresados",
            font=("Arial", 16, "bold"), bg="#FFFFFF"
        ).grid(row=1, column=0, columnspan=3, pady=(10,10))

        # ---------------------------
        # Fila 2: Cargar #1
        # ---------------------------
        self.btn_cargar_1 = tk.Button(
            self, text="Cargar CSV/TXT/Excel Egresados 5A #1", bg="#107FFD", fg="white",
            command=self.load_file_egresados_1
        )
        self.btn_cargar_1.grid(row=2, column=0, padx=5, pady=5)

        self.label_file_1 = tk.Label(self, text="Sin archivo (#1)", bg="#FFFFFF")
        self.label_file_1.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_1 = tk.Button(
            self, text="Exportar NO Cruce #1", bg="#cccccc", fg="white",
            command=self.export_egresados_1
        )
        self.btn_export_1.grid(row=2, column=2, padx=5, pady=5)

        # ---------------------------
        # Fila 3: Cargar #2
        # ---------------------------
        self.btn_cargar_2 = tk.Button(
            self, text="Cargar CSV/TXT/Excel Egresados 5B #2", bg="#107FFD", fg="white",
            command=self.load_file_egresados_2
        )
        self.btn_cargar_2.grid(row=3, column=0, padx=5, pady=5)

        self.label_file_2 = tk.Label(self, text="Sin archivo (#2)", bg="#FFFFFF")
        self.label_file_2.grid(row=3, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_2 = tk.Button(
            self, text="Exportar NO Cruce #2", bg="#cccccc", fg="white",
            command=self.export_egresados_2
        )
        self.btn_export_2.grid(row=3, column=2, padx=5, pady=5)

        # ---------------------------
        # Fila 4: Cargar #3
        # ---------------------------
        self.btn_cargar_3 = tk.Button(
            self, text="Cargar CSV/TXT/Excel Egresados DESERTORES #3", bg="#107FFD", fg="white",
            command=self.load_file_egresados_3
        )
        self.btn_cargar_3.grid(row=4, column=0, padx=5, pady=5)

        self.label_file_3 = tk.Label(self, text="Sin archivo (#3)", bg="#FFFFFF")
        self.label_file_3.grid(row=4, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_3 = tk.Button(
            self, text="Exportar NO Cruce #3", bg="#cccccc", fg="white",
            command=self.export_egresados_3
        )
        self.btn_export_3.grid(row=4, column=2, padx=5, pady=5)

        # ---------------------------
        # Fila 5: Cargar #4
        # ---------------------------
        self.btn_cargar_4 = tk.Button(
            self, text="Cargar CSV/TXT/Excel para comparar #1,#2,#3", bg="#107FFD", fg="white",
            command=self.load_file_vs_no_cruces
        )
        self.btn_cargar_4.grid(row=5, column=0, padx=5, pady=5)

        self.label_file_4 = tk.Label(self, text="Sin archivo (#4)", bg="#FFFFFF")
        self.label_file_4.grid(row=5, column=1, padx=5, pady=5, sticky="w")

        self.btn_export_4 = tk.Button(
            self, text="Exportar NO Cruce #4", bg="#cccccc", fg="white",
            command=self.export_vs_no_cruces
        )
        self.btn_export_4.grid(row=5, column=2, padx=5, pady=5)

        # ---------------------------
        # Botón Volver (fila 6)
        # ---------------------------
        tk.Button(
            self, text="Volver", bg="#aaaaaa", fg="white",
            command=lambda: controller.show_frame("IngresaFrame")
        ).grid(row=6, column=0, columnspan=3, pady=(20,10))


    # ----------------------------------------------------------
    #  Ejecuta query en la BD y carga self.df_egresados
    # ----------------------------------------------------------
    def run_query_egresados(self):
        query = text("""
                        select
                            b.RUT,
                            c.DV, 
                            a.CODCLI,
                            CODIGO_SIES_COMPLETO,
                            PATERNO, 
                            MATERNO, 
                            NOMBRES, 
                            GENERO, 
                            c.FECH_NAC,
                            DIRECCION,
                            NACIONALIDAD,
                            (SELECT TOP 1 X.CODIGO_CIUDAD FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_CIUDAD,
                            (SELECT TOP 1 X.CODIGO_COMUNA FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_COMUNA, 
                            (SELECT TOP 1 X.CODIGO_REGION FROM dim_territorio_ingresa X WHERE X.CODIGO_COMUNA=I.COD_COMUNA) COD_REGION,
                            c.TELEFONO, 
                            c.MAIL_UNIACC, 
                            FECHA_EGRESO,
                            pm.PERIODO as ANO_COHORTE,
                            pm.PERIODO as ANO_INGRESO_INSTITUCION,
                            d.NOMBRE_CARRERA,
                                1 as 'CODIGO_TIPO_IES',
                                13  as 'CODIGO_DE_IES',
                                j.SEDEN_COD  as 'CODIGO_DE_SEDE',
                                j.CARRN_COD  as 'CODIGO_CARRERA',
                                j.JORNN_COD  as 'CODIGO_JORNADA', 
                                    CASE WHEN j.JORNN_COD=1 THEN 'Diurno' ELSE 'Vespertino/Semipresencial/Online'END AS 'JORNADA',
                            F.ARANCEL_ANUAL AS 'ARANCEL_REAL_ANUAL',
                            0 AS 'ARANCEL DE REFERENCIA',FECHA_MAT -- SE CARGA A MANO POSTERIOR A LA GENERACIÓN DEL ARCHIVO EXCEL-SE INCORPORA FECHA MAT
                        from ft_egreso a
                        left join (select distinct CODCLI, PERIODO from ft_matricula where MAT_N = 1 ) pm on a.codcli = pm.codcli
                        inner join dim_matricula b on a.CODCLI = b.CODCLI
                        inner join dim_alumno c on b.RUT = c.RUT
                        inner join dim_plan_academico d on b.CODPLAN = d.LLAVE_MALLA
                        inner join (select  CODIGO_SIES, ARANCEL_ANUAL,
                                            ROW_NUMBER() over (partition by CODIGO_SIES order by periodo desc )	as numero
                                            from dim_oferta_academica) f  on d.CODIGO_SIES_COMPLETO = f.CODIGO_SIES and numero = 1
                        left join dim_territorio i on c.COMUNA=i.COMUNA
                        left join (select distinct [CODIGO SIES SIN VERSION], SEDEN_COD, CARRN_COD, JORNN_COD, NOMBRE_CARRERA,
                                ROW_NUMBER() over (partition by [CODIGO SIES SIN VERSION] order by[CODIGO SIES SIN VERSION] )	as numero
                                    from oferta_academica_ingresa where carrera_discontinua = 'NO'	) j
                        on left (d.CODIGO_SIES_COMPLETO,LEN (d.CODIGO_SIES_COMPLETO)-2)=j.[CODIGO SIES SIN VERSION]
                        inner join (select CODCLI, FECHA_MAT,    
                                ROW_NUMBER() OVER (partition by CODCLI ORDER BY FECHA_MAT DESC) AS numero
                                from ft_matricula) fm on a.CODCLI = fm.CODCLI and fm.numero = 1
                        where 1 = 1
                        and d.NIVEL_GLOBAL = 'PREGRADO'
                        and CODIGO_SIES_COMPLETO <> '0'		
        """)
        try:
            if self.connection is not None:
                self.df_egresados = pd.read_sql_query(query, self.connection)
                print("Query ejecutada y df_egresados cargado correctamente.")
            else:
                print("No se ejecutó la query: conexión no proporcionada.")
        except Exception as e:
            print(f"Error al ejecutar query: {e}")

    # ----------------------------------------------------------------
    # 1) LOAD #1: Cargar archivo y ver qué RUT NO están en self.df_egresados
    # ----------------------------------------------------------------
    def load_file_egresados_1(self):
        df_loaded, file_path = read_any_file("Seleccionar CSV/TXT/Excel Egresados #1")
        if df_loaded is None:
            return  # usuario canceló o error

        if "RUT" not in df_loaded.columns:
            messagebox.showerror("Error", "El archivo #1 no contiene la columna 'RUT'.")
            return

        self.label_file_1.config(text=f"Archivo #1: {os.path.basename(file_path)}")

        # Nos aseguramos de que existan datos en df_egresados
        if self.df_egresados is None or self.df_egresados.empty:
            messagebox.showwarning("Sin datos", "self.df_egresados está vacío.")
            return

        # Ajustar tipos a string
        df_loaded["RUT"] = df_loaded["RUT"].astype(str)
        self.df_egresados["RUT"] = self.df_egresados["RUT"].astype(str)

        # Merge (left) e identificamos los RUT que NO existen en df_egresados
        df_result = pd.merge(
            df_loaded,
            self.df_egresados[["RUT"]],
            on="RUT",
            how="left",
            indicator=True
        )
        # Filtramos los left_only => no cruzan
        df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

        self.df_egresados_not_found_1 = df_result

        # Cambiamos color del botón export a azul (indica que hay datos)
        self.btn_export_1.config(bg="#107FFD")

        messagebox.showinfo(
            "Cargado",
            f"Archivo #1 cargado. {len(df_result)} filas no cruzan con df_egresados."
        )

    def export_egresados_1(self):
        if self.df_egresados_not_found_1 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#1).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_1, "Egresados_NO_Cruce_1")

    # ----------------------------------------------------------------
    # 2) LOAD #2
    # ----------------------------------------------------------------
    def load_file_egresados_2(self):
        df_loaded, file_path = read_any_file("Seleccionar CSV/TXT/Excel Egresados #2")
        if df_loaded is None:
            return

        if "RUT" not in df_loaded.columns:
            messagebox.showerror("Error", "El archivo #2 no contiene la columna 'RUT'.")
            return

        self.label_file_2.config(text=f"Archivo #2: {os.path.basename(file_path)}")

        if self.df_egresados is None or self.df_egresados.empty:
            messagebox.showwarning("Sin datos", "self.df_egresados está vacío.")
            return

        df_loaded["RUT"] = df_loaded["RUT"].astype(str)
        self.df_egresados["RUT"] = self.df_egresados["RUT"].astype(str)

        df_result = pd.merge(
            df_loaded,
            self.df_egresados[["RUT"]],
            on="RUT",
            how="left",
            indicator=True
        )
        df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

        self.df_egresados_not_found_2 = df_result
        self.btn_export_2.config(bg="#107FFD")

        messagebox.showinfo(
            "Cargado",
            f"Archivo #2 cargado. {len(df_result)} filas no cruzan con df_egresados."
        )

    def export_egresados_2(self):
        if self.df_egresados_not_found_2 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#2).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_2, "Egresados_NO_Cruce_2")

    # ----------------------------------------------------------------
    # 3) LOAD #3
    # ----------------------------------------------------------------
    def load_file_egresados_3(self):
        df_loaded, file_path = read_any_file("Seleccionar CSV/TXT/Excel Egresados #3")
        if df_loaded is None:
            return

        if "RUT" not in df_loaded.columns:
            messagebox.showerror("Error", "El archivo #3 no contiene la columna 'RUT'.")
            return

        self.label_file_3.config(text=f"Archivo #3: {os.path.basename(file_path)}")

        if self.df_egresados is None or self.df_egresados.empty:
            messagebox.showwarning("Sin datos", "self.df_egresados está vacío.")
            return

        df_loaded["RUT"] = df_loaded["RUT"].astype(str)
        self.df_egresados["RUT"] = self.df_egresados["RUT"].astype(str)

        df_result = pd.merge(
            df_loaded,
            self.df_egresados[["RUT"]],
            on="RUT",
            how="left",
            indicator=True
        )
        df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

        self.df_egresados_not_found_3 = df_result
        self.btn_export_3.config(bg="#107FFD")

        messagebox.showinfo(
            "Cargado",
            f"Archivo #3 cargado. {len(df_result)} filas no cruzan con df_egresados."
        )

    def export_egresados_3(self):
        if self.df_egresados_not_found_3 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#3).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_3, "Egresados_NO_Cruce_3")

    # ----------------------------------------------------------------
    # 4) LOAD #4: Comparar vs. NO CRUCES #1, #2, #3
    # ----------------------------------------------------------------
    def load_file_vs_no_cruces(self):
        df_loaded, file_path = read_any_file("Seleccionar CSV/TXT/Excel para comparar #1,#2,#3")
        if df_loaded is None:
            return

        if "RUT" not in df_loaded.columns:
            messagebox.showerror("Error", "El archivo #4 no contiene la columna 'RUT'.")
            return

        self.label_file_4.config(text=f"Archivo #4: {os.path.basename(file_path)}")

        # Juntamos los dataframes de no cruce (1,2,3) que existan
        frames_no_cruce = []
        if self.df_egresados_not_found_1 is not None:
            frames_no_cruce.append(self.df_egresados_not_found_1[["RUT"]])
        if self.df_egresados_not_found_2 is not None:
            frames_no_cruce.append(self.df_egresados_not_found_2[["RUT"]])
        if self.df_egresados_not_found_3 is not None:
            frames_no_cruce.append(self.df_egresados_not_found_3[["RUT"]])

        if not frames_no_cruce:
            messagebox.showinfo("Sin cruces previos", "No hay datos de #1,#2,#3 para comparar.")
            return

        df_no_cruces_union = pd.concat(frames_no_cruce, ignore_index=True).drop_duplicates()

        df_loaded["RUT"] = df_loaded["RUT"].astype(str)
        df_result = pd.merge(
            df_loaded,
            df_no_cruces_union,
            on="RUT",
            how="left",
            indicator=True
        )
        df_result = df_result[df_result["_merge"] == "left_only"].drop(columns="_merge")

        self.df_egresados_not_found_4 = df_result
        self.btn_export_4.config(bg="#107FFD")

        messagebox.showinfo(
            "Cargado",
            f"Archivo #4 cargado. {len(df_result)} filas NO se cruzan con salidas #1,#2,#3."
        )

    def export_vs_no_cruces(self):
        if self.df_egresados_not_found_4 is None:
            messagebox.showwarning("Sin datos", "No hay datos para exportar (#4).")
            return
        self._save_df_to_excel(self.df_egresados_not_found_4, "Egresados_NO_Cruce_4")

    # ----------------------------------------------------------------
    #  FUNCIÓN AUXILIAR PARA EXPORTAR A EXCEL
    # ----------------------------------------------------------------
    def _save_df_to_excel(self, df: pd.DataFrame, default_name: str):
        file_path = filedialog.asksaveasfilename(
            title="Guardar archivo Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            initialfile=f"{default_name}.xlsx"
        )
        if file_path:
            try:
                df.to_excel(file_path, index=False)
                messagebox.showinfo("Exportado", f"Se guardó el archivo en:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo exportar:\n{e}")
        else:
            messagebox.showinfo("Cancelado", "No se exportó el archivo.")

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