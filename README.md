**README.md**

---

## 📚 Resumen y conversión de `app_v36.py`

`app_v36.py` es una aplicación Tkinter de más de 5.000 líneas que organiza su interfaz en distintos *frames* y contiene utilidades para leer archivos, consultar SQL Server y exportar datos.
Para modernizar la experiencia y simplificar el mantenimiento se migró a Streamlit dividiendo cada *frame* en una página independiente.

### Principales cambios

1. **Arquitectura multipágina**  
   - Tkinter → Streamlit. Cada `Frame` se convirtió en un archivo dentro de `pages/`.
   - Se crea un `Home.py` que funciona como punto de entrada.
2. **Helpers reutilizables**  
   - Conexiones a la base de datos, lectura de archivos y estilos en `helpers/`.
   - Uso de `@st.cache_data` para acelerar lecturas y consultas.
3. **Carga de archivos**  
   - `filedialog` se reemplazó con `st.file_uploader` y mensajes con `st.toast`.
4. **Empaquetado**  
   - `run_app.py` para lanzar `streamlit run Home.py`.
   - Instrucciones de PyInstaller e Inno Setup.

---

## 🗂️ Estructura del proyecto final

```
.
├── Home.py
├── run_app.py
├── requirements.txt
├── style.css
├── helpers
│   ├── __init__.py
│   ├── db.py
│   ├── file_reader.py
│   └── style.py
└── pages
    ├── 1_MainMenu.py
    ├── 2_Ingresa.py
    ├── 3_FUAS.py
    ├── 4_Licitados.py
    ├── 5_Seguimientos.py
    ├── 6_IngresaRenovantes.py
    ├── 7_Egresados.py
    ├── 8_Validaciones.py
    └── 9_Becas.py
```

---

## 📝 Contenido de cada archivo

### `Home.py`

```python
import streamlit as st
from helpers.style import local_css

st.set_page_config(page_title="UNIACC", page_icon="🎓", layout="wide")
local_css("style.css")

if "auth" not in st.session_state:
    st.session_state.auth = False

if not st.session_state.auth:
    st.title("Login de Usuario")
    user = st.text_input("Usuario")
    passwd = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        if user == "admin" and passwd == "12345":
            st.session_state.auth = True
            st.toast("Bienvenido/a " + user, icon="✅")
        else:
            st.error("Usuario o contraseña inválidos")
    st.stop()

st.success("Autenticado correctamente.")
st.page_link("pages/1_MainMenu.py", label="Ir al menú principal")
```

### `run_app.py`

```python
import subprocess
import sys
subprocess.run([sys.executable, "-m", "streamlit", "run", "Home.py"])
```

### `requirements.txt`

```
streamlit
pandas
sqlalchemy
pyodbc
```

### `style.css`

```
body { background-color: #f5f5f5; }
.stButton>button { background-color: #107FFD; color: white; }
```

### `helpers/__init__.py`

```python
from .db import get_connection
from .file_reader import read_any_file
from .style import local_css
```

### `helpers/db.py`

```python
from sqlalchemy import create_engine
import streamlit as st

@st.cache_data(show_spinner=False)
def get_connection(server, database, user, password, driver="ODBC Driver 17 for SQL Server"):
    conn_str = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}"
    engine = create_engine(conn_str, fast_executemany=False)
    return engine.connect()
```

### `helpers/file_reader.py`

```python
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
```

### `helpers/style.py`

```python
import streamlit as st
from pathlib import Path


def local_css(file_name: str):
    path = Path(file_name)
    if path.exists():
        st.markdown(f"<style>{path.read_text()}</style>", unsafe_allow_html=True)
```

### `pages/1_MainMenu.py`

```python
import streamlit as st
from helpers.style import local_css

st.set_page_config(page_title="Menú Principal", page_icon="🏠")
local_css("../style.css")

st.title("Menú Principal")

st.page_link("pages/2_Ingresa.py", label="Ir a Ingresa", icon="➡️")
st.page_link("pages/8_Validaciones.py", label="Ir a Validaciones", icon="➡️")
st.page_link("pages/9_Becas.py", label="Ir a Becas", icon="➡️")
```

### `pages/2_Ingresa.py`

```python
import streamlit as st

st.set_page_config(page_title="Ingresa", page_icon="📝")

st.title("Subprocesos de Ingresa")
st.page_link("pages/3_FUAS.py", label="FUAS", icon="📄")
st.page_link("pages/4_Licitados.py", label="Licitados", icon="📄")
st.page_link("pages/5_Seguimientos.py", label="Seguimiento Firmas", icon="📄")
st.page_link("pages/6_IngresaRenovantes.py", label="Renovantes", icon="📄")
st.page_link("pages/7_Egresados.py", label="Egresados", icon="📄")
```

### `pages/3_FUAS.py`

```python
import streamlit as st
from helpers import get_connection, read_any_file

st.set_page_config(page_title="FUAS", page_icon="📄")
conn = get_connection(
    server="PUACSCLBI.uniacc.local",
    database="DWH_DAI",
    user="usr_dwhppto",
    password="g8)yT1m23u7H",
)

st.title("Sub-proceso: FUAS")

uploaded = st.file_uploader("Cargar FUAS", type=["csv", "txt", "xlsx"])
df = read_any_file(uploaded)
if df is not None:
    st.success("Archivo cargado")
    st.dataframe(df.head())

    if st.button("Exportar FUAS"):
        df.to_excel("fuas_export.xlsx", index=False)
        st.toast("FUAS exportado", icon="📁")
```

### `pages/4_Licitados.py`

```python
import streamlit as st
from helpers.file_reader import read_any_file

st.set_page_config(page_title="Licitados", page_icon="📑")

st.title("Licitados")

uploaded = st.file_uploader("Cargar archivo de licitados", type=["csv", "xlsx"])
df = read_any_file(uploaded)
if df is not None:
    st.success("Archivo cargado")
    st.dataframe(df.head())
```

### `pages/5_Seguimientos.py`

```python
import streamlit as st

st.set_page_config(page_title="Seguimientos", page_icon="🔎")
st.title("Seguimiento de Firmas")

st.info("TODO: implementar funcionalidades de seguimiento")
```

### `pages/6_IngresaRenovantes.py`

```python
import streamlit as st

st.set_page_config(page_title="Renovantes", page_icon="♻️")
st.title("Renovantes (Ingresa)")

st.info("TODO: implementar renovantes")
```

### `pages/7_Egresados.py`

```python
import streamlit as st

st.set_page_config(page_title="Egresados", page_icon="🎓")
st.title("Egresados")

st.info("TODO: implementar lógica de egresados")
```

### `pages/8_Validaciones.py`

```python
import streamlit as st

st.set_page_config(page_title="Validaciones", page_icon="✅")
st.title("Validaciones Previas")

st.info("TODO: implementar validaciones")
```

### `pages/9_Becas.py`

```python
import streamlit as st

st.set_page_config(page_title="Becas", page_icon="💰")
st.title("Becas")

st.info("TODO: implementar funcionalidades de becas")
```

---

## ⚙️ Packaging

### 1. Wrapper `run_app.py`

```python
import subprocess, sys
subprocess.run([sys.executable, "-m", "streamlit", "run", "Home.py"])
```

### 2. Compilar con PyInstaller

```
pyinstaller run_app.py --onefile --add-data "style.css;." \
    --add-data "pages;pages" --add-data "helpers;helpers"
```

- `--onefile`: genera un único ejecutable.
- `--add-data`: incluye carpetas necesarias (CSS, páginas y helpers).

### 3. Script `installer.iss` (Inno Setup)

```ini
; installer.iss
[Setup]
AppName=UNIACC Streamlit
AppVersion=1.0
DefaultDirName={pf}\UNIACC_Streamlit
OutputDir=dist
OutputBaseFilename=UNIACC_Setup
Compression=lzma
SolidCompression=yes

[Files]
Source: "dist\run_app.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\UNIACC"; Filename: "{app}\run_app.exe"

[Run]
Filename: "{app}\run_app.exe"; Description: "Lanzar aplicación"; Flags: nowait postinstall skipifsilent
```

### 4. Checklist de problemas comunes

- **hidden-import**: si PyInstaller no detecta algún módulo dinámico, usar `--hidden-import`.
- **firewall**: Streamlit abre un puerto local; algunos firewalls pueden mostrar advertencias.
- **tamaño > 2 GB**: el ejecutable puede crecer mucho. Considerar `--onedir` si supera 2 GB.

---

## 🚀 Cómo ejecutar

1. Instalar dependencias:

   ```bash
   pip install -r requirements.txt
   ```

2. Ejecutar en desarrollo:

   ```bash
   streamlit run Home.py
   ```

3. Crear el `.exe`:

   ```bash
   pyinstaller run_app.py --onefile --add-data "style.css;." \
       --add-data "pages;pages" --add-data "helpers;helpers"
   ```

4. Generar instalador con Inno Setup usando `installer.iss`.

---

## 📑 Glosario (español)

- **Helpers**: módulos reutilizables con funciones de apoyo.
- **st.cache_data**: decorador para almacenar resultados y acelerar la aplicación.
- **PyInstaller**: empaqueta aplicaciones Python en ejecutables.
- **Inno Setup**: software para crear instaladores.

---

¡Con esta guía podrás ejecutar la nueva app de Streamlit, crear un `.exe` y armar un instalador amigable!

