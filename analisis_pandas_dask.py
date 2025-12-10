# ============================================
# 1. Instalación de librerías
# ============================================
!pip install dask[dataframe] psutil --quiet

import pandas as pd
import dask.dataframe as dd
import time
import psutil
from dask.diagnostics import ProgressBar

# Activa la barra de progreso de Dask para visualizar el avance de las tareas
ProgressBar().register()

# ============================================
# 2. Ruta del archivo (ajustar si se usa Drive)
# ============================================
# Si el archivo está en Google Drive, cambiar esta ruta por la correspondiente
ruta = "/content/metadata.csv"

# ============================================
# 3. Función para medir memoria
# ============================================
def memoria():
    """
    Retorna la memoria utilizada por el proceso actual en megabytes (MB).
    """
    return psutil.Process().memory_info().rss / (1024**2)

# ============================================
# 4. Lectura del dataset con pandas y Dask
# ============================================
print("=== Lectura con pandas ===")
inicio_pandas = time.time()
mem_before = memoria()

# Lectura del archivo con pandas.
# Se usa engine='python' y on_bad_lines='warn' por posibles líneas corruptas.
df_pandas = pd.read_csv(
    ruta,
    compression=None,
    encoding="latin1",
    engine="python",
    on_bad_lines="warn"
)

mem_after = memoria()
tiempo_pandas = time.time() - inicio_pandas
memoria_pandas = mem_after - mem_before

print(f"Tiempo (s): {tiempo_pandas:.4f}")
print(f"Memoria usada (MB): {memoria_pandas:.2f}")
print("Filas, columnas (pandas):", df_pandas.shape)

print("\nColumnas del dataset (pandas):")
print(df_pandas.columns.tolist())

print("\n=== Lectura con Dask ===")
inicio_dask = time.time()
mem_before = memoria()

# Lectura del mismo archivo con Dask.
# Se fuerzan algunos tipos de columnas a 'object' para evitar errores de conversión.
df_dask = dd.read_csv(
    ruta,
    compression=None,
    encoding="latin1",
    engine="python",
    on_bad_lines="warn",
    assume_missing=True,
    dtype={
        "arxiv_id": "object",
        "who_covidence_id": "object",
        "pubmed_id": "object"
    }
)

mem_after = memoria()
tiempo_dask = time.time() - inicio_dask
memoria_dask = mem_after - mem_before

print(f"Tiempo (s): {tiempo_dask:.4f}")
print(f"Memoria usada (MB): {memoria_dask:.2f}")
print("Columnas del dataset (Dask):")
print(df_dask.columns)

# ============================================
# 5. Creación de la columna publish_year
# ============================================
# Se genera el año de publicación a partir de la columna publish_time.
if "publish_time" in df_pandas.columns:
    print("\n=== Creando columna 'publish_year' a partir de 'publish_time' ===")

    # En pandas la conversión se ejecuta de inmediato sobre todo el DataFrame.
    inicio = time.time()
    df_pandas["publish_year"] = pd.to_datetime(
        df_pandas["publish_time"], errors="coerce"
    ).dt.year
    print(f"Tiempo para crear publish_year (pandas): {time.time() - inicio:.4f} s")

    # En Dask la operación se define de forma diferida y se ejecuta cuando se llama a compute().
    inicio = time.time()
    df_dask["publish_year"] = dd.to_datetime(
        df_dask["publish_time"], errors="coerce"
    ).dt.year
    print(f"Tiempo para crear publish_year (Dask - diferido): {time.time() - inicio:.4f} s")
else:
    print("\n[AVISO] No se encontró la columna 'publish_time'.")
    print("Es necesario ajustar el código para usar otra columna de fecha si corresponde.")

# ============================================
# 6. Experimento 1: Conteo de artículos por año
# ============================================
# Se compara el tiempo y uso de memoria de una agregación simple por año de publicación.
if "publish_year" in df_pandas.columns:
    print("\n=== Agrupación por año (pandas) ===")
    inicio = time.time()
    mem_before = memoria()

    # Agrupación y conteo con pandas
    conteo_anio_pandas = df_pandas.groupby("publish_year").size()

    mem_after = memoria()
    tiempo = time.time() - inicio
    mem_uso = mem_after - mem_before

    print(f"Tiempo (s): {tiempo:.4f}")
    print(f"Memoria usada (MB): {mem_uso:.2f}")
    print("Primeros valores (pandas):")
    print(conteo_anio_pandas.head())

    print("\n=== Agrupación por año (Dask) ===")
    inicio = time.time()
    mem_before = memoria()

    # Agrupación y conteo con Dask.
    # La llamada a compute() dispara la ejecución del grafo de tareas.
    conteo_anio_dask = df_dask.groupby("publish_year").size().compute()

    mem_after = memoria()
    tiempo = time.time() - inicio
    mem_uso = mem_after - mem_before

    print(f"Tiempo (s): {tiempo:.4f}")
    print(f"Memoria usada (MB): {mem_uso:.2f}")
    print("Primeros valores (Dask):")
    print(conteo_anio_dask.head())
else:
    print("\n[AVISO] No se pudo realizar la agrupación por año porque 'publish_year' no existe.")

# ============================================
# 7. Experimento 2: Top 10 journals
# ============================================
# Se calcula el Top 10 de revistas según la cantidad de artículos.
if "journal" in df_pandas.columns:
    print("\n=== Top 10 journals (pandas) ===")
    inicio = time.time()
    mem_before = memoria()

    # Cálculo de frecuencias con pandas
    top_journals_pandas = df_pandas["journal"].value_counts().head(10)

    mem_after = memoria()
    tiempo = time.time() - inicio
    mem_uso = mem_after - mem_before

    print(f"Tiempo (s): {tiempo:.4f}")
    print(f"Memoria usada (MB): {mem_uso:.2f}")
    print(top_journals_pandas)

    print("\n=== Top 10 journals (Dask) ===")
    inicio = time.time()
    mem_before = memoria()

    # En Dask primero se computa value_counts() y luego se seleccionan los 10 primeros.
    top_journals_dask = df_dask["journal"].value_counts().compute().head(10)

    mem_after = memoria()
    tiempo = time.time() - inicio
    mem_uso = mem_after - mem_before

    print(f"Tiempo (s): {tiempo:.4f}")
    print(f"Memoria usada (MB): {mem_uso:.2f}")
    print(top_journals_dask)
else:
    print("\n[AVISO] No se encontró la columna 'journal'.")
    print("Si se requiere este análisis, se debe seleccionar otra columna categórica.")
