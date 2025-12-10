# Universidad LEAD – 2025
# Curso: Computación Paralela y Distribuida
# Estudiantes: Justin López - Oscar Umaña

# Análisis Comparativo de pandas y Dask para Datos a Gran Escala (CORD-19)

## Descripción del Proyecto

Este proyecto evalúa el desempeño de las bibliotecas **pandas** y **Dask** en el procesamiento del dataset **CORD-19**, un repositorio masivo de publicaciones científicas relacionadas con COVID-19. El objetivo principal es comparar su eficiencia en tareas de lectura, transformación y agregación de datos a gran escala, considerando tiempos de ejecución y uso de memoria.
---

## Estructura del Repositorio

```
├── analisis_pandas_dask.py       # Script principal con todos los experimentos
├── resultados/                   # Figuras y archivos generados
│   ├── fig1_tiempo_lectura.png
│   ├── fig2_memoria_lectura.png
│   ├── fig3_tiempos_operaciones.png
├── data/
│   └── metadata.csv              # Dataset CORD-19 (no se incluye por tamaño)
├── README.md                     # Instrucciones y documentación del proyecto
└── informe.pdf                # Paper final en formato IEEE
```

---

## Dependencias

El proyecto fue ejecutado en **Google Colab** con:

- Python 3.x  
- pandas  
- Dask (`dask[dataframe]`)  
- psutil  
- matplotlib  

Instalación de librerías:

```bash
pip install dask[dataframe] psutil matplotlib
```

---

## Instrucciones para Ejecutar el Código

1. Abrir **Google Colab** o un entorno Python equivalente.
2. Cargar el archivo `analisis_pandas_dask.py`.
3. Verificar la ruta del dataset:

```python
ruta = "/content/metadata.csv"
```

Si usa Google Drive, montarlo:

```python
from google.colab import drive
drive.mount("/content/drive")
ruta = "/content/drive/MyDrive/metadata.csv"
```

4. Ejecutar el script completo, el cual realiza:

- Lectura del dataset con pandas y Dask  
- Medición de tiempo y memoria  
- Creación de `publish_year`  
- Agrupación por año  
- Cálculo del Top 10 journals  
- Generación de figuras  
- Impresión de resultados en consola  

---

## Resultados Generados

Las principales figuras generadas son:

- `fig1_tiempo_lectura.png`  
- `fig2_memoria_lectura.png`  
- `fig3_tiempos_operaciones.png`  

Se almacenan en la carpeta `resultados/`.

---

## Consideraciones Importantes

- El dataset CORD-19 supera los límites de memoria de equipos convencionales.  
- Dask utiliza evaluación diferida, lo cual explica algunos tiempos reducidos en ciertas operaciones.  
- Los resultados pueden variar según el hardware disponible.  

---