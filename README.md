# Análisis de Polaridad en Steam según Tiempo de Juego (Apache Pig)

Este repositorio contiene los scripts Pig Latin y de automatización para procesar el dataset masivo de **Steam Reviews** (~100 Millones de filas) sobre **Hadoop / MapReduce**.

---

## 🎯 Objetivo del Análisis

Evaluar la relación entre el **tiempo acumulado de juego** y la **polaridad de las reseñas** (voto positivo vs. negativo) para responder:
- ¿Los jugadores con más horas (veteranos `200h+`) tienden a dejar reseñas más críticas/negativas en comparación con jugadores nuevos (`0-10h`)?
- ¿Cómo varía este fenómeno a nivel global y juego por juego (`app_id`)?

### Segmentación de Rangos de Tiempo
Dado que la API de Steam entrega el tiempo en **minutos**, se segmenta de la siguiente manera:
| Rango de Horas | Minutos Equivalentes | Etiqueta |
|---|---|---|
| **0h a 10h** | `0 <= minutos < 600` | `1. 0-10h` |
| **10h a 50h** | `600 <= minutos < 3000` | `2. 10-50h` |
| **50h a 200h** | `3000 <= minutos < 12000` | `3. 50-200h` |
| **200h+** | `minutos >= 12000` | `4. 200h+` |

---

## 📁 Archivos Incluidos

- [steam_polarity_global.pig](file:///c:/Users/benja/Documents/antigravity/silly-salk/steam_polarity_global.pig): Consulta Pig Latin para calcular la tasa de recomendación global por cada rango de tiempo.
- [steam_polarity_by_game.pig](file:///c:/Users/benja/Documents/antigravity/silly-salk/steam_polarity_by_game.pig): Consulta Pig Latin para calcular la tasa por cada juego (`app_id`) y rango de horas.
- [run_analysis.sh](file:///c:/Users/benja/Documents/antigravity/silly-salk/run_analysis.sh): Script Bash para lanzar los trabajos en el cluster HDFS con parámetros de entrada y salida.
- [sample_reviews.csv](file:///c:/Users/benja/Documents/antigravity/silly-salk/sample_reviews.csv): Archivo de muestra pequeño para validar sintaxis y lógica en modo local (`pig -x local`).

---

## ⚙️ Pasos Previos en el Cluster: Verificación del Esquema

Como el dataset está comprimido en `.bz2` en HDFS, **Hadoop lo descomprime y divide en bloques en paralelo de forma nativa** (bzip2 es un formato divisible / splittable).

Antes de lanzar el trabajo con 100M de registros, verifique la cabecera y el orden exacto de columnas de su archivo:

```bash
# Ver las primeras 2 líneas del archivo en HDFS
hdfs dfs -text /ruta/en/hdfs/archivo.bz2 | head -n 2
```

Si el orden de columnas difiere de:
`app_id, review_id, review_text, voted_up, author_playtime_forever, author_playtime_at_review`

Simplemente edite la cláusula `LOAD ... AS (...)` en los archivos `.pig` para que coincida con las columnas del CSV.

---

## 🚀 Ejecución en el Cluster Hadoop

### Opción 1: Usando el script automatizado
```bash
chmod +x run_analysis.sh
./run_analysis.sh /ruta/en/hdfs/archivo.bz2
```

### Opción 2: Ejecución manual directa con Pig
```bash
# 1. Consulta Global
pig -param INPUT='/ruta/en/hdfs/archivo.bz2' \
    -param OUTPUT='/user/mi_usuario/output_global' \
    steam_polarity_global.pig

# 2. Consulta por Juego
pig -param INPUT='/ruta/en/hdfs/archivo.bz2' \
    -param OUTPUT='/user/mi_usuario/output_por_juego' \
    steam_polarity_by_game.pig
```

---

## 🧪 Prueba Rápida en Modo Local (Opcional)

Si desea probar el script con el archivo de muestra antes de procesar los 100M:

```bash
pig -x local -param INPUT='sample_reviews.csv' -param OUTPUT='output_local_test' steam_polarity_global.pig
cat output_local_test/part*
```

---

## ⚡ Optimizaciones para 100 Millones de Filas

1. **Paralelismo (`PARALLEL`)**: Los scripts incluyen `SET default_parallel 10;` (o 20). Ajústelo según el número de núcleos (vCores) disponibles en su cluster.
2. **Combiner de Pig**: Las operaciones `COUNT` y `SUM` aprovechan automáticamente la interfaz `Algebraic` de Pig, reduciendo el tráfico entre Map y Reduce de forma masiva.
3. **Muestras Estadísticas**: El script `steam_polarity_by_game.pig` descarta grupos con menos de 20 reseñas (`FILTER ... BY total_reviews >= 20`) para evitar sesgos con juegos poco populares.
