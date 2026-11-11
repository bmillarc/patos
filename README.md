# Análisis de Polaridad en Steam según Tiempo de Juego (Apache Pig)

Grupo 4 — CC5212 Procesamiento Masivo de Datos.

Este repositorio contiene los scripts Pig Latin y de automatización para procesar el dataset masivo de **Steam Reviews** (~100 Millones de filas) sobre el clúster Hadoop compartido del curso (5 máquinas, `cm.dcc.uchile.cl`).

---

## 🎯 Objetivo del Análisis

Evaluar la relación entre el **tiempo acumulado de juego** y la **polaridad de las reseñas** (recomendación positiva vs. negativa) para responder:
- ¿Los jugadores con más horas (veteranos `200h+`) tienden a dejar reseñas más críticas/negativas en comparación con jugadores nuevos (`0-10h`)?
- ¿Cómo varía este fenómeno a nivel global y juego por juego (`appid`)?

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

- steam_polarity_global.pig: Consulta Pig Latin para calcular la tasa de recomendación global por cada rango de tiempo.
- steam_polarity_by_game.pig: Consulta Pig Latin para calcular la tasa por cada juego (`appid`) y rango de horas.
- run_analysis.sh: Script Bash para lanzar los trabajos en el cluster HDFS con parámetros de entrada y salida.
- sample_reviews.csv: Muestra pequeña (filas reales del dataset) para validar sintaxis y lógica en modo local (`pig -x local`).

---

## 📦 Datos en HDFS (verificado el 11-nov-2026)

El dataset completo está en HDFS en:

```
/uhadoop2026/projects/grupo4/sample/sample_reviews.csv  (100.000 filas, ~20 MB, todas appid=10)
/uhadoop2026/projects/grupo4/data/steam_reviews.csv.bz2  (11 GB comprimido; se descomprime y divide nativamente en paralelo)
```

**Esquema real del CSV** (verificado con las primeras líneas del archivo; cabecera incluida en el encabezado):

```
recommendationid,appid,game,author_steamid,author_num_games_owned,
author_num_reviews,author_playtime_forever,author_playtime_last_two_weeks,
author_playtime_at_review,author_last_played,language,review,
timestamp_created,timestamp_updated,voted_up,votes_up,votes_funny,
weighted_vote_score,comment_count,steam_purchase,received_for_free,
written_during_early_access,hidden_in_steam_china,steam_china_location
```

Notas importantes (la primera versión de los scripts asumía un esquema que no coincidía; ya fue corregido):

1. **`voted_up` viene como entero `0`/`1`** (no como `True`/`False`).
2. **El campo `review` es texto libre entre comillas**: puede traer comas y comillas escapadas (`""`). Por lo tanto **no se puede usar `PigStorage(',')`**. Los scripts usan `CSVExcelStorage` de piggybank:
   ```
   REGISTER /data/hadoop/spark-2.1.0-bin-hadoop2.3.old/pig-0.14.0/lib/piggybank.jar;
   ```
   (piggybank 0.14 incluido en el clúster funciona con Pig 0.18 para este caso; verificado con 200 líneas reales.)
3. La cabecera no castea a `int`/`long`, por lo que `appid` queda `NULL` y los `FILTER` la descartan automáticamente.
4. El campo `review` no se utiliza en este análisis y se descarta en la proyección.
5. `author_playtime_forever` = minutos acumulados **de toda la vida** en el juego (lifetime). El análisis usa este campo (los rangos del objetivo). `author_playtime_at_review` (minutos al momento de escribir la reseña) queda disponible en la carga si luego se quieren comparar.
6. `bzip2` es splittable: cada mapper procesa un tramo del archivo descomprimido en paralelo.
7. La muestra (`sample_reviews.csv`) contiene **solo reseñas de appid=10 (Counter-Strike)**, útil como prueba de humo, no como resultado.

---

## ⚙️ Ejecución en el Clúster

Las rutas por defecto ya apuntan a `grupo4`. `run_analysis.sh` exporta el entorno (Hadoop 2.10.0 en `/data/hadoop/hadoop-2.10.0`, Pig 0.18 en `/data/hadoop/pig-0.18.0`, Java 8), que **no está en el PATH por defecto** (`pig: command not found` si se llama directo):

```bash
cd <carpeta_con_scripts>
chmod +x run_analysis.sh
./run_analysis.sh
```

### Opción manual
```bash
export HADOOP_HOME=/data/hadoop/hadoop-2.10.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/data/hadoop/pig-0.18.0/bin:$PATH

# 1. Consulta Global
pig -x mapreduce -param INPUT='/uhadoop2026/projects/grupo4/data/steam_reviews.csv.bz2' \
    -param OUTPUT='/uhadoop2026/projects/grupo4/output_steam_polarity_global' \
    steam_polarity_global.pig

# 2. Consulta por Juego
pig -x mapreduce -param INPUT='/uhadoop2026/projects/grupo4/data/steam_reviews.csv.bz2' \
    -param OUTPUT='/uhadoop2026/projects/grupo4/output_steam_polarity_by_game' \
    steam_polarity_by_game.pig
```

### Prueba de humo con la muestra (100.000 filas, appid=10)
```bash
hdfs dfs -rm -r -f /uhadoop2026/projects/grupo4/test_output_global
hdfs dfs -rm -r -f /uhadoop2026/projects/grupo4/test_output_by_game

pig -x mapreduce -param INPUT='/uhadoop2026/projects/grupo4/sample/sample_reviews.csv' \
    -param OUTPUT='/uhadoop2026/projects/grupo4/test_output_global' \
    steam_polarity_global.pig
pig -x mapreduce -param INPUT='/uhadoop2026/projects/grupo4/sample/sample_reviews.csv' \
    -param OUTPUT='/uhadoop2026/projects/grupo4/test_output_by_game' \
    steam_polarity_by_game.pig

hdfs dfs -cat /uhadoop2026/projects/grupo4/test_output_global/part-*
hdfs dfs -cat /uhadoop2026/projects/grupo4/test_output_by_game/part-*
```

Resultado esperado en la muestra (appid=10, ~97% positivas): los 4 rangos con totales y porcentajes cercanos al 97%.

---

## ⚡ Optimizaciones para 100+ Millones de Filas

1. **Paralelismo (`PARALLEL`)**: Los scripts incluyen `SET default_parallel 10;` (global) y `20` (por juego). Ajústelo según vCores disponibles; el clúster es compartido.
2. **Combiner de Pig**: Las operaciones `COUNT` y `SUM` aprovechan automáticamente la interfaz `Algebraic` de Pig, reduciendo el tráfico entre Map y Reduce.
3. **Muestras Estadísticas**: El script `steam_polarity_by_game.pig` descarta grupos con menos de 20 reseñas (`FILTER ... BY total_reviews >= 20`) para evitar sesgos con juegos poco populares.
4. **Solo se proyectan 4 campos por fila** (`appid, game, voted_up, author_playtime_forever`); el resto se descarta antes del shuffle.

---

## 📊 Después del job: responder la pregunta

1. `hdfs dfs -cat <output_global>/part-*` → ¿la proporción de positivas cae al subir las horas? (responder §Objetivo)
2. `hdfs dfs -cat <output_by_game>/part-*` → identificar juegos donde los veteranos son más críticos (porcentajes por rango y volumen por segmento).
3. Copiar los part-* al repo (`results/`) y graficar (Python/Excel) para el informe: barras de `positive_percentage` por rango (global) y comparaciones por juego.
