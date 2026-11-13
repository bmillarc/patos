#!/usr/bin/env bash
# ==============================================================================
# SCRIPT DE EJECUCIÓN EN EL CLÚSTER DEL CURSO (cm.dcc.uchile.cl) - GRUPO 4
# ==============================================================================
# Uso:
#   ./run_analysis.sh [INPUT] [OUTPUT_GLOBAL] [OUTPUT_BY_GAME]
#
#   Sin argumentos: procesa el dataset completo en /uhadoop2026/projects/grupo4
#   y escribe en output_steam_polarity_global / output_steam_polarity_by_game
#   (se BORRAN si ya existen).
#
#   Para validar con la muestra SIN tocar los resultados oficiales:
#     ./run_analysis.sh /uhadoop2026/projects/grupo4/sample/sample_reviews.csv \
#                        /uhadoop2026/projects/grupo4/test_output_global \
#                        /uhadoop2026/projects/grupo4/test_output_by_game
# ==============================================================================
set -e

# Entorno del clúster del curso (no suele estar en el PATH por defecto)
export HADOOP_HOME=/data/hadoop/hadoop-2.10.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/data/hadoop/pig-0.18.0/bin:$PATH

# Rutas por defecto (HDFS del clúster compartido)
HDFS_INPUT_DEFAULT="/uhadoop2026/projects/grupo4/data/steam_reviews.csv.bz2"
HDFS_OUTPUT_GLOBAL="/uhadoop2026/projects/grupo4/output_steam_polarity_global"
HDFS_OUTPUT_BY_GAME="/uhadoop2026/projects/grupo4/output_steam_polarity_by_game"

INPUT_PATH=${1:-$HDFS_INPUT_DEFAULT}
OUTPUT_GLOBAL=${2:-$HDFS_OUTPUT_GLOBAL}
OUTPUT_BY_GAME=${3:-$HDFS_OUTPUT_BY_GAME}

echo "================================================================="
echo "   PROCESAMIENTO MASIVO DE DATOS - STEAM REVIEWS CON APACHE PIG   "
echo "================================================================="
echo "Entrada HDFS : $INPUT_PATH"
echo "Salida Global: $OUTPUT_GLOBAL"
echo "Salida x Juego: $OUTPUT_BY_GAME"
echo "================================================================="
echo "[AVISO] Se borrarán las carpetas de salida listadas si existen."
echo "        Para pruebas use rutas de test (ver encabezado del script)."
echo "================================================================="

# 1. Limpiar directorios de salida previos si existen
echo "[1/3] Limpiando salidas anteriores en HDFS si existen..."
hdfs dfs -rm -r -f "$OUTPUT_GLOBAL" || true
hdfs dfs -rm -r -f "$OUTPUT_BY_GAME" || true

# 2. Ejecutar consulta global (Polaridad por rango de horas a nivel general)
echo "[2/3] Ejecutando análisis GLOBAL (steam_polarity_global.pig)..."
pig -x mapreduce -param INPUT="$INPUT_PATH" \
    -param OUTPUT="$OUTPUT_GLOBAL" \
    steam_polarity_global.pig

echo "Análisis global completado exitosamente."

# 3. Ejecutar consulta segmentada por juego (appid) y rango
echo "[3/3] Ejecutando análisis POR JUEGO (steam_polarity_by_game.pig)..."
pig -x mapreduce -param INPUT="$INPUT_PATH" \
    -param OUTPUT="$OUTPUT_BY_GAME" \
    steam_polarity_by_game.pig

echo "================================================================="
echo "¡TRABAJO FINALIZADO CON ÉXITO!"
echo "================================================================="
echo "Para ver los resultados globales:"
echo "   hdfs dfs -cat $OUTPUT_GLOBAL/part-*"
echo ""
echo "Para ver una muestra de los resultados por juego:"
echo "   hdfs dfs -cat $OUTPUT_BY_GAME/part-* | head -n 30"
echo "================================================================="
