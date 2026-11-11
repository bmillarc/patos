#!/usr/bin/env bash
# ==============================================================================
# SCRIPT DE EJECUCIÓN EN CLUSTER HADOOP / PIG
# ==============================================================================
set -e

# Configuración por defecto (modificar según la estructura de su cluster)
HDFS_INPUT_DEFAULT="/user/$USER/steam_data/steam_reviews.csv.bz2"
HDFS_OUTPUT_GLOBAL="/user/$USER/output_steam_polarity_global"
HDFS_OUTPUT_BY_GAME="/user/$USER/output_steam_polarity_by_game"

INPUT_PATH=${1:-$HDFS_INPUT_DEFAULT}

echo "================================================================="
echo "   PROCESAMIENTO MASIVO DE DATOS - STEAM REVIEWS CON APACHE PIG   "
echo "================================================================="
echo "Entrada HDFS : $INPUT_PATH"
echo "Salida Global: $HDFS_OUTPUT_GLOBAL"
echo "Salida x Juego: $HDFS_OUTPUT_BY_GAME"
echo "================================================================="

# 1. Limpiar directorios de salida previos si existen
echo "[1/3] Limpiando salidas anteriores en HDFS si existen..."
hdfs dfs -rm -r -f "$HDFS_OUTPUT_GLOBAL" || true
hdfs dfs -rm -r -f "$HDFS_OUTPUT_BY_GAME" || true

# 2. Ejecutar consulta global (Polaridad por rango de horas a nivel general)
echo "[2/3] Ejecutando análisis GLOBAL (steam_polarity_global.pig)..."
pig -param INPUT="$INPUT_PATH" \
    -param OUTPUT="$HDFS_OUTPUT_GLOBAL" \
    steam_polarity_global.pig

echo "Análisis global completado exitosamente."

# 3. Ejecutar consulta segmentada por juego (app_id) y rango
echo "[3/3] Ejecutando análisis POR JUEGO (steam_polarity_by_game.pig)..."
pig -param INPUT="$INPUT_PATH" \
    -param OUTPUT="$HDFS_OUTPUT_BY_GAME" \
    steam_polarity_by_game.pig

echo "================================================================="
echo "¡TRABAJO FINALIZADO CON ÉXITO!"
echo "================================================================="
echo "Para ver una muestra de los resultados globales:"
echo "   hdfs dfs -cat $HDFS_OUTPUT_GLOBAL/part* | head -n 20"
echo ""
echo "Para ver una muestra de los resultados por juego:"
echo "   hdfs dfs -cat $HDFS_OUTPUT_BY_GAME/part* | head -n 30"
echo "================================================================="
