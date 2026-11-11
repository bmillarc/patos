-- ==============================================================================
-- PROYECTO: Análisis de polaridad de recomendaciones en Steam según tiempo de juego
-- SCRIPT: steam_polarity_global.pig
-- OBJETIVO: Calcular la tasa de recomendación positiva (polaridad) por rango de
--           tiempo jugado a nivel global en toda la plataforma.
-- ==============================================================================

-- 1. Optimizaciones de ejecución en Hadoop MapReduce
SET default_parallel 10;                     -- Ajustar según capacidad del cluster (vCores/Reducers)
SET pig.exec.reducers.bytes.per.reducer 134217728; -- 128 MB por reducer para balancear carga
SET mapreduce.map.speculative false;
SET mapreduce.reduce.speculative false;

-- 2. Carga de datos desde HDFS (.bz2 es descomprimido y dividido nativamente por Hadoop)
-- Nota: Si el archivo es CSV estándar delimitado por coma:
-- IMPORTANTE: Ajustar los índices o nombres según el archivo exacto.
-- Si contiene comas dentro del review_text, se recomienda CSVExcelStorage de Piggybank,
-- o cargar únicamente las columnas necesarias.

raw_data = LOAD '$INPUT' USING PigStorage(',') AS (
    app_id: chararray,
    review_id: chararray,
    review_text: chararray,
    voted_up: chararray,
    author_playtime_forever: int,
    author_playtime_at_review: int
);

-- 3. Limpieza y filtrado de datos:
-- Descartar cabeceras, registros nulos o tiempos negativos/inconsistentes
cleaned_data = FILTER raw_data BY 
    (app_id IS NOT NULL AND app_id != 'app_id' AND app_id != 'appid') AND
    (voted_up IS NOT NULL) AND
    (author_playtime_forever IS NOT NULL AND author_playtime_forever >= 0);

-- 4. Transformación:
-- a) Convertir voted_up ('True'/'1'/'true') a valor binario (1 = Positivo, 0 = Negativo)
-- b) Segmentación de usuarios por rango de horas jugadas:
--    - 0 a 10 horas   (0 - 600 min)
--    - 10 a 50 horas  (600 - 3000 min)
--    - 50 a 200 horas (3000 - 12000 min)
--    - 200+ horas     (> 12000 min)
projected_data = FOREACH cleaned_data GENERATE
    app_id,
    author_playtime_forever,
    ((LOWER(voted_up) == 'true' OR voted_up == '1') ? 1 : 0) AS is_positive:int,
    (
        author_playtime_forever < 600 ? '1. 0-10h' :
        (author_playtime_forever < 3000 ? '2. 10-50h' :
        (author_playtime_forever < 12000 ? '3. 50-200h' : '4. 200h+'))
    ) AS playtime_range:chararray;

-- 5. Agrupación por Rango de Horas Jugadas
grouped_by_range = GROUP projected_data BY playtime_range;

-- 6. Agregación y Métricas de Polaridad
-- Total reseñas, total positivas, total negativas, y porcentaje de recomendación positiva
range_metrics = FOREACH grouped_by_range {
    total_reviews = COUNT(projected_data);
    pos_reviews = SUM(projected_data.is_positive);
    GENERATE
        group AS playtime_range,
        total_reviews AS total_reviews,
        pos_reviews AS positive_reviews,
        (total_reviews - pos_reviews) AS negative_reviews,
        ROUND_TO(((double)pos_reviews / (double)total_reviews) * 100.0, 2) AS positive_percentage;
};

-- 7. Ordenar por rango
sorted_metrics = ORDER range_metrics BY playtime_range ASC;

-- 8. Almacenar resultados en HDFS
-- Delimitado por tabulación para fácil visualización e inspección
STORE sorted_metrics INTO '$OUTPUT' USING PigStorage('\t');
