-- ==============================================================================
-- PROYECTO: Análisis de polaridad de recomendaciones en Steam según tiempo de juego
-- SCRIPT: steam_polarity_by_game.pig
-- OBJETIVO: Calcular la tasa de polaridad por rango de tiempo para cada juego (app_id).
--           Permite identificar juegos donde jugadores veteranos (200h+) son más
--           críticos en comparación con jugadores nuevos (0-10h).
-- ==============================================================================

-- 1. Optimizaciones de ejecución en Hadoop MapReduce
SET default_parallel 20;                     -- Incrementar según tamaño del cluster
SET pig.exec.reducers.bytes.per.reducer 134217728;
SET mapreduce.map.speculative false;
SET mapreduce.reduce.speculative false;

-- 2. Carga de datos desde HDFS (.bz2)
raw_data = LOAD '$INPUT' USING PigStorage(',') AS (
    app_id: chararray,
    review_id: chararray,
    review_text: chararray,
    voted_up: chararray,
    author_playtime_forever: int,
    author_playtime_at_review: int
);

-- 3. Limpieza y filtrado
cleaned_data = FILTER raw_data BY 
    (app_id IS NOT NULL AND app_id != 'app_id' AND app_id != 'appid') AND
    (voted_up IS NOT NULL) AND
    (author_playtime_forever IS NOT NULL AND author_playtime_forever >= 0);

-- 4. Proyección y categorización
projected_data = FOREACH cleaned_data GENERATE
    app_id,
    ((LOWER(voted_up) == 'true' OR voted_up == '1') ? 1 : 0) AS is_positive:int,
    (
        author_playtime_forever < 600 ? '1. 0-10h' :
        (author_playtime_forever < 3000 ? '2. 10-50h' :
        (author_playtime_forever < 12000 ? '3. 50-200h' : '4. 200h+'))
    ) AS playtime_range:chararray;

-- 5. Agrupación compuesta: por app_id y rango de horas
grouped_by_app_and_range = GROUP projected_data BY (app_id, playtime_range);

-- 6. Métricas por juego y rango
app_range_metrics = FOREACH grouped_by_app_and_range {
    total_reviews = COUNT(projected_data);
    pos_reviews = SUM(projected_data.is_positive);
    GENERATE
        group.app_id AS app_id,
        group.playtime_range AS playtime_range,
        total_reviews AS total_reviews,
        pos_reviews AS positive_reviews,
        (total_reviews - pos_reviews) AS negative_reviews,
        ROUND_TO(((double)pos_reviews / (double)total_reviews) * 100.0, 2) AS positive_percentage;
};

-- 7. Filtrar muestras representativas (ej. al menos 20 reseñas en el segmento)
-- para evitar distorsiones por muestras pequeñas
statistically_significant = FILTER app_range_metrics BY total_reviews >= 20;

-- 8. Ordenar por app_id y rango de juego
sorted_app_metrics = ORDER statistically_significant BY app_id ASC, playtime_range ASC;

-- 9. Guardar resultados
STORE sorted_app_metrics INTO '$OUTPUT' USING PigStorage('\t');
