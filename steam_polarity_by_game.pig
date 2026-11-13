-- ==============================================================================
-- PROYECTO: Análisis de polaridad de recomendaciones en Steam según tiempo de juego
-- GRUPO 4 - CC5212
-- SCRIPT: steam_polarity_by_game.pig
-- OBJETIVO: Calcular la tasa de polaridad por rango de tiempo para cada juego (appid).
--           Permite identificar juegos donde jugadores veteranos (200h+) son más
--           críticos en comparación con jugadores nuevos (0-10h).
--
-- Esquema real verificado en HDFS (noviembre 2026). Ver steam_polarity_global.pig
-- para el detalle completo y las notas sobre CSVExcelStorage / voted_up 0-1.
-- ==============================================================================

-- 1. Optimizaciones de ejecución en Hadoop MapReduce
SET default_parallel 20;                    
SET pig.exec.reducers.bytes.per.reducer 134217728;
SET mapreduce.map.speculative false;
SET mapreduce.reduce.speculative false;

-- 2. Piggybank (CSVExcelStorage)
REGISTER /data/hadoop/spark-2.1.0-bin-hadoop2.3.old/pig-0.14.0/lib/piggybank.jar;

-- 3. Carga de datos desde HDFS (.bz2 es descomprimido y dividido nativamente por Hadoop)
raw_data = LOAD '$INPUT' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (
    recommendationid: int,
    appid: int,
    game: chararray,
    author_steamid: long,
    author_num_games_owned: int,
    author_num_reviews: int,
    author_playtime_forever: int,
    author_playtime_last_two_weeks: int,
    author_playtime_at_review: int,
    author_last_played: long,
    language: chararray,
    review: chararray,
    timestamp_created: long,
    timestamp_updated: long,
    voted_up: int,
    votes_up: int,
    votes_funny: int,
    weighted_vote_score: double,
    comment_count: int,
    steam_purchase: int,
    received_for_free: int,
    written_during_early_access: int,
    hidden_in_steam_china: int,
    steam_china_location: chararray
);

-- 4. Limpieza y filtrado (la cabecera queda con appid NULL y se descarta)
cleaned_data = FILTER raw_data BY
    (appid IS NOT NULL) AND
    (voted_up IS NOT NULL AND (voted_up == 0 OR voted_up == 1)) AND
    (author_playtime_forever IS NOT NULL AND author_playtime_forever >= 0);

-- 5. Proyección y categorización
projected_data = FOREACH cleaned_data GENERATE
    appid,
    game,
    voted_up AS is_positive:int,
    (
        author_playtime_forever < 600 ? '1. 0-10h' :
        (author_playtime_forever < 3000 ? '2. 10-50h' :
        (author_playtime_forever < 12000 ? '3. 50-200h' : '4. 200h+'))
    ) AS playtime_range:chararray;

-- 6. Agrupación compuesta: por appid, juego y rango de horas
grouped_by_app_and_range = GROUP projected_data BY (appid, game, playtime_range);

-- 7. Métricas por juego y rango
app_range_metrics = FOREACH grouped_by_app_and_range {
    total_reviews = COUNT(projected_data);
    pos_reviews = SUM(projected_data.is_positive);
    GENERATE
        group.appid AS appid,
        group.game AS game,
        group.playtime_range AS playtime_range,
        total_reviews AS total_reviews,
        pos_reviews AS positive_reviews,
        (total_reviews - pos_reviews) AS negative_reviews,
        ROUND_TO(((double)pos_reviews / (double)total_reviews) * 100.0, 2) AS positive_percentage;
};

-- 8. Filtrar muestras representativas (al menos 20 reseñas en el segmento)
--    para evitar distorsiones por muestras pequeñas
statistically_significant = FILTER app_range_metrics BY total_reviews >= 20;

-- 9. Ordenar por appid y rango
sorted_app_metrics = ORDER statistically_significant BY appid ASC, playtime_range ASC;

-- 10. Guardar resultados
STORE sorted_app_metrics INTO '$OUTPUT' USING PigStorage('\t');
