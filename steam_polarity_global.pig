-- ==============================================================================
-- PROYECTO: Análisis de polaridad de recomendaciones en Steam según tiempo de juego
-- GRUPO 4 - CC5212
-- SCRIPT: steam_polarity_global.pig
-- OBJETIVO: Calcular la tasa de recomendación positiva (polaridad) por rango de
--           tiempo jugado a nivel global en toda la plataforma.
--
-- ESQUEMA REAL VERIFICADO en HDFS (noviembre 2026):
--   recommendationid,appid,game,author_steamid,author_num_games_owned,
--   author_num_reviews,author_playtime_forever,author_playtime_last_two_weeks,
--   author_playtime_at_review,author_last_played,language,review,
--   timestamp_created,timestamp_updated,voted_up,votes_up,votes_funny,
--   weighted_vote_score,comment_count,steam_purchase,received_for_free,
--   written_during_early_access,hidden_in_steam_china,steam_china_location
--
-- Notas importantes:
--   * El campo 'review' es texto libre entre comillas (puede traer comas y
--     comillas escapadas como ""). PigStorage(',') partiría esos campos y
--     desplazaría todo lo demás, por lo que se usa CSVExcelStorage (piggybank).
--   * voted_up viene como entero 0/1 (NO como True/False).
--   * 'review' no se usa en este análisis; se carga pero se descarta en la
--     proyección (tampoco hay saltos de línea embebidos; WC físico = nº de reseñas).
--   * La cabecera no castea a int/long, por lo que appid queda NULL y el
--     FILTER la descarta automáticamente.
-- ==============================================================================

-- 1. Optimizaciones de ejecución en Hadoop MapReduce
SET default_parallel 10;                     
SET pig.exec.reducers.bytes.per.reducer 134217728; 
SET mapreduce.map.speculative false;
SET mapreduce.reduce.speculative false;

-- 2. Piggybank (CSVExcelStorage). El jar de piggybank 0.14 incluido en el
--    cluster funciona correctamente con Pig 0.18 para este caso de uso.
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

-- 4. Limpieza y filtrado de datos:
-- Descartar cabecera (appid NULL por cast fallido), registros nulos y tiempos inválidos
cleaned_data = FILTER raw_data BY
    (appid IS NOT NULL) AND
    (voted_up IS NOT NULL AND (voted_up == 0 OR voted_up == 1)) AND
    (author_playtime_forever IS NOT NULL AND author_playtime_forever >= 0);

-- 5. Transformación:
-- a) voted_up ya es binario (1 = Positivo, 0 = Negativo)
-- b) Segmentación de usuarios por rango de horas jugadas acumuladas:
--    - 0 a 10 horas   (0 - 600 min)
--    - 10 a 50 horas  (600 - 3000 min)
--    - 50 a 200 horas (3000 - 12000 min)
--    - 200+ horas     (> 12000 min)
projected_data = FOREACH cleaned_data GENERATE
    appid,
    game,
    voted_up AS is_positive:int,
    (
        author_playtime_forever < 600 ? '1. 0-10h' :
        (author_playtime_forever < 3000 ? '2. 10-50h' :
        (author_playtime_forever < 12000 ? '3. 50-200h' : '4. 200h+'))
    ) AS playtime_range:chararray;

-- 6. Agrupación por Rango de Horas Jugadas
grouped_by_range = GROUP projected_data BY playtime_range;

-- 7. Agregación y Métricas de Polaridad
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

-- 8. Ordenar por rango
sorted_metrics = ORDER range_metrics BY playtime_range ASC;

-- 9. Almacenar resultados en HDFS
-- Delimitado por tabulación para fácil visualización e inspección
STORE sorted_metrics INTO '$OUTPUT' USING PigStorage('\t');
