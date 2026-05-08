WITH base AS (
    SELECT
      JSON_VALUE(response, '$.viewModel.date')                        AS snapshot_date,
      JSON_VALUE(response, '$.viewModel.theater.details.id')          AS theater_id,
      JSON_QUERY(response, '$.viewModel.movies')                      AS movies
    FROM `yenjamin.fandango_tmp.showtimes_tmp`
  ),

  movies AS (
    SELECT
      snapshot_date, theater_id,
      JSON_VALUE(movie, '$.id')          AS movie_id,
      JSON_VALUE(movie, '$.title')       AS title,
      JSON_VALUE(movie, '$.rating')      AS rating,
      JSON_VALUE(movie, '$.runtime')     AS runtime,
      JSON_VALUE(movie, '$.releaseDate') AS release_date,
      ARRAY_TO_STRING(JSON_VALUE_ARRAY(movie, '$.genres'), ', ') AS genres,
      JSON_QUERY(movie, '$.variants')    AS variants
    FROM base, UNNEST(JSON_QUERY_ARRAY(movies)) AS movie
  ),

  variants AS (
    SELECT
      snapshot_date, theater_id, movie_id, title, rating, runtime, release_date, genres,
      JSON_VALUE(variant, '$.filmFormatHeader') AS format,
      JSON_QUERY(variant, '$.amenityGroups')    AS amenity_groups
    FROM movies, UNNEST(JSON_QUERY_ARRAY(variants)) AS variant
  ),

  amenity_groups AS (
    SELECT
      snapshot_date, theater_id, movie_id, title, rating, runtime, release_date, genres, format,
      JSON_VALUE(ag, '$.amenityString') AS amenities,
      JSON_QUERY(ag, '$.showtimes')     AS showtimes
    FROM variants, UNNEST(JSON_QUERY_ARRAY(amenity_groups)) AS ag
  )

  SELECT
    CAST(JSON_VALUE(st, '$.id') AS INT64)             AS showtime_id,
    snapshot_date,
    theater_id,
    CAST(movie_id AS INT64)                           AS movie_id,
    title,
    rating,
    CAST(runtime AS INT64)                            AS runtime_mins,
    release_date,
    genres,
    format,
    amenities,
    JSON_VALUE(st, '$.ticketingDate')                 AS showtime_datetime,
    JSON_VALUE(st, '$.date')                          AS showtime_time_display,
    JSON_VALUE(st, '$.type')                          AS availability,
    CAST(JSON_VALUE(st, '$.expired') AS BOOL)         AS expired,
    CAST(JSON_VALUE(st, '$.hasMatineeMessage') AS BOOL) AS has_matinee,
    JSON_VALUE(st, '$.matineeMessage')                AS matinee_discount,
    JSON_VALUE(st, '$.showtimeHashCode')              AS showtime_hash,
    JSON_VALUE(st, '$.ticketingJumpPageURL')          AS ticketing_url
  FROM amenity_groups, UNNEST(JSON_QUERY_ARRAY(showtimes)) AS st