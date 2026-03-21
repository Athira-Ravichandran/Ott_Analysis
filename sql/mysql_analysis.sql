use ott_pulse;
show tables;

SELECT * FROM titles;
SELECT * FROM platforms;


SELECT 'titles'    AS tbl, COUNT(*) AS rows FROM titles   UNION ALL
SELECT 'platforms' AS tbl, COUNT(*) AS rows FROM platforms UNION ALL
SELECT 'genre_map' AS tbl, COUNT(*) AS rows FROM genre_map;

-- Check genre distribution (should show Drama, Crime, Action etc — not "Unknown")
SELECT genre_primary, COUNT(*) AS cnt
FROM titles
GROUP BY genre_primary
ORDER BY cnt DESC;

-- If you see mostly "Unknown" → run the fix steps below before the main queries


-- =============================================================
-- SECTION 1: BASIC KPI METRICS
-- =============================================================

-- KPI 1: Overall summary stats
SELECT
    COUNT(*)                                        AS total_titles,
    SUM(content_type = 'movie')                     AS total_movies,
    SUM(content_type = 'tv_show')                   AS total_shows,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating,
    ROUND(AVG(runtime_min), 0)                      AS avg_runtime_min,
    MIN(year)                                       AS earliest_year,
    MAX(year)                                       AS latest_year,
    SUM(is_recent)                                  AS titles_post_2018
FROM titles;


-- KPI 2: Rating breakdown by content type
SELECT
    content_type,
    COUNT(*)                                        AS total,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating,
    MAX(imdb_rating)                                AS highest_rating,
    MIN(imdb_rating)                                AS lowest_rating,
    ROUND(AVG(vote_weighted_score), 2)              AS avg_weighted_score
FROM titles
GROUP BY content_type;


-- KPI 3: Genre distribution (from titles table — primary genre per title)
SELECT
    genre_primary                                   AS genre,
    COUNT(*)                                        AS title_count,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating,
    ROUND(AVG(vote_weighted_score), 2)              AS avg_weighted_score,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM titles
WHERE genre_primary != 'Unknown'
GROUP BY genre_primary
ORDER BY title_count DESC;


-- KPI 4: Genre distribution from genre_map (multi-genre, more accurate)
SELECT
    genre,
    COUNT(DISTINCT imdb_id)                         AS unique_titles,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating,
    COUNT(DISTINCT CASE WHEN content_type = 'movie'   THEN imdb_id END) AS movies,
    COUNT(DISTINCT CASE WHEN content_type = 'tv_show' THEN imdb_id END) AS shows
FROM genre_map
WHERE genre != 'Unknown'
GROUP BY genre
ORDER BY unique_titles DESC;


-- =============================================================
-- SECTION 2: WINDOW FUNCTION QUERIES
-- =============================================================

-- W1: Rank ALL titles overall by vote_weighted_score
SELECT
    title,
    content_type,
    year,
    imdb_rating,
    votes,
    vote_weighted_score,
    RANK()       OVER (ORDER BY vote_weighted_score DESC)           AS overall_rank,
    RANK()       OVER (PARTITION BY content_type
                       ORDER BY vote_weighted_score DESC)           AS rank_in_type,
    NTILE(4)     OVER (ORDER BY imdb_rating DESC)                   AS rating_quartile
FROM titles
ORDER BY overall_rank;


-- W2: Rank titles within each genre by IMDb rating
SELECT
    t.genre_primary                                 AS genre,
    t.title,
    t.content_type,
    t.year,
    t.imdb_rating,
    t.vote_weighted_score,
    RANK() OVER (
        PARTITION BY t.genre_primary
        ORDER BY t.imdb_rating DESC
    )                                               AS rank_in_genre
FROM titles t
WHERE t.genre_primary != 'Unknown'
ORDER BY t.genre_primary, rank_in_genre;


-- W3: Running average rating ordered by year (whole dataset, no HAVING filter)
SELECT
    year,
    title,
    content_type,
    imdb_rating,
    ROUND(
        AVG(imdb_rating) OVER (
            ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    )                                               AS rolling_3_avg_rating,
    ROUND(
        AVG(imdb_rating) OVER (
            PARTITION BY content_type
            ORDER BY year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    )                                               AS rolling_avg_by_type,
    COUNT(*) OVER (PARTITION BY year)               AS titles_same_year
FROM titles
WHERE year IS NOT NULL
ORDER BY year, imdb_rating DESC;


-- W4: Rating percentile within each content type
SELECT
    title,
    content_type,
    year,
    genre_primary,
    imdb_rating,
    votes,
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY content_type
            ORDER BY imdb_rating ASC
        ) * 100, 1
    )                                               AS rating_percentile,
    LAG(title)  OVER (PARTITION BY content_type ORDER BY imdb_rating DESC)
                                                    AS next_lower_title,
    LEAD(title) OVER (PARTITION BY content_type ORDER BY imdb_rating DESC)
                                                    AS next_higher_title
FROM titles
ORDER BY content_type, imdb_rating DESC;


-- W5: Cumulative count of titles by release year
SELECT
    year,
    titles_that_year,
    SUM(titles_that_year) OVER (ORDER BY year)      AS cumulative_titles,
    ROUND(
        SUM(titles_that_year) OVER (ORDER BY year) * 100.0 /
        SUM(titles_that_year) OVER (), 1
    )                                               AS cumulative_pct
FROM (
    SELECT year, COUNT(*) AS titles_that_year
    FROM titles
    WHERE year IS NOT NULL
    GROUP BY year
) yr
ORDER BY year;


-- W6: YoY rating change by genre — FIXED (no HAVING filter, uses era instead of year)
-- Groups by era (broader bucket) so we always have enough rows
WITH genre_era_avg AS (
    SELECT
        genre_primary                               AS genre,
        era,
        ROUND(AVG(imdb_rating), 2)                  AS avg_rating,
        COUNT(*)                                    AS title_count
    FROM titles
    WHERE genre_primary NOT IN ('Unknown')
      AND era != 'Unknown'
    GROUP BY genre_primary, era
)
SELECT
    genre,
    era,
    avg_rating,
    title_count,
    LAG(avg_rating)  OVER (PARTITION BY genre ORDER BY era)  AS prev_era_avg,
    ROUND(
        avg_rating - LAG(avg_rating) OVER (PARTITION BY genre ORDER BY era),
    2)                                              AS era_rating_change
FROM genre_era_avg
ORDER BY genre, era;


-- W7: Top title per genre (best vote_weighted_score)
WITH ranked AS (
    SELECT
        genre_primary                               AS genre,
        title,
        year,
        imdb_rating,
        vote_weighted_score,
        content_type,
        RANK() OVER (
            PARTITION BY genre_primary
            ORDER BY vote_weighted_score DESC
        )                                           AS rk
    FROM titles
    WHERE genre_primary != 'Unknown'
)
SELECT genre, title, year, imdb_rating, vote_weighted_score, content_type
FROM ranked
WHERE rk = 1
ORDER BY vote_weighted_score DESC;


-- W8: LEAD/LAG — compare each title's rating to the next/prev title by year
SELECT
    year,
    title,
    imdb_rating,
    LAG(title,      1) OVER (ORDER BY year, imdb_rating DESC) AS prev_title,
    LAG(imdb_rating,1) OVER (ORDER BY year, imdb_rating DESC) AS prev_rating,
    ROUND(imdb_rating -
        LAG(imdb_rating,1) OVER (ORDER BY year, imdb_rating DESC), 2) AS diff_from_prev
FROM titles
WHERE year IS NOT NULL
ORDER BY year;


-- =============================================================
-- SECTION 3: STORYTELLING QUERIES
-- =============================================================

-- Story 1: "Decades of Greatness" — how quality changed over time
SELECT
    era,
    COUNT(*)                                        AS total_titles,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating,
    ROUND(AVG(vote_weighted_score), 2)              AS avg_weighted_score,
    MAX(title)                                      AS sample_title
FROM titles
WHERE era != 'Unknown'
GROUP BY era
ORDER BY era;


-- Story 2: "Hidden Gems" — great rating, fewer votes (underrated titles)
SELECT
    title, year, genre_primary, imdb_rating,
    votes, vote_weighted_score, content_type
FROM titles
WHERE imdb_rating >= 8.5
ORDER BY votes ASC
LIMIT 10;


-- Story 3: Genre breakdown — which genres dominate the top-rated list?
SELECT
    g.genre,
    COUNT(DISTINCT g.imdb_id)                       AS title_count,
    ROUND(AVG(g.imdb_rating), 2)                    AS avg_rating,
    SUM(CASE WHEN g.content_type = 'movie'   THEN 1 ELSE 0 END) AS movies,
    SUM(CASE WHEN g.content_type = 'tv_show' THEN 1 ELSE 0 END) AS shows
FROM genre_map g
WHERE g.genre != 'Unknown'
GROUP BY g.genre
HAVING title_count >= 3
ORDER BY avg_rating DESC;


-- Story 4: Runtime vs rating — do longer films rate higher?
SELECT
    runtime_category,
    COUNT(*)                                        AS title_count,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating,
    ROUND(AVG(runtime_min), 0)                      AS avg_runtime_min
FROM titles
WHERE runtime_category != 'Unknown'
GROUP BY runtime_category
ORDER BY avg_rating DESC;


-- Story 5: Certificate breakdown — what age ratings dominate?
SELECT
    certificate,
    COUNT(*)                                        AS title_count,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating
FROM titles
WHERE certificate NOT IN ('Unknown', '')
GROUP BY certificate
ORDER BY title_count DESC;


-- =============================================================
-- SECTION 4: PLATFORM QUERIES
-- (These become powerful once TMDB key is configured)
-- =============================================================

-- Platform summary (shows "Not Available" until TMDB key is set)
SELECT
    platform_group                                  AS platform,
    COUNT(DISTINCT imdb_id)                         AS titles,
    ROUND(AVG(imdb_rating), 2)                      AS avg_rating,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS share_pct
FROM platforms
GROUP BY platform_group
ORDER BY titles DESC;


-- Once TMDB data is loaded, run this for real platform analysis:
SELECT
    p.platform_group                                AS platform,
    COUNT(DISTINCT p.imdb_id)                       AS total_titles,
    ROUND(AVG(p.imdb_rating), 2)                    AS avg_rating,
    ROUND(AVG(p.vote_weighted_score), 2)            AS avg_weighted_score,
    SUM(p.is_recent)                                AS recent_titles,
    COUNT(DISTINCT p.genre_primary)                 AS genre_diversity,
    ROUND(COUNT(DISTINCT p.imdb_id) * 100.0 /
          SUM(COUNT(DISTINCT p.imdb_id)) OVER (), 1) AS platform_share_pct
FROM platforms p
WHERE p.platform_group NOT IN ('Not Available', 'Other', '')
GROUP BY p.platform_group
ORDER BY avg_weighted_score DESC;


-- =============================================================
-- SECTION 5: VIEWS FOR POWER BI
-- =============================================================

CREATE OR REPLACE VIEW vw_genre_performance AS
SELECT
    g.genre,
    COUNT(DISTINCT g.imdb_id)                       AS total_titles,
    ROUND(AVG(g.imdb_rating), 2)                    AS avg_rating,
    MIN(g.year)                                     AS oldest_year,
    MAX(g.year)                                     AS newest_year,
    COUNT(DISTINCT CASE WHEN g.content_type = 'movie'   THEN g.imdb_id END) AS movies,
    COUNT(DISTINCT CASE WHEN g.content_type = 'tv_show' THEN g.imdb_id END) AS shows
FROM genre_map g
WHERE g.genre != 'Unknown'
GROUP BY g.genre;


CREATE OR REPLACE VIEW vw_titles_enriched AS
SELECT
    t.imdb_id, t.title, t.content_type, t.year, t.era, t.decade,
    t.is_recent, t.imdb_rating, t.votes, t.vote_weighted_score,
    t.popularity_tier, t.runtime_min, t.runtime_category,
    t.genre_primary, t.certificate,
    RANK() OVER (ORDER BY t.vote_weighted_score DESC)           AS overall_rank,
    RANK() OVER (PARTITION BY t.content_type
                 ORDER BY t.vote_weighted_score DESC)           AS rank_in_type,
    RANK() OVER (PARTITION BY t.genre_primary
                 ORDER BY t.imdb_rating DESC)                   AS rank_in_genre,
    NTILE(4) OVER (ORDER BY t.imdb_rating DESC)                 AS rating_quartile
FROM titles t;


CREATE OR REPLACE VIEW vw_platform_kpis AS
SELECT
    p.platform_group                                AS platform,
    COUNT(DISTINCT p.imdb_id)                       AS total_titles,
    ROUND(AVG(p.imdb_rating), 2)                    AS avg_rating,
    ROUND(AVG(p.vote_weighted_score), 2)            AS avg_weighted_score,
    COUNT(DISTINCT p.genre_primary)                 AS genre_diversity,
    SUM(p.is_recent)                                AS recent_titles,
    ROUND(COUNT(DISTINCT p.imdb_id) * 100.0 /
          SUM(COUNT(DISTINCT p.imdb_id)) OVER (), 1) AS platform_share_pct
FROM platforms p
WHERE p.platform_group NOT IN ('Not Available', 'Other', '')
GROUP BY p.platform_group;