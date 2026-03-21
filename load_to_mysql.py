
import mysql.connector
import pandas as pd
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

CLEANED_DIR = os.path.join(os.path.dirname(__file__), "data", "cleaned")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     os.getenv("DB_USER",     "root"),
    "password": os.getenv("DB_PASSWORD", ""),
}
DB_NAME = os.getenv("DB_NAME", "ott_pulse")


# ─── DDL ─────────────────────────────────────────────────────────────────────

CREATE_DB = f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
USE_DB    = f"USE `{DB_NAME}`;"

DDL = [
    """
    CREATE TABLE IF NOT EXISTS titles (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        imdb_id         VARCHAR(20)     NOT NULL UNIQUE,
        title           VARCHAR(500)    NOT NULL,
        content_type    ENUM('movie','tv_show') NOT NULL DEFAULT 'movie',
        year            SMALLINT,
        decade          VARCHAR(10),
        era             VARCHAR(20),
        is_recent       TINYINT(1)      DEFAULT 0,
        imdb_rating     DECIMAL(3,1),
        votes           INT             DEFAULT 0,
        vote_weighted_score DECIMAL(8,3),
        popularity_tier ENUM('High','Medium','Low'),
        runtime_min     SMALLINT,
        runtime_category VARCHAR(30),
        genres          VARCHAR(500),
        genre_primary   VARCHAR(100),
        certificate     VARCHAR(30),
        plot            TEXT,
        `rank`          SMALLINT,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_year        (year),
        INDEX idx_rating      (imdb_rating),
        INDEX idx_genre       (genre_primary),
        INDEX idx_content_type(content_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS platforms (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        imdb_id         VARCHAR(20),
        title           VARCHAR(500),
        content_type    VARCHAR(20),
        platform        VARCHAR(100),
        platform_group  VARCHAR(100),
        monetization    VARCHAR(50),
        presentation    VARCHAR(20),
        imdb_rating     DECIMAL(3,1),
        year            SMALLINT,
        genre_primary   VARCHAR(100),
        vote_weighted_score DECIMAL(8,3),
        is_recent       TINYINT(1),
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_platform    (platform_group),
        INDEX idx_imdb_id     (imdb_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS genre_map (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        imdb_id         VARCHAR(20),
        title           VARCHAR(500),
        genre           VARCHAR(100),
        imdb_rating     DECIMAL(3,1),
        year            SMALLINT,
        content_type    VARCHAR(20),
        INDEX idx_genre (genre),
        INDEX idx_imdb  (imdb_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def get_connection(include_db: bool = True):
    cfg = DB_CONFIG.copy()
    if include_db:
        cfg["database"] = DB_NAME
    return mysql.connector.connect(**cfg)


def safe_val(v):
    """Convert NaN / NaT / NA to None for MySQL insertion."""
    if pd.isna(v) if not isinstance(v, str) else False:
        return None
    return v


def bulk_insert(cursor, table: str, df: pd.DataFrame, batch_size: int = 500):
    """Insert DataFrame rows into a MySQL table in batches."""
    if df.empty:
        log.warning(f"  No data to insert into {table}.")
        return

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names    = ", ".join([f"`{c}`" for c in cols])
    sql = f"INSERT IGNORE INTO `{table}` ({col_names}) VALUES ({placeholders})"

    rows = [tuple(safe_val(v) for v in row) for row in df.itertuples(index=False)]
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(sql, batch)
        total += len(batch)
    log.info(f"  Inserted {total} rows into `{table}`.")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def setup_schema():
    log.info("Setting up database schema...")
    conn = get_connection(include_db=False)
    cur  = conn.cursor()
    cur.execute(CREATE_DB)
    cur.execute(USE_DB)
    for ddl in DDL:
        cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Database `{DB_NAME}` and tables ready.")


def load_data():
    titles_path   = os.path.join(CLEANED_DIR, "titles_clean.csv")
    platform_path = os.path.join(CLEANED_DIR, "platform_clean.csv")
    genre_path    = os.path.join(CLEANED_DIR, "genre_exploded.csv")

    titles_df   = pd.read_csv(titles_path)   if os.path.exists(titles_path)   else pd.DataFrame()
    platform_df = pd.read_csv(platform_path) if os.path.exists(platform_path) else pd.DataFrame()
    genre_df    = pd.read_csv(genre_path)    if os.path.exists(genre_path)    else pd.DataFrame()

    log.info(f"Loading: {len(titles_df)} titles, {len(platform_df)} platform rows, {len(genre_df)} genre rows")

    conn = get_connection()
    cur  = conn.cursor()

    # ── Titles
    if not titles_df.empty:
        title_cols = [
            "imdb_id","title","content_type","year","decade","era","is_recent",
            "imdb_rating","votes","vote_weighted_score","popularity_tier",
            "runtime_min","runtime_category","genres","genre_primary",
            "certificate","plot","rank"
        ]
        existing = [c for c in title_cols if c in titles_df.columns]
        bulk_insert(cur, "titles", titles_df[existing])

    # ── Platforms
    if not platform_df.empty:
        plat_cols = [
            "imdb_id","title","content_type","platform","platform_group",
            "monetization","presentation","imdb_rating","year",
            "genre_primary","vote_weighted_score","is_recent"
        ]
        existing = [c for c in plat_cols if c in platform_df.columns]
        bulk_insert(cur, "platforms", platform_df[existing])

    # ── Genres
    if not genre_df.empty:
        genre_cols = ["imdb_id","title","genre","imdb_rating","year","content_type"]
        existing = [c for c in genre_cols if c in genre_df.columns]
        bulk_insert(cur, "genre_map", genre_df[existing])

    conn.commit()
    cur.close()
    conn.close()
    log.info("All data loaded into MySQL successfully.")


if __name__ == "__main__":
    setup_schema()
    load_data()
    log.info("Done. Connect Power BI to MySQL database: ott_pulse")
