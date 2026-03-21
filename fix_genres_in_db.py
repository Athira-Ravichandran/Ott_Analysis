

import pandas as pd
import numpy as np
import mysql.connector
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

RAW_DIR     = os.path.join(os.path.dirname(__file__), "data", "raw")
CLEANED_DIR = os.path.join(os.path.dirname(__file__), "data", "cleaned")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     os.getenv("DB_USER",     "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME",     "ott_pulse"),
}

# ── Complete subgenre → standard genre map ────────────────────
SUBGENRE_MAP = {
    # Action
    "Action Epic":"Action","Gun Fu":"Action","Martial Arts":"Action",
    "Samurai":"Action","Sword & Sorcery":"Action","Superhero":"Action",
    # Adventure
    "Adventure Epic":"Adventure","Globetrotting Adventure":"Adventure",
    "Mountain Adventure":"Adventure","Desert Adventure":"Adventure",
    "Quest":"Adventure","Teen Adventure":"Adventure","Urban Adventure":"Adventure",
    # Animation
    "Hand-Drawn Animation":"Animation","Adult Animation":"Animation",
    "Anime":"Animation","Shōnen":"Animation",
    # Biography / Documentary
    "Docudrama":"Biography",
    "Docuseries":"Documentary","Nature Documentary":"Documentary",
    "History Documentary":"Documentary","Sports Documentary":"Documentary",
    "Science & Technology Documentary":"Documentary",
    "Military Documentary":"Documentary","Basketball":"Documentary",
    # Comedy
    "Dark Comedy":"Comedy","Buddy Comedy":"Comedy","Parody":"Comedy",
    "Satire":"Comedy","Sitcom":"Comedy",
    # Crime
    "Gangster":"Crime","Cop Drama":"Crime","Drug Crime":"Crime",
    "Hard-boiled Detective":"Crime","Heist":"Crime",
    "Police Procedural":"Crime","True Crime":"Crime","Whodunnit":"Mystery",
    "Serial Killer":"Thriller",
    # Drama
    "Legal Drama":"Drama","Medical Drama":"Drama","Psychological Drama":"Drama",
    "Period Drama":"Drama","Workplace Drama":"Drama","Political Drama":"Drama",
    "Prison Drama":"Drama","Coming-of-Age":"Drama",
    # Epic → Drama (Epic is a style tag, not a standalone genre)
    "Epic":"Drama","Romantic Epic":"Romance","Historical Epic":"History",
    # Fantasy
    "Dark Fantasy":"Fantasy","Fantasy Epic":"Fantasy",
    "Supernatural Fantasy":"Fantasy","Teen Fantasy":"Fantasy",
    # Horror
    "Psychological Horror":"Horror","Supernatural Horror":"Horror",
    "Folk Horror":"Horror","Body Horror":"Horror","Slasher":"Horror",
    # Romance
    "Tragic Romance":"Romance","Feel-Good Romance":"Romance",
    "Holiday Romance":"Romance",
    # Sci-Fi
    "Sci-Fi Epic":"Sci-Fi","Space Sci-Fi":"Sci-Fi",
    "Dystopian Sci-Fi":"Sci-Fi","Cyberpunk":"Sci-Fi",
    "Artificial Intelligence":"Sci-Fi","Time Travel":"Sci-Fi","Steampunk":"Sci-Fi",
    # Thriller
    "Psychological Thriller":"Thriller","Suspense Mystery":"Thriller",
    "Disaster":"Thriller",
    # War / History
    "War Epic":"War","Spaghetti Western":"Western","Western Epic":"Western",
    # Family / Sport
    "Holiday Family":"Family","Animal Adventure":"Family",
    "Sports Documentary":"Sport",
    # Musical
    "Musical":"Music",
    # Ignore these — they are mood/keyword tags, not genres
    "Japanese":None,"Atmospheric":None,"Authentic emotion":None,
    "Cinematography":None,"Direction":None,"Performance":None,
    "Character development":None,"Iconic lines":None,"Iconic score":None,
    "Narration":None,"Soundtrack":None,"Friendship":None,"Heartfelt":None,
    "Inspirational":None,"Social commentary":None,"Thought-provoking":None,
    "Philosophical elements":None,"Dark tone":None,"Gritty":None,
    "Rewatchable":None,"Nostalgic":None,"Educational":None,
    "Visual spectacle":None,"Technical achievement":None,"Realism":None,
}

STANDARD_GENRES = {
    "Action","Adventure","Animation","Biography","Comedy","Crime",
    "Documentary","Drama","Family","Fantasy","Film-Noir","History",
    "Horror","Music","Musical","Mystery","Romance","Sci-Fi",
    "Sport","Thriller","War","Western",
}


def map_to_standard(genres_str: str) -> str:
    """Map raw pipe-separated IMDb genres to standard genres."""
    if not genres_str or pd.isna(genres_str):
        return "Unknown"
    parts = [g.strip() for g in str(genres_str).split("|") if g.strip()]
    result, seen = [], set()
    for part in parts:
        if part in STANDARD_GENRES:
            mapped = part
        elif part in SUBGENRE_MAP:
            mapped = SUBGENRE_MAP[part]
        else:
            mapped = None
        if mapped and mapped not in seen:
            seen.add(mapped)
            result.append(mapped)
    return "|".join(result) if result else "Unknown"


def main():
    # ── Load raw CSV files ────────────────────────────────────
    dfs = []
    for f in ["imdb_movies_raw.csv", "imdb_shows_raw.csv"]:
        path = os.path.join(RAW_DIR, f)
        if os.path.exists(path):
            dfs.append(pd.read_csv(path))
    if not dfs:
        log.error("No raw CSV files found in data/raw/. Cannot fix genres.")
        return

    df = pd.concat(dfs, ignore_index=True)
    log.info(f"Loaded {len(df)} titles from raw CSVs.")

    # ── Apply genre fix ───────────────────────────────────────
    df["genres_clean"] = df["genres"].apply(map_to_standard)
    df["genre_primary"] = df["genres_clean"].apply(
        lambda x: x.split("|")[0] if x and x != "Unknown" else "Unknown"
    )

    # Report
    log.info("Genre distribution after fix:")
    for genre, cnt in df["genre_primary"].value_counts().items():
        log.info(f"  {genre:<25} {cnt}")

    # ── Update MySQL titles table ─────────────────────────────
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()

    update_sql = """
        UPDATE titles
        SET genres       = %s,
            genre_primary = %s
        WHERE imdb_id = %s
    """
    rows_updated = 0
    for _, row in df.iterrows():
        imdb_id = row.get("imdb_id", "")
        if not imdb_id:
            continue
        cur.execute(update_sql, (
            row["genres_clean"],
            row["genre_primary"],
            str(imdb_id),
        ))
        rows_updated += cur.rowcount

    conn.commit()
    log.info(f"Updated {rows_updated} rows in titles table.")

    # ── Rebuild genre_map table ───────────────────────────────
    cur.execute("DELETE FROM genre_map")
    log.info("Cleared genre_map table.")

    genre_rows = []
    for _, row in df.iterrows():
        genres_str = row["genres_clean"]
        if not genres_str or genres_str == "Unknown":
            genre_rows.append((
                str(row.get("imdb_id", "")), row.get("title", ""),
                "Unknown",
                None if pd.isna(row.get("imdb_rating")) else float(row["imdb_rating"]),
                None if pd.isna(row.get("year")) else int(row["year"]),
                row.get("content_type", "movie"),
            ))
        else:
            for genre in genres_str.split("|"):
                genre = genre.strip()
                if genre:
                    genre_rows.append((
                        str(row.get("imdb_id", "")), row.get("title", ""),
                        genre,
                        None if pd.isna(row.get("imdb_rating")) else float(row["imdb_rating"]),
                        None if pd.isna(row.get("year")) else int(row["year"]),
                        row.get("content_type", "movie"),
                    ))

    insert_sql = """
        INSERT INTO genre_map (imdb_id, title, genre, imdb_rating, year, content_type)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, genre_rows)
    conn.commit()
    log.info(f"Rebuilt genre_map with {len(genre_rows)} rows.")

    # ── Save updated cleaned CSV too ─────────────────────────
    df["genres"] = df["genres_clean"]
    df.drop(columns=["genres_clean"], inplace=True)
    out_path = os.path.join(CLEANED_DIR, "titles_clean.csv")
    df.to_csv(out_path, index=False)
    log.info(f"Saved updated titles_clean.csv to {out_path}")

    cur.close()
    conn.close()
    log.info("\nDone!")


if __name__ == "__main__":
    main()
