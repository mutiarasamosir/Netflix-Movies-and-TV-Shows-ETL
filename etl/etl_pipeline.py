# etl/etl_pipeline.py
import os
from pathlib import Path
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm
import re
from typing import List, Tuple

# helper: clean string (trim, None if empty)
def clean_str(s):
    if pd.isna(s): 
        return None
    s2 = str(s).strip()
    return s2 if s2 else None

# helper: split by comma-like delimiters and normalize list
def split_list_field(s: str) -> List[str]:
    if pd.isna(s) or not str(s).strip():
        return []
    parts = [p.strip() for p in re.split(r',\s*', s) if p.strip()]
    return parts

# path helper
CSV_PATH = "netflix_titles.csv"
DB_PATH = Path(__file__).resolve().parents[1] / "netflix.db"

# etl/etl_pipeline.py (lanjutan)
def quick_profile(csv_path: Path, nrows=5):
    df_head = pd.read_csv(csv_path, nrows=nrows)
    print("Columns:", list(df_head.columns))
    df = pd.read_csv(csv_path)
    print("Total rows:", len(df))
    print("Null counts per column:")
    print(df.isna().sum())
    print("\nSample unique counts:")
    for c in ["type", "rating", "release_year", "country", "listed_in"]:
        if c in df.columns:
            print(c, df[c].nunique(), "unique")
    return df

if __name__ == "__main__":
    df = quick_profile(CSV_PATH)

# etl/etl_pipeline.py (lanjutan)
def transform_row(df: pd.DataFrame) -> pd.DataFrame:
    # standar kolom
    df = df.copy()
    # clean whitespace
    for c in df.columns:
        df[c] = df[c].apply(clean_str)
    # parse date_added jika ada format "September 9, 2019"
    def parse_date(s):
        try:
            return pd.to_datetime(s)
        except Exception:
            return pd.NaT
    df['date_added_parsed'] = df['date_added'].apply(parse_date)
    # normalize release_year
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce').astype('Int64')
    # normalize listed_in -> genres list
    df['genres_list'] = df['listed_in'].apply(split_list_field)
    # normalize country -> list
    df['country_list'] = df['country'].apply(split_list_field)
    # normalize cast -> list
    df['cast_list'] = df['cast'].apply(split_list_field)
    # clean rating
    df['rating'] = df['rating'].replace({'UR':'Unrated', '0+':'Unrated'}).apply(clean_str)
    # remove exact duplicates based on show_id if exist
    df = df.drop_duplicates(subset=['show_id'])
    return df

# etl/etl_pipeline.py (lanjutan)
from sqlalchemy import create_engine, text
import sqlite3

def init_sqlite(db_path, schema_path):
    # Step 1: pake sqlite3 dulu buat apply schema
    conn = sqlite3.connect(db_path)
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    conn.executescript(sql)   
    conn.commit()
    conn.close()

    # Step 2: bikin engine SQLAlchemy buat ETL
    engine = create_engine(f"sqlite:///{db_path}")
    return engine

# in main
if __name__ == "__main__":
    engine = init_sqlite(DB_PATH, Path(__file__).resolve().parents[1]/"etl/db_schema.sql")
    print("DB initialized at", DB_PATH)

# etl/etl_pipeline.py (lanjutan)
from sqlalchemy import Table, MetaData, select

def get_table(engine, table_name):
    meta = MetaData()
    meta.reflect(bind=engine)
    return Table(table_name, meta, autoload_with=engine)

def upsert_title(conn, title_row: dict):
    # title_row contains columns for titles table
    # Try update by show_id, if 0 rows updated -> insert
    res = conn.execute(
        text("""
        UPDATE titles SET
            type=:type, title=:title, director=:director, country=:country,
            date_added=:date_added, release_year=:release_year, rating=:rating,
            duration=:duration, description=:description
        WHERE show_id=:show_id
        """), title_row
    )
    if res.rowcount == 0:
        conn.execute(
            text("""
            INSERT INTO titles (show_id, type, title, director, country, date_added, release_year, rating, duration, description)
            VALUES (:show_id, :type, :title, :director, :country, :date_added, :release_year, :rating, :duration, :description)
            """), title_row)
# etl/etl_pipeline.py (lanjutan)
def load_etl(csv_path: Path, engine, chunksize=1000):
    # We'll iterate chunks and process per-chunk to avoid memory issues.
    chunks = pd.read_csv(csv_path, chunksize=chunksize)
    total_rows = 0
    with engine.begin() as conn:
        for chunk in tqdm(chunks, desc="Processing chunks"):
            total_rows += len(chunk)
            df_clean = transform_row(chunk)
            # insert titles
            for _, row in df_clean.iterrows():
                title_row = {
                    "show_id": row.get("show_id"),
                    "type": row.get("type"),
                    "title": row.get("title"),
                    "director": row.get("director"),
                    "country": row.get("country"),
                    "date_added": row.get("date_added_parsed").strftime("%Y-%m-%d") if not pd.isna(row.get("date_added_parsed")) else None,
                    "release_year": int(row.get("release_year")) if pd.notna(row.get("release_year")) else None,
                    "rating": row.get("rating"),
                    "duration": row.get("duration"),
                    "description": row.get("description")
                }
                try:
                    upsert_title(conn, title_row)
                except IntegrityError:
                    # log and continue
                    print("IntegrityError for", title_row.get("show_id"))
            # handle genres and people mapping
            # insert genres unique and mapping
            for _, row in df_clean.iterrows():
                # fetch title_id
                res = conn.execute(text("SELECT title_id FROM titles WHERE show_id = :sid"), {"sid": row.get("show_id")})
                title_id_row = res.first()
                if not title_id_row:
                    continue
                title_id = title_id_row[0]
                # genres
                for g in row['genres_list']:
                    if not g: continue
                    # insert genre if not exists
                    conn.execute(text("INSERT OR IGNORE INTO genres (name) VALUES (:name)"), {"name": g})
                    gid = conn.execute(text("SELECT genre_id FROM genres WHERE name=:name"), {"name": g}).scalar()
                    # insert mapping
                    conn.execute(text("""
                        INSERT OR IGNORE INTO title_genres (title_id, genre_id) VALUES (:tid, :gid)
                    """), {"tid": title_id, "gid": gid})
                # cast
                for person in row['cast_list']:
                    if not person: continue
                    conn.execute(text("INSERT OR IGNORE INTO people (name) VALUES (:name)"), {"name": person})
                    pid = conn.execute(text("SELECT person_id FROM people WHERE name=:name"), {"name": person}).scalar()
                    conn.execute(text("""
                        INSERT OR IGNORE INTO title_cast (title_id, person_id, role_type) VALUES (:tid, :pid, :role)
                    """), {"tid": title_id, "pid": pid, "role": "cast"})
                # countries (as people table with role_type='country' or separate table - here we keep in title_cast for simplicity)
                for country in row['country_list']:
                    if not country: continue
                    conn.execute(text("INSERT OR IGNORE INTO people (name) VALUES (:name)"), {"name": country})
                    pid = conn.execute(text("SELECT person_id FROM people WHERE name=:name"), {"name": country}).scalar()
                    conn.execute(text("""
                        INSERT OR IGNORE INTO title_cast (title_id, person_id, role_type) VALUES (:tid, :pid, :role)
                    """), {"tid": title_id, "pid": pid, "role": "country"})
                # director as person with role_type='director'
                if row.get('director'):
                    director = row.get('director')
                    conn.execute(text("INSERT OR IGNORE INTO people (name) VALUES (:name)"), {"name": director})
                    pid = conn.execute(text("SELECT person_id FROM people WHERE name=:name"), {"name": director}).scalar()
                    conn.execute(text("""
                        INSERT OR IGNORE INTO title_cast (title_id, person_id, role_type) VALUES (:tid, :pid, :role)
                    """), {"tid": title_id, "pid": pid, "role": "director"})
    print("Total rows processed:", total_rows)


# etl/etl_pipeline.py (lanjutan)
def minimal_profile(engine):
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM titles")).scalar()
        print("Titles rows:", total)
        # null PKs check
        null_pk = conn.execute(text("SELECT COUNT(*) FROM titles WHERE show_id IS NULL")).scalar()
        print("Titles with null show_id:", null_pk)
        # sample unique counts
        unique_ratings = conn.execute(text("SELECT COUNT(DISTINCT rating) FROM titles")).scalar()
        print("Unique ratings:", unique_ratings)
        # top 5 genres
        print("Top 5 genres:")
        rows = conn.execute(text("""
            SELECT g.name, COUNT(*) as cnt FROM genres g
            JOIN title_genres tg ON g.genre_id = tg.genre_id
            GROUP BY g.name ORDER BY cnt DESC LIMIT 5
        """))
        for r in rows:
            print(r)
def dq_checks(engine):
    with engine.connect() as conn:
        t_src = conn.execute(text("SELECT COUNT(*) FROM titles")).scalar()
        t_gen_map = conn.execute(text("SELECT COUNT(*) FROM title_genres")).scalar()
        # Example checks
        if t_src == 0:
            raise ValueError("DQ Failed: titles table empty")
        # no nulls in primary key show_id
        null_showid = conn.execute(text("SELECT COUNT(*) FROM titles WHERE show_id IS NULL OR show_id = ''")).scalar()
        if null_showid > 0:
            raise ValueError(f"DQ Failed: {null_showid} rows with null/empty show_id")
        # foreign key consistency (every mapping points to existing title)
        bad_fk = conn.execute(text("""
            SELECT COUNT(*) FROM title_genres tg
            LEFT JOIN titles t ON tg.title_id = t.title_id
            WHERE t.title_id IS NULL
        """)).scalar()
        if bad_fk > 0:
            raise ValueError(f"DQ Failed: {bad_fk} title_genres rows point to missing titles")
        print("DQ checks passed.")

