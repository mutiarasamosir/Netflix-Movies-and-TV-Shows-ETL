-- etl/db_schema.sql
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS titles (
    title_id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id TEXT NOT NULL UNIQUE, -- id asli dari dataset
    type TEXT,
    title TEXT NOT NULL,
    director TEXT,
    country TEXT,
    date_added DATE,
    release_year INTEGER,
    rating TEXT,
    duration TEXT,
    description TEXT
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS title_genres (
    title_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (title_id, genre_id),
    FOREIGN KEY (title_id) REFERENCES titles (title_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres (genre_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS people (
    person_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS title_cast (
    title_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    role_type TEXT, -- 'cast' or 'country' or 'director' flexible
    PRIMARY KEY (title_id, person_id, role_type),
    FOREIGN KEY (title_id) REFERENCES titles (title_id) ON DELETE CASCADE,
    FOREIGN KEY (person_id) REFERENCES people (person_id) ON DELETE CASCADE
);

-- Index recommendations (SQLite will use automatically for PK/UNIQUE, add extras)
CREATE INDEX IF NOT EXISTS idx_titles_release_year ON titles (release_year);
CREATE INDEX IF NOT EXISTS idx_titles_type ON titles (type);


-- jumlah per country (menggunakan title_cast role_type='country')
SELECT p.name AS country, COUNT(*) AS cnt
FROM title_cast tc
JOIN people p ON tc.person_id = p.person_id
WHERE tc.role_type = 'country'
GROUP BY p.name
ORDER BY cnt DESC
LIMIT 10;

SELECT g.name AS genre, COUNT(*) AS cnt
FROM title_genres tg
JOIN genres g ON tg.genre_id = g.genre_id
GROUP BY g.name
ORDER BY cnt DESC
LIMIT 10;

SELECT release_year, COUNT(*) AS cnt
FROM titles
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year;

SELECT p.name AS director, COUNT(*) AS cnt
FROM title_cast tc
JOIN people p ON tc.person_id = p.person_id
WHERE tc.role_type = 'director'
GROUP BY p.name
ORDER BY cnt DESC
LIMIT 10;

SELECT t.title, t.release_year, group_concat(DISTINCT g.name) AS genres, group_concat(DISTINCT pc.name) AS countries
FROM titles t
LEFT JOIN title_genres tg ON t.title_id = tg.title_id
LEFT JOIN genres g ON tg.genre_id = g.genre_id
LEFT JOIN title_cast tc ON t.title_id = tc.title_id AND tc.role_type = 'country'
LEFT JOIN people pc ON tc.person_id = pc.person_id
GROUP BY t.title_id
LIMIT 50;
