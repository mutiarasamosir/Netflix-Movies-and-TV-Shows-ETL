import matplotlib.pyplot as plt
import sqlite3
import pandas as pd

# 1. Trend jumlah rilisan per tahun
conn = sqlite3.connect('netflix.db')
df = pd.read_sql_query("""
SELECT release_year, COUNT(*) as cnt
FROM titles
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year
""", conn)

plt.figure(figsize=(10,4))
plt.plot(df['release_year'], df['cnt'])
plt.title("Jumlah Rilisan per Tahun")
plt.xlabel("Release Year")
plt.ylabel("Jumlah")
plt.show()

# 2. Top 10 genres
df = pd.read_sql_query("""
SELECT g.name AS genre, COUNT(*) AS cnt
FROM title_genres tg
JOIN genres g ON tg.genre_id = g.genre_id
GROUP BY g.name
ORDER BY cnt DESC
LIMIT 10
""", conn)

plt.figure(figsize=(8,5))
plt.bar(df['genre'], df['cnt'])
plt.xticks(rotation=45, ha='right')
plt.title("Top 10 Genres")
plt.tight_layout()
plt.show()

# 3. Distribusi durasi
df = pd.read_sql_query("SELECT duration FROM titles WHERE duration IS NOT NULL", conn)
df['num'] = df['duration'].str.extract(r'(\d+)').astype(float)

plt.figure(figsize=(8,4))
plt.hist(df['num'].dropna(), bins=30)
plt.title("Distribusi Durasi (numeric)")
plt.xlabel("Durasi (number)")
plt.ylabel("Frekuensi")
plt.show()
