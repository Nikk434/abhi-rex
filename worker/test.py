from ingest.db import init_db
from app.config import INGEST_OUT_DIR, DB_FILENAME

conn = init_db(INGEST_OUT_DIR / DB_FILENAME)
cur = conn.cursor()

# cur.execute("""
# INSERT INTO ingest_jobs(payload)
# VALUES (
#   '{"video":"C:/Users/Nikhil/Videos/Screen Recordings/Screen Recording 2026-01-11 131818.mp4","metadata":{"title":"Worker Test 001"}}'
# );
# """)
cur.execute("""
SELECT * FROM jobs;
""")
rows = cur.fetchall()
for row in rows:
    print(row)

# print(rows)

conn.commit()
conn.close()
print("Done")