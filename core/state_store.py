import sqlite3

class StateStore:
    def __init__(self, db_path="state.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_articles (
            id TEXT PRIMARY KEY
        )
        """)

    def is_new(self, article_id: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM seen_articles WHERE id=?", (article_id,)
        )
        return cur.fetchone() is None

    def mark_seen(self, article_id: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO seen_articles VALUES (?)",
            (article_id,)
        )
        self.conn.commit()
