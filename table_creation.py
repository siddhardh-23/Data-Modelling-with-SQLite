import sqlite3
from commands import create_table_queries, drop_table_queries


def create_database():
    # Connect to SQLite (this creates a database file 'sparkifydb.db' if it doesn't exist)
    conn = sqlite3.connect('outputdb.db')
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    print("Tables dropped successfully!!")

    create_tables(cur, conn)
    print("Tables created successfully!!")

    conn.close()


if __name__ == "__main__":
    main()
