import sqlite3


def setup_db():
    conn = sqlite3.connect('settings.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY, theme TEXT)''')
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS names (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, avatar BLOB)''')
    cursor.execute('''SELECT theme FROM settings WHERE id = 1''')
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute('''INSERT INTO settings (id, theme) VALUES (1, 'cyborg')''')
        conn.commit()
        return 'cyborg'


def update_theme_in_db(theme):
    conn = sqlite3.connect('settings.db')
    cursor = conn.cursor()
    cursor.execute('''UPDATE settings SET theme = ? WHERE id = 1''', (theme,))
    conn.commit()


def load_saved_names():
    conn = sqlite3.connect('settings.db')
    cursor = conn.cursor()
    cursor.execute('SELECT name, avatar FROM names')
    return cursor.fetchall()


def save_name_in_db(name, avatar_bytes):
    conn = sqlite3.connect('settings.db')
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT INTO names (name, avatar) VALUES (?, ?)', (name, avatar_bytes))
        conn.commit()
    except sqlite3.IntegrityError:
        raise Exception("Duplicate name")


def clear_all_names():
    """
    Delete all rows from the names table in the database.
    """
    conn = sqlite3.connect('settings.db')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM names')  # Remove all names from the table
    conn.commit()
