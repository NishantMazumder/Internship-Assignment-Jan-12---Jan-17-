import sqlite3
import time
import threading
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- Database Setup ---
def init_db():
    conn = sqlite3.connect('ledger.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL,
            role TEXT
        )
    ''')
    users = [
        (1, 'alice', 100.0, 'user'),
        (2, 'bob', 50.0, 'user'),
        (3, 'admin', 9999.0, 'admin'),
        (4, 'charlie', 10.0, 'user')
    ]
    c.executemany(
        "INSERT OR IGNORE INTO users (id, username, balance, role) VALUES (?, ?, ?, ?)",
        users
    )
    conn.commit()
    conn.close()

init_db()

def background_worker(user_id, amount):
    """
    This function runs in a separate thread.
    It handles the slow banking delay and then updates the database atomically.
    """
    print(f"[Worker] Starting background process for User {user_id}...")
    # simulate a delay in connecting to bank api
    time.sleep(5)

    conn = None
    try:
        # Open a new connection for every thread
        conn = sqlite3.connect('ledger.db', timeout=10)
        cursor = conn.cursor()

        # Begin atomic transaction
        conn.execute("BEGIN")

        # Conditional atomic update
        cursor.execute(
            """
            UPDATE users
            SET balance = balance - ?
            WHERE id = ? AND balance >= ?
            """,
            (amount, user_id, amount)
        )

        if cursor.rowcount == 0:
            # Logical failure results in rollback
            raise ValueError("Insufficient funds or invalid user")

        # Commit only if everything succeeded
        conn.commit()
        print(f"[Worker] Success: Deducted {amount} from User {user_id}")

    except Exception as e:
        # Rollback guarantees atomicity
        if conn:
            conn.rollback()
        print(f"[Worker] Transaction rolled back: {e}")

    finally:
        if conn:
            conn.close()

@app.route('/transaction', methods=['POST'])
def process_transaction():
    """
    Responsive endpoint: Hands work to a thread and returns immediately.
    """
    data = request.json
    user_id = data.get('user_id')
    amount = data.get('amount')

    if not user_id or amount is None or amount <= 0:
        return jsonify({"error": "Invalid input"}), 400

    # Start background thread
    thread = threading.Thread(
        target=background_worker,
        args=(user_id, amount),
        daemon=True
    )
    thread.start()

    # Return immediately
    return jsonify({
        "status": "Accepted",
        "message": "Transaction is being processed.",
        "user_id": user_id
    }), 202

@app.route('/search', methods=['GET'])
def search_users():
    query = request.args.get('q')
    if not query:
        return jsonify({"error": "Missing query parameter"}), 400

    conn = sqlite3.connect('ledger.db')
    cursor = conn.cursor()
    
    #parameterised query to prevent SQL Injection
    cursor.execute(
        "SELECT id, username, role FROM users WHERE username = ?",
        (query,)
    )
    results = cursor.fetchall()
    conn.close()

    data = [
        {"id": r[0], "username": r[1], "role": r[2]}
        for r in results
    ]
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
