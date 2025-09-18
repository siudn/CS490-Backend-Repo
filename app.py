from flask import Flask, jsonify
from flask_cors import CORS
import os, mysql.connector as mc

app = Flask(__name__)
CORS(app)

def db():
    return mc.connect(
        host=os.getenv("DB_HOST","localhost"),
        user=os.getenv("DB_USER","root"),
        password=os.getenv("DB_PASS",""),
        database=os.getenv("DB_NAME","sakila")
    )

@app.get("/api/films/top")
def top_films():
    sql = """
    SELECT f.film_id, f.title, COUNT(r.rental_id) rentals
    FROM rental r
    JOIN inventory i ON r.inventory_id=i.inventory_id
    JOIN film f ON i.film_id=f.film_id
    GROUP BY f.film_id, f.title
    ORDER BY rentals DESC, f.title
    LIMIT 5;
    """
    conn=db(); cur=conn.cursor(dictionary=True); cur.execute(sql)
    rows=cur.fetchall(); cur.close(); conn.close()
    return jsonify(rows)

@app.get("/api/films/<int:film_id>")
def film_details(film_id):
    sql_film = """
    SELECT f.film_id, f.title, f.description, f.length, f.rating,
           l.name language, c.name category
    FROM film f
    JOIN language l ON f.language_id=l.language_id
    LEFT JOIN film_category fc ON fc.film_id=f.film_id
    LEFT JOIN category c ON c.category_id=fc.category_id
    WHERE f.film_id=%s
    """
    sql_actors = """
    SELECT a.actor_id, CONCAT(a.first_name,' ',a.last_name) name
    FROM actor a JOIN film_actor fa ON fa.actor_id=a.actor_id
    WHERE fa.film_id=%s ORDER BY name
    """
    conn=db(); cur=conn.cursor(dictionary=True)
    cur.execute(sql_film,(film_id,)); film=cur.fetchone()
    cur.execute(sql_actors,(film_id,)); actors=cur.fetchall()
    cur.close(); conn.close()
    return jsonify({"film": film, "actors": actors})

if __name__ == "__main__":
    app.run(port=5000, debug=True)
