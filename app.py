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

@app.get("/api/actors/top")
def top_actors():
    sql = """
    SELECT a.actor_id, CONCAT(a.first_name,' ',a.last_name) AS name,
           COUNT(r.rental_id) AS rentals
    FROM actor a
    JOIN film_actor fa ON fa.actor_id=a.actor_id
    JOIN inventory i ON i.film_id=fa.film_id
    LEFT JOIN rental r ON r.inventory_id=i.inventory_id
    GROUP BY a.actor_id, name
    ORDER BY rentals DESC, name
    LIMIT 5;
    """
    conn=db(); cur=conn.cursor(dictionary=True); cur.execute(sql)
    rows=cur.fetchall(); cur.close(); conn.close()
    return jsonify(rows)

@app.get("/api/actors/<int:actor_id>/top-films")
def actor_top_films(actor_id):
    sql_actor = "SELECT actor_id, first_name, last_name FROM actor WHERE actor_id=%s"
    sql_films = """
      SELECT f.film_id, f.title, COUNT(r.rental_id) AS rentals
      FROM film f
      JOIN film_actor fa ON fa.film_id=f.film_id AND fa.actor_id=%s
      JOIN inventory i ON i.film_id=f.film_id
      LEFT JOIN rental r ON r.inventory_id=i.inventory_id
      GROUP BY f.film_id, f.title
      ORDER BY rentals DESC, f.title
      LIMIT 5;
    """
    conn=db(); cur=conn.cursor(dictionary=True)
    cur.execute(sql_actor,(actor_id,)); actor=cur.fetchone()
    cur.execute(sql_films,(actor_id,)); films=cur.fetchall()
    cur.close(); conn.close()
    return jsonify({"actor": actor, "films": films})

@app.get("/api/search")
def search():
    q = f"%{request.args.get('q','').strip()}%"
    sql = """
    SELECT DISTINCT f.film_id, f.title
    FROM film f
    LEFT JOIN film_actor fa ON fa.film_id=f.film_id
    LEFT JOIN actor a ON a.actor_id=fa.actor_id
    LEFT JOIN film_category fc ON fc.film_id=f.film_id
    LEFT JOIN category c ON c.category_id=fc.category_id
    WHERE f.title LIKE %s
       OR CONCAT(a.first_name,' ',a.last_name) LIKE %s
       OR c.name LIKE %s
    ORDER BY f.title
    LIMIT 50;
    """
    conn=db(); cur=conn.cursor(dictionary=True)
    cur.execute(sql,(q,q,q)); rows=cur.fetchall()
    cur.close(); conn.close()
    return jsonify(rows)

@app.get("/api/customers")
def customers():
    from flask import request, jsonify
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    offset = (page - 1) * limit
    sql = """
      SELECT customer_id, first_name, last_name, email, active
      FROM customer
      ORDER BY last_name, first_name
      LIMIT %s OFFSET %s
    """
    conn = db(); cur = conn.cursor(dictionary=True)
    cur.execute(sql, (limit, offset)); rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify({"data": rows, "page": page, "limit": limit})

if __name__ == "__main__":
    app.run(port=5000, debug=True)
