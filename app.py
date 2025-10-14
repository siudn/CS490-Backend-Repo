from flask import Flask, jsonify, request
import mysql.connector as mc
import os
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def db():
    return mc.connect(
        host=os.getenv("DB_HOST","localhost"),
        user=os.getenv("DB_USER","root"),
        password=os.getenv("DB_PASS",""),
        database=os.getenv("DB_NAME","sakila")
    )

def ensure_city(conn, city: str, country_id: int) -> int:
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT city_id FROM city WHERE city=%s AND country_id=%s LIMIT 1", (city, country_id))
    row = cur.fetchone()
    if row: 
        cur.close(); 
        return row["city_id"]
    cur2 = conn.cursor()
    cur2.execute("INSERT INTO city (city, country_id, last_update) VALUES (%s,%s,NOW())", (city, country_id))
    conn.commit()
    cid = cur2.lastrowid
    cur.close(); cur2.close()
    return cid

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
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    offset = (page - 1) * limit

    base = """
      SELECT c.customer_id, c.first_name, c.last_name, c.email, c.active
      FROM customer c
    """
    where = []
    params = []

    if q:
        if q.isdigit():
            where.append("c.customer_id = %s")
            params.append(int(q))

        where.append("c.first_name LIKE %s")
        params.append(f"%{q}%")
        where.append("c.last_name LIKE %s")
        params.append(f"%{q}%")

        where.append("CONCAT_WS(' ', c.first_name, c.last_name) LIKE %s")
        params.append(f"%{q}%")
        where.append("CONCAT_WS(' ', c.last_name, c.first_name) LIKE %s")
        params.append(f"%{q}%")

        parts = q.split()
        if len(parts) == 2:
            where.append("(c.first_name LIKE %s AND c.last_name LIKE %s)")
            params.extend([f"%{parts[0]}%", f"%{parts[1]}%"])

    sql = base
    if where:
        sql += " WHERE " + " OR ".join(f"({w})" for w in where)
    sql += " ORDER BY c.last_name, c.first_name LIMIT %s OFFSET %s"

    conn = db(); cur = conn.cursor(dictionary=True)
    cur.execute(sql, (*params, limit, offset))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify({"data": rows, "page": page, "limit": limit})

@app.post("/api/rent")
def rent_film():
    data = request.get_json(force=True)
    film_id = int(data.get("film_id"))
    customer_id = int(data.get("customer_id"))
    staff_id = 1
    conn = db(); cur = conn.cursor(dictionary=True)

    cur.execute("""
      SELECT i.inventory_id
      FROM inventory i
      LEFT JOIN rental r
        ON r.inventory_id = i.inventory_id AND r.return_date IS NULL
      WHERE i.film_id = %s AND r.rental_id IS NULL
      LIMIT 1
    """, (film_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return jsonify({"error":"No available copies to rent"}), 409

    inventory_id = row["inventory_id"]

    cur.execute("""
      INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
      VALUES (NOW(), %s, %s, NULL, %s, NOW())
    """, (inventory_id, customer_id, staff_id))
    conn.commit()
    rental_id = cur.lastrowid
    cur.close(); conn.close()
    return jsonify({"rental_id": rental_id, "inventory_id": inventory_id}), 201

@app.get("/api/countries")
def countries():
    conn = db(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT country_id, country FROM country ORDER BY country")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify(rows)

@app.get("/api/customers/<int:customer_id>")
def get_customer(customer_id):
    sql = """
    SELECT c.customer_id, c.first_name, c.last_name, c.email, c.active,
           a.address_id, a.address, a.address2, a.district, a.postal_code, a.phone,
           ci.city_id, ci.city, co.country_id, co.country
    FROM customer c
    JOIN address a ON a.address_id = c.address_id
    JOIN city ci ON ci.city_id = a.city_id
    JOIN country co ON co.country_id = ci.country_id
    WHERE c.customer_id=%s
    """
    conn=db(); cur=conn.cursor(dictionary=True)
    cur.execute(sql,(customer_id,))
    row=cur.fetchone()
    cur.close(); conn.close()
    return (jsonify(row),200) if row else (jsonify({"error":"Not found"}),404)

@app.post("/api/customers")
def add_customer():
    d = request.get_json(force=True)
    conn=db()
    try:
        city_id = int(d["city_id"]) if d.get("city_id") else ensure_city(conn, d["city"], int(d["country_id"]))
        cur = conn.cursor()
        cur.execute("""
          INSERT INTO address(address,address2,district,city_id,postal_code,phone,location,last_update)
          VALUES (%s,%s,%s,%s,%s,%s,POINT(0,0),NOW())
        """, (d["address"], d.get("address2",""), d["district"], city_id, d.get("postal_code",""), d.get("phone","")))
        addr_id = cur.lastrowid

        cur.execute("""
          INSERT INTO customer(store_id,first_name,last_name,email,address_id,active,last_update)
          VALUES (%s,%s,%s,%s,%s,1,NOW())
        """, (int(d.get("store_id",1)), d["first_name"], d["last_name"], d.get("email",""), addr_id))
        conn.commit()
        return jsonify({"customer_id": cur.lastrowid, "address_id": addr_id}), 201
    finally:
        conn.close()

@app.put("/api/customers/<int:customer_id>")
def update_customer(customer_id):
    d = request.get_json(force=True)
    conn=db(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT address_id FROM customer WHERE customer_id=%s",(customer_id,))
        row = cur.fetchone()
        if not row: return jsonify({"error":"Not found"}),404
        addr_id = row["address_id"]

        # city (existing or create)
        city_id = int(d["city_id"]) if d.get("city_id") else ensure_city(conn, d["city"], int(d["country_id"]))

        cur2 = conn.cursor()
        cur2.execute("""
          UPDATE customer
          SET first_name=%s,last_name=%s,email=%s,active=%s,last_update=NOW()
          WHERE customer_id=%s
        """, (d["first_name"], d["last_name"], d.get("email",""), int(d.get("active",1)), customer_id))
        cur2.execute("""
          UPDATE address
          SET address=%s,address2=%s,district=%s,city_id=%s,postal_code=%s,phone=%s,last_update=NOW()
          WHERE address_id=%s
        """, (d["address"], d.get("address2",""), d["district"], city_id, d.get("postal_code",""), d.get("phone",""), addr_id))
        conn.commit()
        cur2.close()
        return jsonify({"message":"updated"})
    finally:
        cur.close(); conn.close()

@app.delete("/api/customers/<int:customer_id>")
def delete_customer(customer_id):
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("START TRANSACTION")
        cur.execute("DELETE FROM payment WHERE customer_id=%s",(customer_id,))
        cur.execute("DELETE FROM rental  WHERE customer_id=%s",(customer_id,))
        cur.execute("DELETE FROM customer WHERE customer_id=%s",(customer_id,))
        conn.commit()
        return jsonify({"message":"deleted"})
    finally:
        cur.close(); conn.close()

@app.get("/api/customers/<int:customer_id>/rentals")
def customer_rentals(customer_id):
    sql = """
    SELECT r.rental_id, r.rental_date, r.return_date,
           f.film_id, f.title
    FROM rental r
    JOIN inventory i ON i.inventory_id = r.inventory_id
    JOIN film f      ON f.film_id = i.film_id
    WHERE r.customer_id = %s
    ORDER BY r.rental_date DESC
    """
    conn=db(); cur=conn.cursor(dictionary=True)
    cur.execute(sql, (customer_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    open_rentals = [x for x in rows if x["return_date"] is None]
    returned     = [x for x in rows if x["return_date"] is not None]
    return jsonify({"open": open_rentals, "returned": returned})

@app.post("/api/rentals/<int:rental_id>/return")
def mark_returned(rental_id):
    conn=db(); cur=conn.cursor()
    cur.execute("""
        UPDATE rental
        SET return_date = NOW()
        WHERE rental_id = %s AND return_date IS NULL
    """, (rental_id,))
    conn.commit()
    changed = cur.rowcount
    cur.close(); conn.close()
    if changed == 0:
        return jsonify({"message":"No open rental found for this id"}), 404
    return jsonify({"message":"Rental marked returned", "rental_id": rental_id})

if __name__ == "__main__":
    app.run(port=5000, debug=True)
