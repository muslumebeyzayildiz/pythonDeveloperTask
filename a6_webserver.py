#Flask web uygulamasını başlatır, sonuçları web arayüzünde gösterir.


from flask import Flask, render_template_string, redirect, url_for
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

# Veritabanı bağlantısı
def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        client_encoding="utf8"
    )
    return conn

@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT entity_id, forename, name, date_of_birth, age, sex, nationalities, detail_link, image_path, created_at, last_updated_at
        FROM notices
        ORDER BY entity_id;
    """)
    notices = cur.fetchall()
    cur.close()
    conn.close()

    display_notices = []
    for notice in notices:
        status = ""
        if notice[8] == notice[9]:  # created_at == last_updated_at
            status = "NEW"
        else:
            status = "UPDATED"

        display_notices.append({
            "entity_id": notice[0],
            "forename": notice[1],
            "name": notice[2],
            "date_of_birth": notice[3],
            "age": notice[4],
            "sex": notice[5],
            "nationalities": notice[6],
            "detail_link": notice[7],
            "image_path": notice[8],#.replace("9000", "9001") if notice[8] else "",
            "status": status
        })

    return render_template_string('''
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Interpol Red Notices</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
        <meta http-equiv="refresh" content="600">
      </head>
      <body class="bg-light">
        <div class="container mt-4">
          <div class="d-flex justify-content-between align-items-center mb-4">
            <h1 class="text-center flex-grow-1">Interpol Red Notices</h1>
            <a href="{{ url_for('index') }}" class="btn btn-primary">Refresh</a>
          </div>
          <div class="row row-cols-1 row-cols-md-3 g-4">
            {% for notice in notices %}
            <div class="col">
              <div class="card h-100">
                <img src="{{ notice.image_path }}" class="card-img-top" alt="Fotoğraf yok" style="height: 250px; object-fit: cover;">
                <div class="card-body">
                  <h5 class="card-title">{{ notice.name }} {{ notice.forename }}</h5>
                  <p class="card-text">
                    <strong>Doğum:</strong> {{ notice.date_of_birth }}<br>
                    <strong>Yaş:</strong> {{ notice.age }}<br>
                    <strong>Cinsiyet:</strong> {{ notice.sex }}<br>
                    <strong>Uyruk:</strong> {{ notice.nationalities }}
                  </p>
                  <a href="{{ notice.detail_link }}" class="btn btn-sm btn-outline-secondary" target="_blank">Detay</a>
                  {% if notice.status == 'NEW' %}
                    <span class="badge bg-success float-end">NEW</span>
                  {% elif notice.status == 'UPDATED' %}
                    <span class="badge bg-danger float-end">UPDATED</span>
                  {% endif %}
                </div>
              </div>
            </div>
            {% endfor %}
          </div>
        </div>
      </body>
    </html>
    ''', notices=display_notices)


if __name__ == '__main__':
    app.run(debug=True)