#RabbitMQ'dan verileri alır ve yeni PostgreSQL veritabanına kaydeder.

import pika
import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
queue_name = os.getenv("RABBITMQ_QUEUE", "interpol_notices_v4")

#veritabanına bağlan
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        client_encoding="utf8"
    )
# tabloyu otomatik oluştur (varsa atla)
def init_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS notices (
        entity_id TEXT PRIMARY KEY,
        forename TEXT,
        name TEXT,
        date_of_birth TEXT,
        age INTEGER,
        sex TEXT,
        nationalities TEXT,
        detail_link TEXT,
        image_path TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

# RabbitMQ mesajı alındığında çalışır
def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"[📩] Mesaj alındı:\n{json.dumps(data, indent=2, ensure_ascii=False)}\n")

    conn = get_db_connection()
    cur = conn.cursor()

    # Önce entity_id varsa kayıtlı mı diye bakalım
    cur.execute("SELECT forename, name, date_of_birth, age, sex, nationalities, detail_link FROM notices WHERE entity_id = %s", (data.get("entity_id"),))
    existing = cur.fetchone()

    if existing:
        # Eğer veri değişmişse UPDATE yap
        if (
            existing[0] != data.get("forename") or
            existing[1] != data.get("name") or
            existing[2] != data.get("date_of_birth") or
            existing[3] != data.get("age") or
            existing[4] != data.get("sex") or
            existing[5] != data.get("nationalities") or
            existing[6] != data.get("detail_link")
        ):
            cur.execute("""
                UPDATE notices
                SET forename = %s,
                    name = %s,
                    date_of_birth = %s,
                    age = %s,
                    sex = %s,
                    nationalities = %s,
                    detail_link = %s,
                    last_updated_at = CURRENT_TIMESTAMP
                WHERE entity_id = %s;
            """, (
                data.get("forename"),
                data.get("name"),
                data.get("date_of_birth"),
                data.get("age"),
                data.get("sex"),
                data.get("nationalities"),
                data.get("detail_link"),
                data.get("entity_id")
            ))
            print(f"[✏️] Kayıt güncellendi: {data.get('entity_id')}")
        else:
            print(f"[ℹ️] Kayıt zaten aynı: {data.get('entity_id')}")
    else:
        # Yeni kayıt ekle
        cur.execute("""
            INSERT INTO notices (entity_id, forename, name, date_of_birth, age, sex, nationalities, detail_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            data.get("entity_id"),
            data.get("forename"),
            data.get("name"),
            data.get("date_of_birth"),
            data.get("age"),
            data.get("sex"),
            data.get("nationalities"),
            data.get("detail_link")
        ))
        print(f"[➕] Yeni kayıt eklendi: {data.get('entity_id')}")

    conn.commit()
    cur.close()
    conn.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    init_table()  # tabloyu ilk başta oluştur
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.getenv("RABBITMQ_HOST", "localhost")))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    print("[🐇] RabbitMQ kuyruğu dinleniyor ve veritabanına yazılıyor...")
    channel.start_consuming()

if __name__ == "__main__":
    main()