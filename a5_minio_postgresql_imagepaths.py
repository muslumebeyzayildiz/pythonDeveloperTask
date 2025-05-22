import os
import json
import psycopg2
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

# Veritabanı bağlantısı
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        client_encoding="utf8"
    )

# MinIO ayarları
minio_url = os.getenv("MINIO_URL", "localhost:9000")
minio_bucket = os.getenv("MINIO_BUCKET", "interpol-images-v4")

# MinIO istemcisi
client = Minio(
    minio_url,
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

conn = get_db_connection()
cur = conn.cursor()

# Tüm entity_id'leri veritabanından al
cur.execute("SELECT entity_id FROM notices")
entity_ids = cur.fetchall()

for (entity_id,) in entity_ids:
    safe_entity_id = entity_id.replace("/", "_")

    # MinIO'dan resimleri kontrol et
    image_paths = []
    idx = 1
    while True:
        file_name = f"{safe_entity_id}_{idx}.jpg"
        try:
            client.stat_object(minio_bucket, file_name)
            image_url = f"http://{minio_url}/{minio_bucket}/{file_name}"
            image_paths.append(image_url)
            idx += 1
        except:
            break

    # Resim yoksa boş liste ekle
    if not image_paths:
        print(f"[❌] Resim bulunamadı: {entity_id}")
        continue

    # image_paths sütununu JSON olarak güncelle
    cur.execute("""
        UPDATE notices
        SET image_paths = %s
        WHERE entity_id = %s
    """, (image_paths, entity_id))

    print(f"[🖼️] image_paths güncellendi: {entity_id}")

conn.commit()
cur.close()
conn.close()