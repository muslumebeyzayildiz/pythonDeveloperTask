import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

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

# Excel dosyasını oku
df = pd.read_excel("Interpol_Tum_Kayitlar.xlsx")

# MinIO ayarları
minio_url = os.getenv("MINIO_URL", "localhost:9000")
minio_bucket = os.getenv("MINIO_BUCKET", "interpol-images-v4")

conn = get_db_connection()
cur = conn.cursor()

for idx, row in df.iterrows():
    entity_id = row["entity_id"].replace("/", "_")
    image_url = f"http://{minio_url}/{minio_bucket}/{entity_id}_1.jpg"

    # image_path zaten varsa atla
    cur.execute("SELECT image_path FROM notices WHERE entity_id = %s", (row["entity_id"],))
    existing = cur.fetchone()
    if existing and existing[0]:
        print(f"Zaten kayıtlı: {entity_id}")
        continue

    cur.execute("""
        UPDATE notices
        SET image_path = %s
        WHERE entity_id = %s
    """, (image_url, row["entity_id"]))

    print(f"[🖼️] image_path güncellendi: {entity_id}")

conn.commit()
cur.close()
conn.close()