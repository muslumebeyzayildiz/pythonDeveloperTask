#Excel dosyasından resimleri MinIO'ya yükler.

import requests
import pandas as pd
import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
load_dotenv()
MINIO_URL = os.getenv("MINIO_URL", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadminv4")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminv4")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "interpol-images-v4")

# MinIO istemcisi
client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Bucket varsa oluşturma
if not client.bucket_exists(MINIO_BUCKET):
    client.make_bucket(MINIO_BUCKET)

# Excel dosyasını oku
df = pd.read_excel("Interpol_Tum_Kayitlar.xlsx")

# Header bilgisi
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*"
}

# Her bir kayıt için işlem
for idx, row in df.iterrows():
    detail_url = row["detail_link"]
    entity_id = row["entity_id"]

    response = requests.get(detail_url, headers=headers)
    if response.status_code != 200:
        print(f"Detay alınamadı: {entity_id}")
        continue

    data = response.json()
    image_links = data.get("_links", {}).get("images", {}).get("href", "")
    if not image_links:
        print(f"Resim yok: {entity_id}")
        continue

    img_resp = requests.get(image_links, headers=headers)
    if img_resp.status_code != 200:
        print(f"Fotoğraf bağlantısı alınamadı: {entity_id}")
        continue

    img_data = img_resp.json()
    embedded_images = img_data.get("_embedded", {}).get("images", [])

    for i, img in enumerate(embedded_images):
        picture_url = img.get("_links", {}).get("self", {}).get("href", "")
        if not picture_url:
            continue

        pic_resp = requests.get(picture_url, headers=headers)
        if pic_resp.status_code == 200:
            safe_entity_id = entity_id.replace("/", "_")
            file_name = f"{safe_entity_id}_{i + 1}.jpg"
            local_path = os.path.join("temp", file_name)
            os.makedirs("temp", exist_ok=True)

            with open(local_path, "wb") as file:
                file.write(pic_resp.content)

            try:
                client.fput_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=file_name,
                    file_path=local_path,
                    content_type="image/jpeg"
                )
                print(f"{file_name} başarıyla yüklendi.")
            except S3Error as e:
                print(f"[HATA] {file_name} yüklenemedi:", e)

            os.remove(local_path)
