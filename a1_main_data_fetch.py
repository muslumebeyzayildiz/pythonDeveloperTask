#Verileri çeker, RabbitMQ kuyruğuna gönderir, Excel'e kaydeder.
import os
import requests
import pandas as pd
import time
import math
import string
from datetime import datetime
from a2_rabbitmq_sender import send_to_queue
from dotenv import load_dotenv

load_dotenv()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.interpol.int/",
    "Origin": "https://www.interpol.int",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site"
}

seen_ids = set()
final_notices = []

def calculate_age(dob):
    try:
        birth = datetime.strptime(dob, "%Y/%m/%d")
        today = datetime.today()
        return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
    except:
        return None

def fetch_notices(params):
    base_url = "https://ws-public.interpol.int/notices/v1/red"
    all_notices = []
    params["page"] = 1
    response = requests.get(base_url, params=params, headers=headers)
    if response.status_code != 200:
        return [], 0

    data = response.json()
    total_results = data.get("total", 0)
    total_pages = math.ceil(total_results / params.get("resultPerPage", 160))
    notices = data.get("_embedded", {}).get("notices", [])
    all_notices.extend(notices)

    for page in range(2, total_pages + 1):
        params["page"] = page
        response = requests.get(base_url, params=params, headers=headers)
        if response.status_code == 200:
            page_data = response.json()
            page_notices = page_data.get("_embedded", {}).get("notices", [])
            all_notices.extend(page_notices)
        time.sleep(0.2)

    return all_notices, total_results

def process_filters(nationality):
    base_params = {"nationality": nationality, "resultPerPage": 160}
    notices, total = fetch_notices(base_params)
    if total < 160:
        return notices

    filtered_notices = []
    for sex in ["M", "F"]:
        sex_params = base_params.copy()
        sex_params["sexId"] = sex
        sex_notices, sex_total = fetch_notices(sex_params)
        if sex_total < 160:
            filtered_notices.extend(sex_notices)
        else:
            for age_min, age_max in [(0, 20), (21, 25), (26, 30),(31, 35), (36, 40), (41,45), (46,50), (51, 55), (56, 60), (61,65), (66, 70), (71,75), (76,80), (81, 85),(86,90), (91,95), (96, 100), (101,120)]:
                age_params = sex_params.copy()
                age_params["ageMin"] = age_min
                age_params["ageMax"] = age_max
                age_notices, age_total = fetch_notices(age_params)
                if age_total < 160:
                    filtered_notices.extend(age_notices)
                else:
                    for letter in string.ascii_uppercase:
                        letter_params = age_params.copy()
                        letter_params["name"] = letter
                        letter_notices, _ = fetch_notices(letter_params)
                        filtered_notices.extend(letter_notices)
    return filtered_notices

def process_record(notice):
    entity_id = notice.get("entity_id")
    if not entity_id or entity_id in seen_ids:
        return

    seen_ids.add(entity_id)

    record={
        "entity_id": entity_id,
        "forename": notice.get("forename", ""),
        "name": notice.get("name", ""),
        "date_of_birth": notice.get("date_of_birth", ""),
        "age": calculate_age(notice.get("date_of_birth", "")),
        "sex": notice.get("sex_id", ""),
        "nationalities": ", ".join(notice.get("nationalities", [])) if notice.get("nationalities") else "",
        "detail_link": notice.get("_links", {}).get("self", {}).get("href", "")
    }

    final_notices.append(record)
    send_to_queue(record)  # 🔁 Anlık olarak RabbitMQ’ya gönder

def fetch_missing_nationality():
    base_url = "https://ws-public.interpol.int/notices/v1/red"
    page = 1
    result_per_page = 160

    while True:
        params = {"page": page, "resultPerPage": result_per_page}
        response = requests.get(base_url, params=params, headers=headers)
        if response.status_code != 200:
            break
        data = response.json()
        notices = data.get("_embedded", {}).get("notices", [])
        if not notices:
            break

        for notice in notices:
            entity_id = notice.get("entity_id")
            if entity_id in seen_ids:
                continue
            detail_url = notice["_links"]["self"]["href"]
            detail_response = requests.get(detail_url, headers=headers)
            if detail_response.status_code != 200:
                continue
            detail = detail_response.json()
            if not detail.get("nationalities"):
                process_record(detail)

        print(f"Ülkesi olmayanları tarama: Sayfa {page}")
        if page >= data.get("totalPages", 0):
            break
        page += 1
        time.sleep(0.2)

def save_to_excel(filename="Interpol_Tum_Kayitlar.xlsx"):
    df = pd.DataFrame(final_notices)
    df.to_excel(filename, index=False)
    print(f"{len(df)} kayıt Excel'e kaydedildi: {filename}")

# 🌍 Ülke kodları (tam liste)
nationalities = [
    "AF", "AX", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM", "AW", "AU", "AT", "AZ", "BS", "BH", "BD", "BB",
    "BY", "BE", "BZ", "BJ", "BM", "BT", "BO", "BQ", "BA", "BW", "BV", "BR", "IO", "BN", "BG", "BF", "BI", "CV", "KH", "CM",
    "CA", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM", "CG", "CD", "CK", "CR", "CI", "HR", "CU", "CW", "CY", "CZ",
    "DK", "DJ", "DM", "DO", "EC", "EG", "SV", "GQ", "ER", "EE", "SZ", "ET", "FK", "FO", "FJ", "FI", "FR", "GF", "PF", "TF",
    "GA", "GM", "GE", "DE", "GH", "GI", "GR", "GL", "GD", "GP", "GU", "GT", "GG", "GN", "GW", "GY", "HT", "HM", "VA", "HN",
    "HK", "HU", "IS", "IN", "ID", "IR", "IQ", "IE", "IM", "IL", "IT", "JM", "JP", "JE", "JO", "KZ", "KE", "KI", "KP", "KR",
    "KW", "KG", "LA", "LV", "LB", "LS", "LR", "LY", "LI", "LT", "LU", "MO", "MG", "MW", "MY", "MV", "ML", "MT", "MH", "MQ",
    "MR", "MU", "YT", "MX", "FM", "MD", "MC", "MN", "ME", "MS", "MA", "MZ", "MM", "NA", "NR", "NP", "NL", "NC", "NZ", "NI",
    "NE", "NG", "NU", "NF", "MK", "MP", "NO", "OM", "PK", "PW", "PS", "PA", "PG", "PY", "PE", "PH", "PN", "PL", "PT", "PR",
    "QA", "RE", "RO", "RU", "RW", "BL", "SH", "KN", "LC", "MF", "PM", "VC", "WS", "SM", "ST", "SA", "SN", "RS", "SC", "SL",
    "SG", "SX", "SK", "SI", "SB", "SO", "ZA", "GS", "SS", "ES", "LK", "SD", "SR", "SJ", "SE", "CH", "SY", "TW", "TJ", "TZ",
    "TH", "TL", "TG", "TK", "TO", "TT", "TN", "TR", "TM", "TC", "TV", "UG", "UA", "AE", "GB", "US", "UM", "UY", "UZ", "VU",
    "VE", "VN", "VG", "VI", "WF", "EH", "YE", "ZM", "ZW"
]

# 🔁 Başlat
for nat in nationalities:
    country_notices = process_filters(nat)
    for notice in country_notices:
        process_record(notice)
    print(f"{nat} için veri işlendi. Toplam şu an: {len(final_notices)}")

# Ülkesi boş olanlar
fetch_missing_nationality()

# Kaydet
save_to_excel()


