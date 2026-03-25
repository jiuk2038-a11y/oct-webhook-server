"""
OCT Webhook Server (Cloud Version)
- 아임웹 입력폼에서 웹훅으로 리드(이름/전화번호)를 수신
- SQLite에 저장하면서 전화번호 중복 체크
- 신규 리드만 Google Ads 오프라인 전환(OCT)으로 업로드
"""

import datetime
import sqlite3
import hashlib
import os
import yaml
import tempfile
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# ── 설정 (환경변수에서 읽기) ──
CUSTOMER_ID = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "1972486360")
CONVERSION_ACTION_ID = os.environ.get("GOOGLE_ADS_CONVERSION_ACTION_ID", "7547978098")

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "leads.db")

# Google Ads YAML을 환경변수에서 생성
def get_google_ads_yaml_path():
    config = {
        "developer_token": os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_ADS_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_ADS_CLIENT_SECRET", ""),
        "refresh_token": os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", ""),
        "login_customer_id": CUSTOMER_ID,
        "use_proto_plus": True,
    }
    path = os.path.join(tempfile.gettempdir(), "google-ads.yaml")
    with open(path, "w") as f:
        yaml.dump(config, f)
    return path

YAML_FILE = get_google_ads_yaml_path()

app = FastAPI(title="Implant OCT Webhook Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── SQLite DB ──
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            phone TEXT UNIQUE,
            phone_hash TEXT,
            created_at TEXT,
            oct_sent INTEGER DEFAULT 0,
            oct_sent_at TEXT
        )
    """)
    existing = {row[1] for row in c.execute("PRAGMA table_info(leads)").fetchall()}
    migrations = {
        "phone_hash": "ALTER TABLE leads ADD COLUMN phone_hash TEXT",
        "oct_sent": "ALTER TABLE leads ADD COLUMN oct_sent INTEGER DEFAULT 0",
        "oct_sent_at": "ALTER TABLE leads ADD COLUMN oct_sent_at TEXT",
    }
    for col, sql in migrations.items():
        if col not in existing:
            c.execute(sql)
    conn.commit()
    conn.close()


def is_duplicate(phone: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id FROM leads WHERE phone = ?", (phone,))
    row = c.fetchone()
    conn.close()
    return row is not None


def save_lead(name: str, phone: str) -> int:
    now = datetime.datetime.now().isoformat()
    phone_hash = hashlib.sha256(phone.encode()).hexdigest()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT INTO leads (name, phone, phone_hash, created_at) VALUES (?, ?, ?, ?)",
        (name, phone, phone_hash, now),
    )
    lead_id = c.lastrowid
    conn.commit()
    conn.close()
    return lead_id


def mark_oct_sent(lead_id: int):
    now = datetime.datetime.now().isoformat()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE leads SET oct_sent = 1, oct_sent_at = ? WHERE id = ?", (now, lead_id))
    conn.commit()
    conn.close()


# ── Google Ads OCT 업로드 ──
def normalize_phone(phone: str) -> str:
    digits = "".join(filter(str.isdigit, phone))
    if digits.startswith("010"):
        digits = "82" + digits[1:]
    elif digits.startswith("0"):
        digits = "82" + digits[1:]
    return "+" + digits


def upload_oct(phone: str, conversion_time: str):
    client = GoogleAdsClient.load_from_storage(YAML_FILE)
    conversion_upload_service = client.get_service("ConversionUploadService")
    conversion_action_service = client.get_service("ConversionActionService")

    conversion_action_resource = conversion_action_service.conversion_action_path(
        CUSTOMER_ID, CONVERSION_ACTION_ID
    )

    click_conversion = client.get_type("ClickConversion")
    click_conversion.conversion_action = conversion_action_resource
    click_conversion.conversion_date_time = conversion_time

    normalized = normalize_phone(phone)
    phone_hash = hashlib.sha256(normalized.encode()).hexdigest()

    user_identifier = client.get_type("UserIdentifier")
    user_identifier.hashed_phone_number = phone_hash
    click_conversion.user_identifiers.append(user_identifier)

    request = client.get_type("UploadClickConversionsRequest")
    request.customer_id = CUSTOMER_ID
    request.conversions.append(click_conversion)
    request.partial_failure = True

    response = conversion_upload_service.upload_click_conversions(request=request)

    pfe = response.partial_failure_error
    if pfe and pfe.message:
        print(f"[OCT] 부분 실패: {pfe}")
        raise Exception(f"OCT 부분 실패: {pfe.message}")

    print(f"[OCT] 응답 results 수: {len(response.results)}")
    for i, result in enumerate(response.results):
        print(f"[OCT] result[{i}]: conversion_action={result.conversion_action}")

    return response


# ── API 엔드포인트 ──
@app.on_event("startup")
def startup():
    init_db()
    print(f"[서버] DB 초기화 완료")
    print(f"[서버] Google Ads 고객 ID: {CUSTOMER_ID}")
    print(f"[서버] 전환 액션 ID: {CONVERSION_ACTION_ID}")


@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON 파싱 실패")

    name = body.get("name", "").strip()
    phone = body.get("phone", "").strip()
    phone = "".join(filter(str.isdigit, phone))

    if not phone:
        raise HTTPException(status_code=400, detail="전화번호가 없습니다")

    if is_duplicate(phone):
        return {"status": "duplicate", "message": "이미 등록된 전화번호입니다"}

    lead_id = save_lead(name, phone)
    print(f"[웹훅] 신규 리드: #{lead_id} {name} / ***{phone[-4:]}")

    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    conversion_time = now.strftime("%Y-%m-%d %H:%M:%S+09:00")

    try:
        upload_oct(phone, conversion_time)
        mark_oct_sent(lead_id)
        oct_status = "sent"
    except GoogleAdsException as gae:
        err_details = [error.message for error in gae.failure.errors]
        oct_status = f"google_ads_error: {'; '.join(err_details)}"
    except Exception as e:
        oct_status = f"failed: {e}"

    return {"status": "ok", "lead_id": lead_id, "name": name, "phone_last4": phone[-4:], "oct": oct_status}


@app.post("/webhook/imweb")
async def receive_imweb_webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON 파싱 실패")

    print(f"[아임웹] 수신 데이터: {body}")

    name = ""
    phone = ""
    phone_parts = {}

    skip_keys = {"board_code", "board_name", "unit_code", "widget_code", "write_token", "write_token_key"}

    for k, v in body.items():
        val = str(v).strip() if v else ""
        if not val or k in skip_keys:
            continue

        if k.startswith("phonenumber1_"):
            phone_parts[1] = val
        elif k.startswith("phonenumber2_"):
            phone_parts[2] = val
        elif k.startswith("phonenumber3_"):
            phone_parts[3] = val
        elif k.startswith("input_") and not name:
            name = val

    if phone_parts:
        phone = phone_parts.get(1, "") + phone_parts.get(2, "") + phone_parts.get(3, "")

    phone = "".join(filter(str.isdigit, phone))

    if not phone:
        return {"status": "error", "message": "전화번호를 찾을 수 없습니다"}

    print(f"[아임웹] 추출 → 이름: {name}, 전화번호: ***{phone[-4:]}")

    if is_duplicate(phone):
        return {"status": "duplicate", "message": "이미 등록된 전화번호입니다"}

    lead_id = save_lead(name, phone)
    print(f"[아임웹] 신규 리드: #{lead_id} {name} / ***{phone[-4:]}")

    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    conversion_time = now.strftime("%Y-%m-%d %H:%M:%S+09:00")

    try:
        upload_oct(phone, conversion_time)
        mark_oct_sent(lead_id)
        oct_status = "sent"
    except GoogleAdsException as gae:
        err_details = [error.message for error in gae.failure.errors]
        oct_status = f"google_ads_error: {'; '.join(err_details)}"
    except Exception as e:
        oct_status = f"failed: {e}"

    return {"status": "ok", "lead_id": lead_id, "name": name, "phone_last4": phone[-4:], "oct": oct_status}


@app.get("/leads")
def get_leads():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, name, phone, oct_sent, created_at FROM leads ORDER BY id DESC LIMIT 100")
    rows = c.fetchall()
    conn.close()
    return [
        {"id": r[0], "name": r[1], "phone_last4": r[2][-4:] if r[2] else "", "oct_sent": bool(r[3]), "created_at": r[4]}
        for r in rows
    ]


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.datetime.now().isoformat()}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
