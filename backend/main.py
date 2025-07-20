import os
import uuid
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from pydantic import BaseModel
from typing import Dict

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors.rpcerrorlist import SessionPasswordNeededError, PhoneCodeInvalidError, PasswordHashInvalidError

load_dotenv()

# --- Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

if not all([SUPABASE_SERVICE_KEY, TELEGRAM_API_ID, TELEGRAM_API_HASH]):
    raise ValueError("FATAL: Required environment variables are not set.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

LOGIN_SESSIONS: Dict[str, Dict] = {}

# --- Pydantic Models for Phone Auth ---
class InitiateRequest(BaseModel): apiId: str; apiHash: str
class PhoneRequest(BaseModel): sessionToken: str; phoneNumber: str
class CodeRequest(BaseModel): sessionToken: str; loginCode: str
class TwoFaRequest(BaseModel): sessionToken: str; password2FA: str

async def finalize_login(client: TelegramClient, profile_id: str, session_token: str):
    """Saves the session and disconnects the client after a successful login."""
    session_string = client.session.save()
    me = await client.get_me()
    account_payload = {
        "id": str(me.id),
        "username": me.username or "N/A",
        "phone_number": f"+{me.phone}" if me.phone else None,
        "status": "connected",
        "linked_by": profile_id,
        "session_string": session_string
    }
    supabase.table("managed_telegram_accounts").upsert(account_payload).execute()
    await client.disconnect()
    if session_token in LOGIN_SESSIONS:
        del LOGIN_SESSIONS[session_token]
    print(f"Successfully finalized and saved session for {me.username}")

@app.post("/api/telegram/auth/initiate")
async def auth_initiate(req: InitiateRequest, http_request: Request):
    user = supabase.auth.get_user(http_request.headers['authorization'].split(' ')[1])
    session_token = str(uuid.uuid4())
    client = TelegramClient(StringSession(), int(req.apiId), req.apiHash)
    await client.connect()
    LOGIN_SESSIONS[session_token] = { "client": client, "profile_id": user.user.id }
    return {"success": True, "sessionToken": session_token, "nextStep": "phone"}

@app.post("/api/telegram/auth/submit-phone")
async def auth_submit_phone(req: PhoneRequest):
    session = LOGIN_SESSIONS.get(req.sessionToken)
    if not session: raise HTTPException(404, "Session expired or invalid.")
    client = session["client"]
    result = await client.send_code_request(req.phoneNumber)
    session["phone_code_hash"] = result.phone_code_hash
    session["phone_number"] = req.phoneNumber
    return {"success": True, "message": "Code sent.", "nextStep": "code"}

@app.post("/api/telegram/auth/submit-code")
async def auth_submit_code(req: CodeRequest):
    session = LOGIN_SESSIONS.get(req.sessionToken)
    if not session: raise HTTPException(404, "Session expired or invalid.")
    client = session["client"]
    try:
        await client.sign_in(session["phone_number"], req.loginCode, phone_code_hash=session["phone_code_hash"])
        await finalize_login(client, session["profile_id"], req.sessionToken)
        return {"success": True, "message": "Login successful!", "nextStep": "connected"}
    except SessionPasswordNeededError:
        return {"success": True, "message": "2FA required.", "nextStep": "2fa"}
    except PhoneCodeInvalidError:
        raise HTTPException(400, "The login code you provided is invalid.")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/telegram/auth/submit-2fa")
async def auth_submit_2fa(req: TwoFaRequest):
    session = LOGIN_SESSIONS.get(req.sessionToken)
    if not session: raise HTTPException(404, "Session expired or invalid.")
    client = session["client"]
    try:
        await client.sign_in(password=req.password2FA)
        await finalize_login(client, session["profile_id"], req.sessionToken)
        return {"success": True, "message": "Successfully connected!", "nextStep": "connected"}
    except PasswordHashInvalidError:
        raise HTTPException(400, "Incorrect 2FA password.")
    except Exception as e:
        raise HTTPException(500, str(e))