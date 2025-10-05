# server_fixed_with_signalling.py
from fastapi import FastAPI, HTTPException, Body, Query
from pydantic import BaseModel
import json
import threading
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------
# Config / persistence
# ---------------------------
USERS_FILE = "users.json"
file_lock = threading.Lock()
_state_lock = threading.RLock()

# ---------- Billing/pricing constants ----------
# price units are "credits" per 30s
VRAM_PRICE_PER_GB_30S = 0.02      # credits per GB VRAM every 30s
CPU_PRICE_PER_CORE_30S = 0.005    # credits per CPU core every 30s
RAM_PRICE_PER_GB_30S = 0.001      # credits per GB RAM every 30s
GPU_PRICE_PER_GPU_30S = 0.03      # credits per GPU every 30s
SHARER_SHARE = 0.85               # fraction of fee that goes to sharer
PLATFORM_SHARE = 1.0 - SHARER_SHARE
# ------------------------------------------------

def touch_user(username: Optional[str]) -> None:
    """Record recent activity for a given username (if present)."""
    if not username:
        return
    u = server_state["users"].get(username)
    if u is not None:
        u["last_poll_ts"] = time.time()
        mark_users_dirty()

app = FastAPI()

# Load users from disk (if present)
if os.path.exists(USERS_FILE):
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        try:
            users_data: Dict[str, Dict[str, Any]] = json.load(f)
        except Exception:
            users_data = {}
else:
    users_data = {}

users_dirty = False


def mark_users_dirty() -> None:
    """Mark users state dirty so background loop persists to disk."""
    global users_dirty
    with _state_lock:
        users_dirty = True


def save_users_if_dirty() -> None:
    """If users_dirty, write server_state['users'] to USERS_FILE."""
    global users_dirty
    with _state_lock:
        if not users_dirty:
            return
        with file_lock:
            with open(USERS_FILE, "w", encoding="utf-8") as f:
                json.dump(server_state["users"], f, indent=2)
            print("[SAVE] users.json written, users:", list(server_state["users"].keys()))
        users_dirty = False


# ---------------------------
# Server state
# ---------------------------
# Note: tokens map token -> session dict (not index). That avoids index shifts when sessions list changes.
server_state: Dict[str, Any] = {
    "users": users_data,       # username -> info dict
    "pending": {},             # sharer -> {renter, state, timestamp, ...}
    "sessions": [],            # list of session dicts
    "tokens": {},              # token -> session_dict  (do NOT persist this to disk)
}

def prune_expired_sessions():
    now = time.time()
    for s in list(server_state["sessions"]):
        if s.get("active") and s.get("expires_at") and now > s["expires_at"]:
            print(f"[TIMEOUT] Ending session {s['token']} (max_time reached)")
            _end_session(s)
# ---------------------------
# Utilities / housekeeping
# ---------------------------
def gpu_tier_from_vram(vram_gb: Optional[int]) -> str:
    """Return a coarse tier name for a VRAM size (do not leak raw GPU strings)."""
    if vram_gb is None:
        return "unknown"
    if vram_gb < 4:
        return "tiny"
    if vram_gb < 8:
        return "small"
    if vram_gb < 12:
        return "medium"
    if vram_gb < 16:
        return "large"
    return "x-large"


def ensure_user_fields(username: str) -> Dict[str, Any]:
    """Ensure minimal fields exist for a user record and return it."""
    u = server_state["users"].setdefault(username, {})
    # credits are floats now (allow fractional credits)
    if "credits" not in u:
        u["credits"] = 0.0
    if "status" not in u:
        u["status"] = "idle"
    if "auto_accept" not in u:
        u["auto_accept"] = False
    return u


def prune_pending(expire_seconds: int = 60) -> None:
    """Drop pending requests older than `expire_seconds`."""
    now = time.time()
    to_delete = [k for k, v in server_state["pending"].items() if now - v.get("timestamp", now) > expire_seconds]
    for k in to_delete:
        server_state["pending"].pop(k, None)
    if to_delete:
        mark_users_dirty()


def prune_sessions(expire_seconds: int = 3600, max_sessions: int = 1000) -> None:
    """Remove sessions older than expire_seconds and trim list to max_sessions."""
    now = time.time()
    new_sessions: List[Dict[str, Any]] = []

    for s in server_state["sessions"]:
        if s.get("active"):
            if now - s.get("timestamp", now) > expire_seconds:
                print(f"[PRUNE] Session {s.get('token')} expired -> ending")
                _end_session(s, reason="timeout")
            else:
                new_sessions.append(s)
        else:
            # keep non-active sessions if recent
            if now - s.get("timestamp", now) < expire_seconds:
                new_sessions.append(s)

    # trim to max_sessions
    if len(new_sessions) > max_sessions:
        new_sessions = new_sessions[-max_sessions:]

    server_state["sessions"] = new_sessions

    # rebuild token -> session map
    server_state["tokens"].clear()
    for s in server_state["sessions"]:
        tok = s.get("token")
        if tok and s.get("active"):
            server_state["tokens"][tok] = s


def _sharer_has_active_session(sharer: str) -> bool:
    for s in server_state["sessions"]:
        if s.get("sharer") == sharer and s.get("active"):
            return True
    return False


def _create_session_token() -> str:
    return uuid.uuid4().hex


def _find_session_by_token(token: str) -> Tuple[Optional[Dict[str, Any]], Optional[int], bool]:
    """
    Return (session, index, valid_flag).
    - session: the session dict if found (active or inactive), else None
    - index: index in server_state['sessions'] if found else None
    - valid_flag: True if token maps to an ACTIVE session (i.e. normal signalling allowed)
    """
    # fast path: map lookup for active sessions
    session = server_state["tokens"].get(token)
    
    if session:
        now = time.time()
        session["last_activity_ts"] = now
        
        try:
            idx = server_state["sessions"].index(session)
        except Exception:
            idx = None
        # ensure active
        if session.get("active"):
            return session, idx, True
        else:
            return session, idx, False

    # fallback: scan sessions list for a session that still holds the token
    for idx, s in enumerate(server_state.get("sessions", [])):
        if s.get("token") == token:
            # we found the session object (may be inactive)
            return s, idx, bool(s.get("active"))
    return None, None, False


def _clear_signalling_for_session(session: Dict[str, Any]) -> None:
    token = session.get("token")
    if token and token in server_state["tokens"]:
        server_state["tokens"].pop(token, None)
    session.pop("token", None)
    session.pop("signalling", None)



def _end_session(session: Dict[str, Any], reason: str = "timeout", done_payload: Optional[Dict[str, Any]] = None) -> None:
    """End an active session, restore states and leave final 'done' messages
    so clients can receive a clear termination notification on the next poll.

    If done_payload is provided, include it verbatim in the final messages so
    both sides get the same artifact/status info.
    """
    if not session.get("active"):
        # still allow attaching final payload/messages for a recently-closed session
        # (caller may want messages preserved), but don't run cleanup twice.
        # However, if there is a done_payload and signalling exists, append it.
        if done_payload:
            sig = session.setdefault("signalling", {})
            i = sig.get("next_msg_index", 0)
            # Post as if the *other* role sent it, plus a server copy.
            sharer = session.get("sharer")
            renter = session.get("renter")
            msg_renter = {"index": i, "from": "sharer", "payload": done_payload, "timestamp": time.time()}
            msg_sharer = {"index": i+1, "from": "renter", "payload": done_payload, "timestamp": time.time()}
            sig.setdefault("messages", []).extend([msg_renter, msg_sharer])
            sig["next_msg_index"] = i + 2
        return

    # mark inactive
    session["active"] = False

    # mark as completed if not already
    if "completed" not in session:
        session["completed"] = True
        session["job_status"] = reason

    sig = session.setdefault("signalling", {})
    i = sig.get("next_msg_index", 0)

    # If caller supplied a done_payload, put that in the messages so both clients
    # see the same artifact/status. If not provided, create a minimal server 'done'.
    final_payload = done_payload if done_payload is not None else {"flag": "done", "reason": reason}

    # Add message(s) so both roles receive a done signal that is *not* filtered
    # by the client's "ignore-self-authored" logic:
    # - Put a message with "from" equal to the *other* role for each side,
    #   and also include a "server" copy if you prefer. Here we append both
    #   a renter- and sharer-origin message so each client sees it regardless.
    sharer = session.get("sharer")
    renter = session.get("renter")

    # message that renter will see (from sharer)
    msg_renter = {"index": i, "from": "sharer", "payload": final_payload, "timestamp": time.time()}
    # message that sharer will see (from renter)
    msg_sharer = {"index": i + 1, "from": "renter", "payload": final_payload, "timestamp": time.time()}
    sig.setdefault("messages", []).extend([msg_renter, msg_sharer])
    sig["next_msg_index"] = i + 2

    # restore/cleanup user states
    ru = server_state["users"].get(renter)
    su = server_state["users"].get(sharer)
    if ru:
        ru["status"] = "idle"
        ru.pop("rent_sharer", None)
        ru["last_poll_ts"] = ru.get("last_poll_ts") or time.time()
    if su:
        su.pop("current_renter", None)
        su["last_poll_ts"] = su.get("last_poll_ts") or time.time()

    # remove token mapping so new requests with token are rejected; but keep
    # signalling in the session dict so one last poll can return the messages.
    token = session.get("token")
    if token and token in server_state["tokens"]:
        server_state["tokens"].pop(token, None)

    mark_users_dirty()


def _cancel_pending_for_sharer(sharer: str) -> None:
    if sharer in server_state["pending"]:
        server_state["pending"].pop(sharer, None)
        mark_users_dirty()


def _cancel_pending_for_renter(renter: str) -> bool:
    """Remove any pending entries created by `renter`. Return True if any removed."""
    to_remove: List[str] = []
    for s, pend in list(server_state["pending"].items()):
        if pend.get("renter") == renter:
            to_remove.append(s)
    for s in to_remove:
        server_state["pending"].pop(s, None)
    if to_remove:
        mark_users_dirty()
    return bool(to_remove)


# ---------------------------
# Billing logic (replaces simple tick credit awarding)
# ---------------------------
def compute_fee_per_30s(vram_gb: Optional[float], cpu_cores: Optional[int], ram_gb: Optional[float], num_gpu: Optional[int]) -> float:
    """Compute fee per 30s based on resource parameters. Returns raw float (not rounded)."""
    vram = float(vram_gb or 0.0)
    cores = int(cpu_cores or 0)
    ram_g = float(ram_gb or 0.0)
    ngpu = int(num_gpu or 0)
    fee = (vram * VRAM_PRICE_PER_GB_30S) + (cores * CPU_PRICE_PER_CORE_30S) + (ram_g * RAM_PRICE_PER_GB_30S) + (ngpu * GPU_PRICE_PER_GPU_30S)
    return float(fee)

def tick_billing_background() -> Dict[str, float]:
    """
    Charge renters and credit sharers for active sessions.
    Also grant idle trickle credits to sharers even when not rented.
    This runs every ~5s, but only bills in 30s increments.
    Returns mapping username->credits for debugging.
    """
    updated: Dict[str, float] = {}
    now = time.time()
    with _state_lock:
        # ensure all users exist minimally
        for u in list(server_state["users"].keys()):
            ensure_user_fields(u)

        # --- 1. Normal active session billing ---
        for s in list(server_state["sessions"]):
            if not s.get("active"):
                continue
            if "last_billed_ts" not in s:
                s["last_billed_ts"] = s.get("timestamp", now)

            if "fee_per_30s" not in s:
                sharer = s.get("sharer")
                sharer_info = server_state["users"].get(sharer, {})
                vram = s.get("requested_vram") or sharer_info.get("vram")
                cpu_cores = sharer_info.get("cpu_cores")
                ram_gb = sharer_info.get("ram_gb")
                num_gpu = sharer_info.get("num_gpu")
                s["fee_per_30s"] = compute_fee_per_30s(vram, cpu_cores, ram_gb, num_gpu)

            last = s.get("last_billed_ts", s.get("timestamp", now))
            elapsed = now - last
            if elapsed < 30:
                continue
            steps = int(elapsed // 30)
            if steps <= 0:
                continue

            fee_step_raw = float(s.get("fee_per_30s", 0.0))
            fee_step_rounded = round(fee_step_raw, 2)

            renter = s.get("renter")
            sharer = s.get("sharer")
            renter_user = server_state["users"].get(renter)
            sharer_user = server_state["users"].get(sharer)

            steps_applied = 0
            for _ in range(steps):
                if not renter_user or not sharer_user:
                    break
                renter_credits = float(renter_user.get("credits", 0.0))
                charge = fee_step_rounded
                if renter_credits + 1e-9 < charge:
                    available = round(max(0.0, renter_credits), 2)
                    if available <= 0:
                        done_payload = {"flag": "done", "status": "failed", "reason": "insufficient_credits", "charged": 0.0}
                        print(f"[BILL] Renter {renter} has no credits -> ending session {s.get('token')}")
                        _end_session(s, reason="insufficient_credits", done_payload=done_payload)
                        break
                    actual_charge = available
                    renter_user["credits"] = round(renter_credits - actual_charge, 2)
                    sharer_gain = round(actual_charge * SHARER_SHARE, 2)
                    sharer_user["credits"] = round(float(sharer_user.get("credits", 0.0)) + sharer_gain, 2)
                    print(f"[BILL] Partial charge {actual_charge:.2f} from renter {renter} -> sharer {sharer} +{sharer_gain:.2f} (session {s.get('token')})")
                    done_payload = {"flag": "done", "status": "failed", "reason": "insufficient_credits", "charged": actual_charge}
                    _end_session(s, reason="insufficient_credits", done_payload=done_payload)
                    steps_applied += 1
                    break
                else:
                    renter_user["credits"] = round(renter_credits - charge, 2)
                    sharer_gain = round(charge * SHARER_SHARE, 2)
                    sharer_user["credits"] = round(float(sharer_user.get("credits", 0.0)) + sharer_gain, 2)
                    steps_applied += 1
                    print(f"[BILL] Charged {charge:.2f} from renter {renter} -> sharer {sharer} +{sharer_gain:.2f} (session {s.get('token')})")

            if steps_applied > 0:
                s["last_billed_ts"] = last + steps_applied * 30

        # --- 2. Idle sharer credits (new) ---
        IDLE_CREDIT_PER_30S = 0.01  # configurable trickle per sharer per 30s

                
        # --- 2. Idle sharer credits (new) ---
        for username, user in server_state["users"].items():
            if user["status"] != "sharing":
                continue

            if "last_idle_billed_ts" not in user:
                user["last_idle_billed_ts"] = now  # initialize timer
                continue

            # Skip if they already have an active renter
            active = any(s.get("active") and s.get("sharer") == username for s in server_state["sessions"])
            if active:
                continue

            last_idle = user["last_idle_billed_ts"]
            elapsed_idle = now - last_idle
            if elapsed_idle < 30:
                continue

            # compute standard fee for this sharer
            vram = user.get("vram", 0)
            cpu_cores = user.get("cpu_cores", 0)
            ram_gb = user.get("ram_gb", 0)
            num_gpu = user.get("num_gpu", 0)
            fee_per_30s = compute_fee_per_30s(vram, cpu_cores, ram_gb, num_gpu)

            # idle reward is 1/10 of that
            idle_per_step = round(fee_per_30s / 10.0, 4)

            steps = int(elapsed_idle // 30)
            gain_total = round(steps * idle_per_step, 2)
            if gain_total > 0:
                before = float(user.get("credits", 0.0))
                user["credits"] = round(before + gain_total, 2)
                user["last_idle_billed_ts"] = last_idle + steps * 30
                print(f"[BILL] Idle credit {before:.2f} -> {user['credits']:.2f} (+{gain_total:.2f}) "
                      f"to sharer {username} (no active sessions, {steps}×{idle_per_step:.4f})")
        # --- build return mapping ---
        for username, u in server_state["users"].items():
            updated[username] = float(round(u.get("credits", 0.0), 2))

    mark_users_dirty()
    return updated

from fastapi.responses import StreamingResponse
from fastapi import UploadFile, File

# Upload file (encrypted by client)
from fastapi import Form

# simple in-memory mapping; consider persistent DB for production
ARTIFACTS: Dict[str, str] = {}  # artifact_id -> filepath

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Existing upload endpoint (keep behavior) — but return artifact_id too
@app.post("/upload")
async def upload_file(token: str = Query(None), role: str = Query(None), file: UploadFile = File(...)):
    """
    If token+role provided, behave as before and save file prefixed by session token.
    Also generate an artifact_id and return it so sharer/renter can download later without relying on session state.
    """
    safe_name = os.path.basename(file.filename)
    # save canonical file name for compatibility with older clients:
    # saved path with session token prefix if token was provided and valid-looking
    if token:
        saved_name = f"{token}_{safe_name}"
    else:
        saved_name = safe_name

    # unique artifact filename to avoid collisions
    artifact_id = uuid.uuid4().hex
    artifact_filename = f"{artifact_id}_{safe_name}"
    path = os.path.join(UPLOAD_DIR, artifact_filename)

    # save file bytes
    with open(path, "wb") as out_f:
        out_f.write(await file.read())

    # register artifact id -> path (so clients can download later without token)
    ARTIFACTS[artifact_id] = path

    # also optionally maintain the legacy path (session-token prefixed) for backward compatibility:
    legacy_path = os.path.join(UPLOAD_DIR, saved_name)
    try:
        # create a lightweight copy (or symlink) so legacy download still works if desired
        # copy is simpler and robust cross-platform
        with open(path, "rb") as src, open(legacy_path, "wb") as dst:
            dst.write(src.read())
    except Exception:
        # non-fatal: ignore
        pass

    return {"ok": True, "path": path, "artifact_id": artifact_id, "filename": safe_name}

# New artifact download endpoint — does not require session token
@app.get("/artifact/download")
async def download_artifact(artifact_id: str = Query(...)):
    path = ARTIFACTS.get(artifact_id)
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Artifact not found")
    # stream the artifact
    return StreamingResponse(open(path, "rb"), media_type="application/octet-stream")

# Keep your existing /download (session-protected) if you rely on it elsewhere.
# But prefer /artifact/download for artifact fetching after 'done'.



# Download file (encrypted, server does not modify)
@app.get("/download")
async def download_file(token: str, filename: str):
    session, idx, valid = _find_session_by_token(token)
    if not valid or not session:
        # return session ended response same shape as client expects
        raise HTTPException(status_code=404, detail="Session token not found")

    safe_name = os.path.basename(filename)
    path = f"uploads/{session['token']}_{safe_name}"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return StreamingResponse(open(path, "rb"), media_type="application/octet-stream")


# ---------------------------
# Inactivity prune for sharers
# ---------------------------


def prune_inactive_sessions(inactive_seconds: int = 30) -> None:
    now = time.time()
    with _state_lock:
        for s in list(server_state.get("sessions", [])):
            if not s.get("active"):
                continue

            sharer = s.get("sharer")
            renter = s.get("renter")

            # user last poll timestamps (explicitly coerce to floats; missing -> 0.0)
            sharer_last_poll = float(server_state["users"].get(sharer, {}).get("last_poll_ts") or 0.0)
            renter_last_poll = float(server_state["users"].get(renter, {}).get("last_poll_ts") or 0.0)

            # session-level last activity (fallback to session.timestamp if present)
            session_last = float(s.get("last_activity_ts") or s.get("timestamp") or 0.0)

            # Last known activity for each role is the more recent of the user's poll or any session activity.
            # This prevents prunes while someone is actively interacting with the session.
            sharer_last = max(sharer_last_poll, session_last)
            renter_last = max(renter_last_poll, session_last)

            sharer_inactive = (sharer_last == 0.0) or (now - sharer_last > inactive_seconds)
            renter_inactive = (renter_last == 0.0) or (now - renter_last > inactive_seconds)

            # debug: print detailed info every time for visibility (remove/quiet later)
            print(
                "[PRUNE-CHECK] token", s.get("token"),
                f"sharer_last={sharer_last:.1f}s (poll={sharer_last_poll:.1f}s),",
                f"renter_last={renter_last:.1f}s (poll={renter_last_poll:.1f}s),",
                f"threshold={inactive_seconds}s"
            )

            if sharer_inactive and renter_inactive:
                print(f"[PRUNE] Ending session {s.get('token')} because both sides inactive "
                      f"(sharer:{now - sharer_last:.1f}s renter:{now - renter_last:.1f}s)")
                _end_session(s, reason="inactive")
            else:
                # optional debug for skipped sessions
                print(f"[PRUNE-SKIP] Keeping session {s.get('token')} (sharer_active={not sharer_inactive}, renter_active={not renter_inactive})")


def prune_inactive_sharers(inactive_seconds: int = 15) -> None:
    """
    Turn 'sharing' -> 'idle' if no activity for `inactive_seconds`.
    Do not mark sharer idle if there is recent session-level activity
    (someone uploaded, signalled, etc.). This prevents killing sessions
    while sharer is busy executing a job and not polling frequently.
    """
    now = time.time()
    with _state_lock:
        for username, u in list(server_state["users"].items()):
            if u.get("status") != "sharing":
                continue

            last_poll = u.get("last_poll_ts")
            # If there is no last_poll timestamp, skip pruning — update_status should set it.
            if last_poll is None:
                continue

            elapsed_user_poll = now - last_poll
            if elapsed_user_poll <= inactive_seconds:
                # user polled recently => keep sharing
                continue

            # Check active sessions where this user is the sharer for session-level activity
            recent_session_activity = False
            for s in server_state.get("sessions", []):
                if s.get("sharer") != username or not s.get("active"):
                    continue
                sess_last = s.get("last_activity_ts") or 0.0
                if now - sess_last <= inactive_seconds:
                    # there's session activity within threshold -> treat sharer as active
                    recent_session_activity = True
                    break

            if recent_session_activity:
                # do not prune sharing status while an active session shows recent activity
                continue

            # No recent user poll and no recent session activity -> mark sharer idle
            print(f"[PRUNE] User {username} inactive (last_poll {elapsed_user_poll:.1f}s) -> set to idle")
            u["status"] = "idle"
            u["vram"] = None
            u["_gpu_raw"] = None
            u["last_credit_ts"] = None

            # end any active session where this user is sharer (they truly look gone)
            for s in list(server_state["sessions"]):
                if s.get("sharer") == username and s.get("active"):
                    _end_session(s)
            mark_users_dirty()


# ---------------------------
# Background maintenance loop
# ---------------------------
def background_loop() -> None:
    """Periodic maintenance: persist, prune, bill, and prune inactive sharers/sessions."""
    while True:
        time.sleep(5)
        with _state_lock:
            save_users_if_dirty()
            prune_pending()
            prune_sessions()
            # billing tick: apply per-30s charges to active sessions
            tick_billing_background()
            prune_inactive_sharers(inactive_seconds=15)
            prune_inactive_sessions(inactive_seconds=30)
            prune_expired_sessions()
            # minimal debug output:
            print("[MAINT] users:", len(server_state["users"]),
                  "pending:", len(server_state["pending"]),
                  "sessions:", len(server_state["sessions"]))


# We'll start this thread later (after endpoint definitions)


# ---------------------------
# FastAPI app & models
# ---------------------------


class User(BaseModel):
    username: str
    pwd: str
    credits: Optional[float] = 0.0
    # optional system spec fields when a sharer registers
    cpu_cores: Optional[int] = None
    cpu_threads: Optional[int] = None
    ram_gb: Optional[int] = None
    num_gpu: Optional[int] = None
    gpu: Optional[str] = None
    vram: Optional[int] = None
    auto_accept: Optional[bool] = False


class UpdateStatusRequest(BaseModel):
    username: str
    pwd: str
    status: str
    gpu: Optional[str] = None
    vram: Optional[int] = None
    num_gpu: Optional[int] = None
    cpu_cores: Optional[int] = None
    cpu_threads: Optional[int] = None
    ram_gb: Optional[int] = None


class UpdateSettingsRequest(BaseModel):
    username: str
    pwd: str
    auto_accept: Optional[bool] = None


class LoginRequest(BaseModel):
    username: str
    pwd: str


class RequestGPU(BaseModel):
    username: str  # renter
    vram: int
    max_time: int
    cpu_cores: int
    cpu_threads: int
    num_gpu: int
    ram_gb: int


class MatchRequest(BaseModel):
    renter: str
    sharer: str


class RespondRequest(BaseModel):
    sharer: str
    accept: bool


class OfferRequest(BaseModel):
    token: str
    role: str  # 'sharer' or 'renter'
    sdp: Dict[str, Any]


class AnswerRequest(BaseModel):
    token: str
    role: str
    sdp: Dict[str, Any]


class CandidateRequest(BaseModel):
    token: str
    role: str
    candidate: Dict[str, Any]


class MessageRequest(BaseModel):
    token: str
    role: str
    payload: Dict[str, Any]


# ---------------------------
# API endpoints
# ---------------------------
@app.post("/update_settings")
async def update_settings(req: UpdateSettingsRequest):
    with _state_lock:
        user = server_state["users"].get(req.username)
        if not user or user.get("pwd") != req.pwd:
            raise HTTPException(status_code=401, detail="Unauthorized")
        if req.auto_accept is not None:
            user["auto_accept"] = bool(req.auto_accept)
            mark_users_dirty()
        return {"ok": True, "auto_accept": user["auto_accept"]}


@app.post("/register")
async def register(user: User):
    if not user.username.strip() or not user.pwd:
        raise HTTPException(status_code=400, detail="Username and password required")

    with _state_lock:
        if user.username in server_state["users"]:
            raise HTTPException(status_code=400, detail="User exists")

        server_state["users"][user.username] = {
            "pwd": user.pwd,
            "credits": float(getattr(user, "credits", 0.0) or 0.0),
            "status": "idle",
            # hardware/resource info — initially None until sharer updates
            "gpu": None,
            "vram": None,
            "ram_gb": None,
            "cpu_cores": None,
            "cpu_threads": None,
            "num_gpu": None,
            # misc
            "last_credit_ts": None,
            "auto_accept": False,
        }
        mark_users_dirty()

        # immediate synchronous save so the file reflects the registration right away
        save_users_if_dirty()
        print(f"[REGISTER] user saved: {user.username}")

    return {"ok": True}


@app.post("/login")
async def login(req: LoginRequest):
    with _state_lock:
        user = server_state["users"].get(req.username)
        if not user or user.get("pwd") != req.pwd:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        ensure_user_fields(req.username)
        return {"ok": True, "username": req.username, "credits": user.get("credits", 0.0), "status": user.get("status", "idle")}


@app.get("/poll")
async def poll(username: str):
    with _state_lock:
        user = server_state["users"].get(username)
        if user:
            user["last_poll_ts"] = time.time()
            mark_users_dirty()
        pend = server_state["pending"].get(username)
    return {"ok": True, "pending": pend}


@app.post("/request_gpu")
async def request_gpu(req: RequestGPU):
    requested_vram = int(req.vram)
    requested_cores = int(req.cpu_cores)
    requested_threads = int(req.cpu_threads)
    requested_gpus = int(req.num_gpu)
    requested_ram = int(req.ram_gb)
    with _state_lock:
        candidates = []
        for uname, info in server_state["users"].items():
            print(uname,info)
            if info.get("status") == "sharing" and (info.get("vram") or 0) >= requested_vram and (info.get("ram_gb") or 0) >= requested_ram and (info.get("cpu_cores") or 0) >= requested_cores and (info.get("cpu_threads") or 0) >= requested_threads and (info.get("num_gpu") or 0) >= requested_gpus:
                
                if not _sharer_has_active_session(uname) and (uname not in server_state["pending"]):
                    candidates.append({
                        "username": uname,
                        "vram": info.get("vram"),
                        "tier": gpu_tier_from_vram(info.get("vram")),
                        "auto_accept": info.get("auto_accept", False)
                    })
        if not candidates:
            return {"ok": False, "error": "No sharer has enough VRAM available right now"}

        best = min(candidates, key=lambda g: g["vram"] or 0)
        sharer = best["username"]

        # immediate session if auto_accept
        if best.get("auto_accept"):
            renter_user = server_state["users"].get(req.username)
            sharer_user = server_state["users"].get(sharer)
            if renter_user.get("credits", 0.0) <= 0.0:
                return {"ok": False, "error": "Renter has insufficient credits"}

            # NOTE: keep existing behavior that deducted 1 credit at start (legacy).
            # You may change this later if you prefer deposit-less-start.
            renter_user["credits"] = round(float(renter_user.get("credits", 0.0)) - 1.0, 2)

            now = time.time()
            expires_at = now + int(req.max_time) * 60
            session = {
                "sharer": sharer,
                "renter": req.username,
                "timestamp": now,
                "expires_at": expires_at,
                "active": True,
                "requested_vram": requested_vram,
                "token": _create_session_token(),
                "signalling": {
                    "offer": None, "answer": None,
                    "candidates": {"sharer": [], "renter": []},
                    "messages": [], "next_msg_index": 0
                }
            }
            session["last_activity_ts"] = now
            # compute and store fee and last_billed_ts at session start
            fee = compute_fee_per_30s(
                vram_gb=sharer_user.get("vram") or requested_vram,
                cpu_cores=sharer_user.get("cpu_cores"),
                ram_gb=sharer_user.get("ram_gb"),
                num_gpu=sharer_user.get("num_gpu")
            )
            session["fee_per_30s"] = fee
            session["last_billed_ts"] = now
            fee_step = round(session["fee_per_30s"], 2)
            sharer_gain = round(fee_step * SHARER_SHARE, 2)

            # bonus is 10x sharer_gain
            session_bonus = round(sharer_gain * 10, 2)

            before = float(sharer_user.get("credits", 0.0))
            sharer_user["credits"] = round(before + session_bonus, 2)
            print(f"[BILL] Sharer {sharer_user} received session start bonus +{session_bonus:.2f} "
                  f"(10x regular step {sharer_gain:.2f})")
            server_state["sessions"].append(session)
            # map token -> session dict object
            server_state["tokens"][session["token"]] = session

            renter_user["status"] = "renting"
            renter_user["rent_sharer"] = sharer
            sharer_user["current_renter"] = req.username

            mark_users_dirty()
            return {
                "ok": True,
                "sharer": {"username": sharer, "vram": sharer_user.get("vram"), "tier": best["tier"]},
                "session": session,
                "token": session["token"],
                "renter_credits": renter_user["credits"]
            }
        else:
            server_state["pending"][sharer] = {
                "renter": req.username,
                "state": "pending",
                "timestamp": time.time(),
                "max_time": int(req.max_time),
                "requested_vram": requested_vram,
            }
            mark_users_dirty()
            return {"ok": True, "sharer": {"username": sharer, "vram": int(best["vram"]) if best["vram"] is not None else None, "tier": best["tier"]}}


@app.post("/match")
async def match(req: MatchRequest):
    with _state_lock:
        if req.renter not in server_state["users"]:
            raise HTTPException(status_code=404, detail="Renter not found")
        sharer_user = server_state["users"].get(req.sharer)
        if not sharer_user:
            raise HTTPException(status_code=404, detail="Sharer not found")
        if sharer_user.get("status") != "sharing":
            raise HTTPException(status_code=400, detail="Sharer not available")
        if _sharer_has_active_session(req.sharer):
            raise HTTPException(status_code=400, detail="Sharer already in an active session")
        if req.sharer in server_state["pending"]:
            raise HTTPException(status_code=409, detail="Sharer already has a pending request")
        for s, pend in server_state["pending"].items():
            if pend.get("renter") == req.renter:
                raise HTTPException(status_code=409, detail="Renter already has a pending request")
        for s in server_state.get("sessions", []):
            if s.get("renter") == req.renter and s.get("active"):
                raise HTTPException(status_code=409, detail="Renter already in an active session")

        requested_vram = sharer_user.get("vram", 0)
        server_state["pending"][req.sharer] = {
            "renter": req.renter,
            "state": "pending",
            "timestamp": time.time(),
            "requested_vram": requested_vram,
        }
        mark_users_dirty()
        return {"ok": True, "sharer": {"username": req.sharer, "vram": int(requested_vram) if requested_vram is not None else None}}


@app.post("/respond")
async def respond(req: RespondRequest):
    with _state_lock:
        pend = server_state["pending"].get(req.sharer)
        if not pend:
            raise HTTPException(status_code=404, detail="No pending request for this sharer")

        renter = pend.get("renter")
        if not renter:
            server_state["pending"].pop(req.sharer, None)
            mark_users_dirty()
            raise HTTPException(status_code=400, detail="Malformed pending entry")

        if req.accept:
            renter_user = server_state["users"].get(renter)
            sharer_user = server_state["users"].get(req.sharer)
            if not renter_user:
                server_state["pending"].pop(req.sharer, None)
                mark_users_dirty()
                raise HTTPException(status_code=404, detail="Renter not found")
            if renter_user.get("credits", 0.0) <= 0.0:
                server_state["pending"].pop(req.sharer, None)
                mark_users_dirty()
                return {"ok": False, "error": "Renter has insufficient credits"}
            if not sharer_user or sharer_user.get("status") != "sharing" or _sharer_has_active_session(req.sharer):
                server_state["pending"].pop(req.sharer, None)
                mark_users_dirty()
                raise HTTPException(status_code=400, detail="Sharer no longer available")

            # legacy: deduct 1 credit at start (keeps backward compatibility)
            renter_user["credits"] = round(renter_user.get("credits", 0.0) - 1.0, 2)

            now = time.time()
            requested_vram = pend.get("requested_vram", 0)
            expires_at = now + (pend.get("max_time", 60) * 60 if isinstance(pend.get("max_time"), (int, float)) else 3600)
            session: Dict[str, Any] = {
                "sharer": req.sharer,
                "renter": renter,
                "timestamp": now,
                "expires_at": expires_at,
                "active": True,
                "requested_vram": requested_vram
            }

            token = _create_session_token()
            session["token"] = token
            session["signalling"] = {
                "offer": None,
                "answer": None,
                "candidates": {"sharer": [], "renter": []},
                "messages": [],
                "next_msg_index": 0
            }

            # compute fee_per_30s and initialize last_billed_ts
            fee = compute_fee_per_30s(
                vram_gb=sharer_user.get("vram") or requested_vram,
                cpu_cores=sharer_user.get("cpu_cores"),
                ram_gb=sharer_user.get("ram_gb"),
                num_gpu=sharer_user.get("num_gpu")
            )
            session["fee_per_30s"] = fee
            session["last_billed_ts"] = now
            fee_step = round(session["fee_per_30s"], 2)
            sharer_gain = round(fee_step * SHARER_SHARE, 2)

            # bonus is 10x sharer_gain
            session_bonus = round(sharer_gain * 10, 2)

            before = float(sharer_user.get("credits", 0.0))
            sharer_user["credits"] = round(before + session_bonus, 2)
            print(f"[BILL] Sharer {sharer_user} received session start bonus +{session_bonus:.2f} "
                  f"(10x regular step {sharer_gain:.2f})")
            server_state["sessions"].append(session)
            server_state["tokens"][token] = session

            renter_user["status"] = "renting"
            renter_user["rent_sharer"] = req.sharer
            sharer_user["current_renter"] = renter

            server_state["pending"].pop(req.sharer, None)
            mark_users_dirty()
            return {"ok": True, "session": session, "renter_credits": renter_user["credits"], "token": token}

        else:
            server_state["pending"].pop(req.sharer, None)
            mark_users_dirty()
            return {"ok": True}


@app.get("/counts")
def counts():
    with _state_lock:
        sharing = sum(1 for u in server_state["users"].values() if u.get("status") == "sharing")
        renting = sum(1 for s in server_state.get("sessions", []) if s.get("active"))
    return {"ok": True, "sharing": sharing, "renting": renting}


@app.post("/cancel_rent")
async def cancel_rent(data: dict = Body(...)):
    username = data.get("username")
    pwd = data.get("pwd")
    if not username or not pwd:
        raise HTTPException(status_code=400, detail="username+pwd required")
    with _state_lock:
        user = server_state["users"].get(username)
        if not user or user.get("pwd") != pwd:
            raise HTTPException(status_code=401, detail="Unauthorized")

        for s in list(server_state["sessions"]):
            if s.get("renter") == username and s.get("active"):
                completed = bool(s.get("completed", False))
                s["active"] = False
                _clear_signalling_for_session(s)

                user["status"] = "idle"
                user.pop("rent_sharer", None)
                

                sharer = s.get("sharer")
                su = server_state["users"].get(sharer)
                if su:
                    su.pop("current_renter", None)

                mark_users_dirty()
                return {
                    "ok": True,
                    "cancelled_active_session": True,
                    
                    "completed": completed
                }

        # Also clear pending requests created by this renter (if they cancel a pending request)
        removed = _cancel_pending_for_renter(username)
        if removed:
            return {"ok": True, "cancelled_pending": True}

        return {"ok": True, "nothing_to_cancel": True}


@app.post("/update_status")
async def update_status(req: UpdateStatusRequest):
    with _state_lock:
        user = server_state["users"].get(req.username)
        if not user or user.get("pwd") != req.pwd:
            raise HTTPException(status_code=401, detail="Unauthorized")

        prev = user.get("status", "idle")
        user["status"] = req.status
        now = time.time()

        if req.status == "sharing":
            # GPU + VRAM
            if req.vram is not None:
                user["vram"] = int(req.vram)
            if req.gpu:
                user["_gpu_raw"] = req.gpu[:128]
                user["gpu"] = req.gpu[:128]

            # Other system specs
            if req.ram_gb is not None:
                user["ram_gb"] = int(req.ram_gb)
            if req.cpu_cores is not None:
                user["cpu_cores"] = int(req.cpu_cores)
            if req.cpu_threads is not None:
                user["cpu_threads"] = int(req.cpu_threads)
            if req.num_gpu is not None:
                user["num_gpu"] = int(req.num_gpu)

            if prev != "sharing":
                user["last_credit_ts"] = now
            user["last_poll_ts"] = now

            mark_users_dirty()
            return {"ok": True}

        else:
            # leaving sharing: reset hardware metadata
            user["vram"] = None
            user["_gpu_raw"] = None
            user["gpu"] = None
            user["ram_gb"] = None
            user["cpu_cores"] = None
            user["cpu_threads"] = None
            user["num_gpu"] = None
            user["last_credit_ts"] = None
            user["last_poll_ts"] = now

            # cancel pending
            if req.username in server_state["pending"]:
                server_state["pending"].pop(req.username)

            # end sessions where sharer
            for s in list(server_state["sessions"]):
                if s.get("sharer") == req.username and s.get("active"):
                    s["active"] = False
                    renter = s.get("renter")
                    ru = server_state["users"].get(renter)
                    if ru:
                        completed = bool(s.get("completed", False))
                        ru["status"] = "idle"
                        ru.pop("rent_sharer", None)
                    _clear_signalling_for_session(s)

            user.pop("current_renter", None)
            mark_users_dirty()
            return {"ok": True}


@app.get("/pending_for")
async def pending_for(renter: str):
    with _state_lock:
        for s in server_state["sessions"]:
            if s.get("renter") == renter and s.get("active"):
                sharer = s.get("sharer")
                sharer_info = server_state["users"].get(sharer, {})
                gpu_name = sharer_info.get("gpu")
                gpu_vram = sharer_info.get("vram")
                gpu_tier = gpu_tier_from_vram(gpu_vram)
                return {
                    "ok": True,
                    "pending": {
                        "state": "accepted",
                        "sharer": sharer,
                        "gpu": gpu_name,
                        "tier": gpu_tier,
                        "session": {"expires_at": s.get("expires_at")},
                        "token": s.get("token")
                    }
                }

        for sharer, pend in server_state["pending"].items():
            if pend.get("renter") == renter:
                sharer_info = server_state["users"].get(sharer, {})
                gpu_name = sharer_info.get("gpu")
                gpu_vram = sharer_info.get("vram")
                gpu_tier = gpu_tier_from_vram(gpu_vram)
                return {
                    "ok": True,
                    "pending": {
                        "state": pend.get("state"),
                        "sharer": sharer,
                        "gpu": gpu_name,
                        "tier": gpu_tier,
                        "requested_vram": pend.get("requested_vram"),
                        "timestamp": pend.get("timestamp"),
                    }
                }

    return {"ok": True, "pending": None}


# ---------------------------
# Signalling endpoints
# ---------------------------
@app.post("/signal/offer")
async def signal_offer(req: OfferRequest):
    with _state_lock:
        session, idx, valid = _find_session_by_token(req.token)
        if not valid or not session:
            raise HTTPException(status_code=404, detail="Session token not found")

        if req.role not in ("sharer", "renter"):
            raise HTTPException(status_code=400, detail="Invalid role")
        session.setdefault("signalling", {})["offer"] = {"role": req.role, "sdp": req.sdp, "timestamp": time.time()}

        # refresh session activity + touch sender user so pruning sees it
        session["last_activity_ts"] = time.time()
        sender_name = session.get(req.role)
        touch_user(sender_name)

        mark_users_dirty()
    return {"ok": True}


@app.post("/signal/answer")
async def signal_answer(req: AnswerRequest):
    with _state_lock:
        session, idx, valid = _find_session_by_token(req.token)
        if not valid or not session:
            raise HTTPException(status_code=404, detail="Session token not found")

        if req.role not in ("sharer", "renter"):
            raise HTTPException(status_code=400, detail="Invalid role")
        session.setdefault("signalling", {})["answer"] = {...}
        session["last_activity_ts"] = time.time()
        sender_name = session.get(req.role)
        touch_user(sender_name)
        mark_users_dirty()
    return {"ok": True}


@app.post("/signal/candidate")
async def signal_candidate(req: CandidateRequest):
    with _state_lock:
        session, idx, valid = _find_session_by_token(req.token)
        if not valid or not session:
            raise HTTPException(status_code=404, detail="Session token not found")
        if req.role not in ("sharer", "renter"):
            raise HTTPException(status_code=400, detail="Invalid role")
        sig = session.setdefault("signalling", {})
        sig.setdefault("candidates", {}).setdefault(req.role, []).append({"candidate": req.candidate, "timestamp": time.time()})
        session["last_activity_ts"] = time.time()
        sender_name = session.get(req.role)
        touch_user(sender_name)
        mark_users_dirty()
    return {"ok": True}


@app.post("/signal/message")
async def signal_message(req: MessageRequest):
    with _state_lock:
        session, idx, valid = _find_session_by_token(req.token)
        if not valid or not session:
            raise HTTPException(status_code=404, detail="Session token not found")

        sig = session.setdefault("signalling", {})
        i = sig.get("next_msg_index", 0)

        msg = {"index": i, "from": req.role, "payload": req.payload, "timestamp": time.time()}
        sig.setdefault("messages", []).append(msg)
        sig["next_msg_index"] = i + 1

        # If a 'done' payload arrives, mark session completed and *end* the session
        # immediately, ensuring both sides get the final done payload and state is cleaned.
        if req.payload.get("flag") == "done":
            # store job completion status on session (for housekeeping)
            session["completed"] = True
            session["job_status"] = req.payload.get("status", "done").lower()
            # call _end_session with the exact payload so both sides get it
            _end_session(session, reason="done", done_payload=req.payload)

            # note: _end_session will also remove the token from server_state['tokens']
            # and mark_users_dirty() so we can return the original message index to caller.
            return {"ok": True, "index": i}

        # handle 'output' final marker (existing behavior)
        if req.payload.get("flag") == "output" and req.payload.get("final"):
            session["completed"] = True
            session["job_status"] = req.payload.get("status", "done").lower()

        session["last_activity_ts"] = time.time()
        sender_name = session.get(req.role)
        touch_user(sender_name)
        mark_users_dirty()
    return {"ok": True, "index": i}


@app.post("/rent/complete")
async def rent_complete(data: dict = Body(...)):
    token = data.get("token")
    role = data.get("role")
    with _state_lock:
        session, idx, valid = _find_session_by_token(token)
        if not valid or not session:
            raise HTTPException(status_code=404, detail="Session not found")
        session["completed"] = True
        session["job_status"] = "done"
        mark_users_dirty()
    return {"ok": True}

@app.get("/signal/poll")
def signal_poll(token: str = Query(...), role: str = Query(...), since: int = Query(0)):
    with _state_lock:
        session, idx, valid = _find_session_by_token(token)

        if not session:
            # Try to find a recently-closed session with that token
            for s in server_state.get("sessions", []):
                if s.get("token") == token:
                    sig = s.get("signalling", {})
                    msgs = [m for m in sig.get("messages", []) if m["index"] >= since]

                    if not msgs:
                        return {"ok": False, "session_ended": True, "error": "Session has ended"}

                    return {
                        "ok": True,
                        "session_ended": True,
                        "offer": sig.get("offer"),
                        "answer": sig.get("answer"),
                        "candidates": sig.get("candidates", {}),
                        "messages": msgs,
                        "next_msg_index": sig.get("next_msg_index", 0)
                    }

            # No session found at all
            return {"ok": False, "session_ended": True, "error": "Session has ended"}

        if role not in ("sharer", "renter"):
            return {"ok": False, "error": "Invalid role"}

        # Update last_poll_ts (if user exists)
        now = time.time()
        if role == "sharer":
            server_state["users"].get(session.get("sharer"), {})["last_poll_ts"] = now
        else:
            server_state["users"].get(session.get("renter"), {})["last_poll_ts"] = now

        session["last_activity_ts"] = now
        mark_users_dirty()

        sig = session.get("signalling", {})
        msgs = [m for m in sig.get("messages", []) if m["index"] >= since]

        session_ended = not bool(session.get("active"))

        return {
            "ok": True,
            "offer": sig.get("offer"),
            "answer": sig.get("answer"),
            "candidates": sig.get("candidates", {}),
            "messages": msgs,
            "next_msg_index": sig.get("next_msg_index", 0),
            "session_ended": session_ended
        }

@app.get("/sharers")
async def sharers():
    out: List[Dict[str, Any]] = []
    with _state_lock:
        for uname, info in server_state["users"].items():
            if info.get("status") == "sharing":
                v = info.get("vram")
                tier = gpu_tier_from_vram(v)
                out.append({"username": uname, "vram": int(v) if v is not None else None, "tier": tier})
    return {"ok": True, "sharers": out}


@app.post("/credits")
def credits(data: dict = Body(...)):
    username = data.get("username")
    pwd = data.get("pwd")
    if not username or not pwd:
        raise HTTPException(status_code=400, detail="username+pwd required")
    user = server_state["users"].get(username)
    if not user or user.get("pwd") != pwd:
        raise HTTPException(status_code=401, detail="Unauthorized")
    # best-effort application of billing ticks before returning
    tick_billing_background()
    return {"ok": True, "credits": round(float(user.get("credits", 0.0)), 2)}


def _start_background_thread():
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    print("[STARTUP] background maintenance thread started")

@app.on_event("startup")
def _on_startup():
    # start the background maintenance thread once per process
    _start_background_thread()

# ---------------------------
# Start background thread then run uvicorn if main
if __name__ == "__main__":
    

    import uvicorn

    # Prefer env vars, then fallback to local files
    ssl_certfile = os.environ.get("SSL_CERTFILE", "server.crt")
    ssl_keyfile = os.environ.get("SSL_KEYFILE", "server.key")

    # Only enable TLS if both files exist
    use_tls = bool(ssl_certfile and ssl_keyfile and os.path.exists(ssl_certfile) and os.path.exists(ssl_keyfile))

    try:
        if use_tls:
            print("[START] Running with TLS (wss://)")
            uvicorn.run("server:app", host="0.0.0.0", port=8000,
                        log_level="info", ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile)
        else:
            print("[START] TLS cert/key not found - running without TLS (ws://). Set SSL_CERTFILE/SSL_KEYFILE to enable.")
            uvicorn.run("server:app", host="0.0.0.0", port=8000, log_level="info")
    except Exception as exc:
        print("[ERROR] uvicorn failed:", exc)
        # fallback to loopback (no TLS)
        uvicorn.run("server:app", host="127.0.0.1", port=8000, log_level="info")
