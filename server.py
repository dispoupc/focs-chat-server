from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="FOCS Community Final")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

rooms: Dict[str, Set[WebSocket]] = defaultdict(set)
meta: Dict[WebSocket, dict] = {}
by_name: Dict[str, WebSocket] = {}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def norm_room(value: str):
    return ((value or "general").strip().lower() or "general")[:40]


def norm_name(value: str):
    return (" ".join((value or "Invitado").strip().split()) or "Invitado")[:30]


async def safe_send(ws: WebSocket, payload: dict):
    try:
        await ws.send_json(payload)
        return True
    except Exception:
        return False


async def room_send(room: str, payload: dict):
    dead = []
    for ws in list(rooms.get(room, set())):
        if not await safe_send(ws, payload):
            dead.append(ws)
    for ws in dead:
        await unregister(ws)


def public_user(ws: WebSocket):
    m = meta[ws]
    return {
        "username": m["username"],
        "status": m.get("status", "online"),
        "thought": m.get("thought", "Ready to fight"),
        "room": m.get("room", "general"),
    }


async def broadcast_global_presence():
    users = [public_user(ws) for ws in list(meta.keys())]
    users.sort(key=lambda x: x["username"].lower())
    payload = {"type": "presence_global", "users": users, "timestamp": now_iso()}
    dead = []
    for ws in list(meta.keys()):
        if not await safe_send(ws, payload):
            dead.append(ws)
    for ws in dead:
        await unregister(ws, notify=False)


async def broadcast_room_presence(room: str):
    users = [public_user(ws) for ws in rooms.get(room, set()) if ws in meta]
    users.sort(key=lambda x: x["username"].lower())
    await room_send(room, {
        "type": "presence_room",
        "room": room,
        "users": users,
        "timestamp": now_iso(),
    })


async def system(room: str, text: str):
    await room_send(room, {
        "type": "system",
        "sender": "Sistema",
        "room": room,
        "text": text,
        "timestamp": now_iso(),
    })


def bot_reply(text: str):
    q = (text or "").strip().lower()
    if not q:
        return "Escribe una consulta después de /bot."
    if any(x in q for x in ("hola", "hello", "buenas", "hi")):
        return "¡Hola! Soy FOCS Bot. ¿Listo para Fight of Characters?"
    if "ayuda" in q:
        return "Prueba: /bot comunidad, /bot medallas, /bot ranking, /bot partidas."
    if "medalla" in q:
        return "Sistema sugerido: +15 por victoria, cada 100 puntos una nueva medalla."
    if "ranking" in q:
        return "El ranking visual del cliente es demostrativo; luego se puede enlazar al mapa FOCS."
    if "partida" in q:
        return "La pestaña Play WC3 sirve como base visual para futuras colas y salas."
    return f"FOCS Bot recibió: «{text.strip()}»"


async def unregister(ws: WebSocket, notify=True):
    if ws not in meta:
        return
    m = meta.pop(ws)
    room = m["room"]
    username = m["username"]
    rooms[room].discard(ws)
    if by_name.get(username.lower()) is ws:
        by_name.pop(username.lower(), None)
    if not rooms.get(room):
        rooms.pop(room, None)
    if notify:
        if rooms.get(room):
            await system(room, f"{username} salió de la sala.")
            await broadcast_room_presence(room)
        await broadcast_global_presence()


@app.get('/')
async def root():
    return {
        'ok': True,
        'name': 'FOCS Community Final',
        'version': '3.0',
        'features': ['rooms', 'private_messages', 'buzz', 'presence', 'thoughts', 'wc3_queue_ui'],
    }


@app.get('/health')
async def health():
    return {'ok': True}


@app.websocket('/ws/{room}/{username}')
async def ws_chat(websocket: WebSocket, room: str, username: str):
    room = norm_room(room)
    username = norm_name(username)
    await websocket.accept()

    if username.lower() in by_name:
        await websocket.send_json({'type': 'error', 'text': f'El nombre "{username}" ya está en uso.', 'timestamp': now_iso()})
        await websocket.close()
        return

    rooms[room].add(websocket)
    meta[websocket] = {'room': room, 'username': username, 'status': 'online', 'thought': 'Ready to fight'}
    by_name[username.lower()] = websocket

    await system(room, f'{username} entró a la sala.')
    await broadcast_room_presence(room)
    await broadcast_global_presence()

    try:
        while True:
            data = await websocket.receive_json()
            action = str(data.get('action', 'chat')).strip().lower()
            m = meta[websocket]

            if action == 'presence':
                status = str(data.get('status', 'online')).strip().lower()
                if status not in {'online', 'away', 'busy', 'afk'}:
                    status = 'online'
                m['status'] = status
                thought = str(data.get('thought', m.get('thought', 'Ready to fight'))).strip()[:80]
                m['thought'] = thought or 'Ready to fight'
                await broadcast_room_presence(m['room'])
                await broadcast_global_presence()
                continue

            channel = str(data.get('channel', 'room')).strip().lower()
            target = norm_name(str(data.get('target', ''))) if data.get('target') else ''

            if action == 'buzz':
                payload = {
                    'type': 'buzz',
                    'channel': channel,
                    'sender': m['username'],
                    'target': target,
                    'room': m['room'],
                    'text': '¡Zumbido!',
                    'timestamp': now_iso(),
                }
                if channel == 'private':
                    dest = by_name.get(target.lower())
                    if not dest:
                        await safe_send(websocket, {'type': 'error', 'text': f'{target} no está conectado.'})
                        continue
                    await safe_send(websocket, payload)
                    if dest is not websocket:
                        await safe_send(dest, payload)
                else:
                    await room_send(m['room'], payload)
                continue

            text = str(data.get('text', '')).strip()[:2000]
            if not text:
                continue

            if channel == 'private':
                dest = by_name.get(target.lower())
                if not dest:
                    await safe_send(websocket, {'type': 'error', 'text': f'{target} no está conectado.'})
                    continue
                payload = {
                    'type': 'private',
                    'channel': 'private',
                    'sender': m['username'],
                    'target': target,
                    'text': text,
                    'timestamp': now_iso(),
                }
                await safe_send(websocket, payload)
                if dest is not websocket:
                    await safe_send(dest, payload)
            else:
                payload = {
                    'type': 'message',
                    'channel': 'room',
                    'sender': m['username'],
                    'room': m['room'],
                    'text': text,
                    'timestamp': now_iso(),
                }
                await room_send(m['room'], payload)
                if text.lower().startswith('/bot'):
                    await room_send(m['room'], {
                        'type': 'bot',
                        'sender': 'FOCS Bot',
                        'room': m['room'],
                        'text': bot_reply(text[4:].strip()),
                        'timestamp': now_iso(),
                    })
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await unregister(websocket)
