from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="FOCS Community Chat Alpha")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

rooms: Dict[str, Set[WebSocket]] = defaultdict(set)
users: Dict[str, Dict[WebSocket, str]] = defaultdict(dict)


def iso_now():
    return datetime.now(timezone.utc).isoformat()


async def send_room(room: str, payload: dict):
    dead = []
    for ws in list(rooms.get(room, set())):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)

    for ws in dead:
        rooms[room].discard(ws)
        users[room].pop(ws, None)


async def send_users(room: str):
    names = sorted(set(users.get(room, {}).values()), key=str.lower)
    await send_room(room, {
        "type": "users",
        "users": names,
        "timestamp": iso_now()
    })


def bot_reply(text: str) -> str:
    q = text.strip().lower()
    if not q:
        return "Escribe una consulta después de /bot."
    if any(k in q for k in ["hola", "buenas", "hello"]):
        return "¡Hola! Soy FOCS Bot Alpha. Estoy listo para pruebas."
    if "ayuda" in q:
        return "Puedes preguntarme por: comandos, comunidad, partidas, horario o versión."
    if "comunidad" in q:
        return "FOCS Community será el espacio de usuarios, salas, partidas y soporte."
    if "partida" in q or "jugar" in q:
        return "En esta versión todavía no creo partidas; por ahora estamos probando el chat."
    if "comando" in q:
        return "Prueba: /bot hola, /bot comunidad, /bot partidas, /bot versión."
    if "version" in q or "versión" in q:
        return "FOCS Community Chat Alpha 0.2."
    if "horario" in q:
        return "El bot de prueba está disponible mientras el servidor esté encendido."
    return f"FOCS Bot Alpha recibió: «{text.strip()}»"


@app.get("/")
async def index():
    return {
        "ok": True,
        "name": "FOCS Community Chat Alpha",
        "version": "0.2"
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.websocket("/ws/{room}/{username}")
async def ws_chat(websocket: WebSocket, room: str, username: str):
    room = (room.strip() or "general")[:40]
    username = (username.strip() or "Invitado")[:30]

    await websocket.accept()
    rooms[room].add(websocket)
    users[room][websocket] = username

    await send_room(room, {
        "type": "system",
        "sender": "Sistema",
        "text": f"{username} entró a la sala.",
        "timestamp": iso_now()
    })
    await send_users(room)

    try:
        while True:
            data = await websocket.receive_json()
            text = str(data.get("text", "")).strip()
            if not text:
                continue

            text = text[:2000]

            await send_room(room, {
                "type": "message",
                "sender": username,
                "text": text,
                "timestamp": iso_now()
            })

            if text.lower().startswith("/bot"):
                query = text[4:].strip()
                await send_room(room, {
                    "type": "bot",
                    "sender": "FOCS Bot",
                    "text": bot_reply(query),
                    "timestamp": iso_now()
                })

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        rooms[room].discard(websocket)
        users[room].pop(websocket, None)

        await send_room(room, {
            "type": "system",
            "sender": "Sistema",
            "text": f"{username} salió de la sala.",
            "timestamp": iso_now()
        })
        await send_users(room)

        if not rooms.get(room):
            rooms.pop(room, None)
            users.pop(room, None)
