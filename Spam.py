import asyncio
import threading
import sys
import json
import traceback
import time
import aiohttp
import ssl
from flask import Flask, request, jsonify

from Eren import *

bot_id = 16330728166
main_loop = None
ob_version = ob

active = False
target_uid = None
lock = threading.Lock()

join_active = False
join_target_room_id = None
join_password = ""
join_message = "Eren Yeager On Top"
join_lock = threading.Lock()

spam_loop_active = False
spam_room_id = None
spam_target_uid = None
spam_lock = threading.Lock()

clients = []
clients_lock = threading.Lock()

# ---------- all existing helper functions (create_custom_room_packet, send_room_invite, etc.) remain unchanged ----------
# (they are exactly as in your original file, so they are omitted here for brevity)

class SpamClient(CLIENT):
    def __init__(self, uid, password):
        super().__init__()
        self.account_uid = uid
        self.password = password
        self.room_created = False
        self.room_id = None
        self.online_connected = False
        self.reader = None
        self.expecting_create_response = False

        self.expecting_join_response = False
        self.join_room_id = None
        self.join_auth = None
        self.join_sent_auth = False
        self.join_sent_msg = False
        self.already_in_room = False

        self.chat_connected = False
        self.whisper_reader = None
        self.whisper_writer = None

        self.token = None
        self.region = None

    async def SEndPacKeT(self, OnLinE, ChaT, TypE, PacKeT):
        if TypE == 'OnLine':
            if self.online_writer is None:
                return False
            try:
                self.online_writer.write(PacKeT)
                await self.online_writer.drain()
                return True
            except Exception:
                return False
        elif TypE == 'ChaT':
            if self.whisper_writer is None:
                return False
            try:
                self.whisper_writer.write(PacKeT)
                await self.whisper_writer.drain()
                return True
            except Exception:
                return False
        return False

    async def chat_reader_loop(self):
        while self.whisper_reader and self.chat_connected:
            try:
                data = await self.whisper_reader.read(1024)
                if not data:
                    self.chat_connected = False
                    break
            except Exception:
                self.chat_connected = False
                break

    async def connect_chat(self, chat_ip, chat_port, auth_hex, key, iv, reconnect_delay=5):
        while True:
            try:
                reader, writer = await asyncio.open_connection(chat_ip, int(chat_port))
                self.whisper_reader = reader
                self.whisper_writer = writer
                self.chat_connected = True
                writer.write(bytes.fromhex(auth_hex))
                await writer.drain()
                print(f"[{self.account_uid}] ✅ Chat connected")
                asyncio.create_task(self.chat_reader_loop())
                while self.chat_connected:
                    await asyncio.sleep(1)
                print(f"[{self.account_uid}] Chat connection lost")
            except Exception as e:
                print(f"[{self.account_uid}] ❌ Chat connection error: {e}")
            if not self.chat_connected:
                await asyncio.sleep(reconnect_delay)

    async def send_direct_message(self, room_id, message, count=1):
        if self.join_room_id == room_id and self.join_sent_msg and self.chat_connected:
            for i in range(count):
                pkt = await send_room_message(room_id, message, self.key, self.iv)
                if await self.SEndPacKeT(None, None, 'ChaT', pkt):
                    print(f"[{self.account_uid}] ✅ Message {i+1}/{count} sent: {message[:20]}...")
                else:
                    print(f"[{self.account_uid}] ❌ Failed to send message {i+1}/{count}")
                if i < count - 1:
                    await asyncio.sleep(1)
            return True
        return False

    async def send_room_spam(self, room_id, target_uid):
        if self.online_writer and self.online_connected:
            pkt = await Room_Spam(room_id, target_uid, self.key, self.iv)
            if await self.SEndPacKeT(None, None, 'OnLine', pkt):
                print(f"[{self.account_uid}] ✅ Room spam sent (room: {room_id}, target: {target_uid})")
                return True
        return False

    async def send_friend_request(self, target_uid):
        if not self.token or not self.region:
            print(f"[{self.account_uid}] ⚠️ No token/region for friend request")
            return False

        region_lower = self.region.lower()
        if region_lower == "ind":
            url = "https://client.ind.freefiremobile.com/RequestAddingFriend"
        elif region_lower == "bd":
            url = "https://clientbp.ggpolarbear.com/RequestAddingFriend"
        else:
            url = "https://client.ind.freefiremobile.com/RequestAddingFriend"

        encrypted_payload = await encrypt_friend_payload(target_uid)
        if not encrypted_payload:
            return False

        headers = {
            "Authorization": f"Bearer {self.token}",
            "X-Unity-Version": "2018.4.11f1",
            "X-GA": "v1 1",
            "ReleaseVersion": f"{ob_version}",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "fadai/1.0 (Linux; Android 13; SM-S918B Build/TP1A.220.624.014)"
        }

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=bytes.fromhex(encrypted_payload), headers=headers, ssl=ssl_context, timeout=10) as resp:
                    if resp.status == 200:
                        print(f"[{self.account_uid}] ✅ Friend request sent to {target_uid}")
                        return True
                    else:
                        print(f"[{self.account_uid}] ❌ Friend request failed: {resp.status}")
                        return False
        except Exception as e:
            print(f"[{self.account_uid}] ❌ Friend request error: {e}")
            return False

    async def response_reader(self):
        global join_target_room_id
        while self.reader and self.online_connected:
            try:
                raw = await self.reader.read(4096)
                if not raw:
                    self.online_connected = False
                    break

                hex_data = raw.hex()
                print(f"[{self.account_uid}] 📩 Received {len(raw)} bytes")
                print(f"    Raw header (first 10 hex): {hex_data[:10]}")
                print(f"    Full raw: {hex_data}")

                if hex_data.startswith("0e00") and len(hex_data) > 10:
                    inner_hex = hex_data[10:]
                    try:
                        decoded = await DeCode_PackEt(inner_hex)
                        if decoded:
                            parsed = json.loads(decoded)
                            print(f"[{self.account_uid}] 📨 Decoded fields:\n{json.dumps(parsed, indent=2)}")

                            field4 = parsed.get('4', {}).get('data')
                            print(f"[{self.account_uid}] 🔍 Field 4 = {field4}")

                            if field4 == 25 or field4 == 14:
                                self.room_id = 99999999
                                self.room_created = True
                                reason = "SETREADY_NTF" if field4 == 25 else "CHANGE_NTF"
                                print(f"[{self.account_uid}] ⚠️ {reason} (field4={field4}) – already in room, dummy room_id 99999999")
                                if self.expecting_create_response:
                                    self.expecting_create_response = False
                                self.already_in_room = True
                                if join_target_room_id:
                                    self.join_room_id = int(join_target_room_id)
                                    self.join_auth = "dummy_auth"
                                continue

                            if self.expecting_join_response and field4 == 5:
                                data = parsed.get('5', {}).get('data', {})
                                if '2' in data and 'data' in data['2']:
                                    inner = data['2']['data']
                                    room_auth = inner.get('36', {}).get('data')
                                    room_id = inner.get('1', {}).get('data')
                                    if room_auth and room_id:
                                        self.join_room_id = room_id
                                        self.join_auth = str(room_auth)
                                        print(f"[{self.account_uid}] ✅ Joined room! Room_ID: {room_id}, Auth: {room_auth}")
                                        self.expecting_join_response = False
                                        self.already_in_room = False
                                    else:
                                        print(f"[{self.account_uid}] ⚠️ Join success but missing auth or room_id")
                                else:
                                    print(f"[{self.account_uid}] ⚠️ Join success but no data structure")
                                continue

                            if self.expecting_join_response and field4 is None:
                                print(f"[{self.account_uid}] ⚠️ Received field4=None while waiting for join response – sending dismiss")
                                leave_room_id = int(join_target_room_id) if join_target_room_id else 99999999
                                leave_pkt = await Room_Leave(leave_room_id, self.key, self.iv)
                                await self.SEndPacKeT(None, None, 'OnLine', leave_pkt)
                                self.expecting_join_response = False
                                continue

                            if self.expecting_create_response:
                                if field4 == 2:
                                    room_id = parsed.get('5', {}).get('data', {}).get('1', {}).get('data')
                                    if room_id:
                                        self.room_id = room_id
                                        self.room_created = True
                                        print(f"[{self.account_uid}] ✅ Room created! ID: {room_id}")
                                    else:
                                        print(f"[{self.account_uid}] ⚠️ Success but no room_id")
                                elif field4 is None:
                                    if not self.room_created:
                                        print(f"[{self.account_uid}] ⚠️ No field4 in response – assuming room exists?")
                                        self.room_id = 99999999
                                        self.room_created = True
                                else:
                                    print(f"[{self.account_uid}] ❌ Room creation failed (field4={field4})")
                                    self.room_created = False
                                    self.room_id = None
                                self.expecting_create_response = False
                            else:
                                if field4 in (8, 22):
                                    type_name = "DISMISS_NTF" if field4 == 8 else "INVITE_NTF"
                                    print(f"[{self.account_uid}] 📨 {type_name} (field4={field4}) – ignored")
                                else:
                                    print(f"[{self.account_uid}] 📨 Other response (field4={field4}) – ignored")
                    except Exception as e:
                        print(f"[{self.account_uid}] ❌ Decode error: {e}")
                        print(f"    inner_hex: {inner_hex[:100]}")
            except Exception as e:
                print(f"[{self.account_uid}] ❌ Online reader error: {e}")
                self.online_connected = False
                break

    async def room_loop(self):
        global active, target_uid, join_active, join_target_room_id, join_password, join_message, spam_loop_active, spam_room_id, spam_target_uid
        print(f"[{self.account_uid}] 🔄 room_loop started")
        while self.online_writer and self.online_connected:
            if join_active and join_target_room_id:
                if not self.join_sent_auth:
                    print(f"[{self.account_uid}] 🚪 Joining room {join_target_room_id} with pass '{join_password}'...")
                    self.expecting_join_response = True
                    pkt = await RoomJoin_fields(join_target_room_id, join_password, self.key, self.iv)
                    print(f"[{self.account_uid}] ➡️ Join hex: {pkt.hex()[:100]}...")
                    if await self.SEndPacKeT(None, None, 'OnLine', pkt):
                        print(f"[{self.account_uid}] ✅ Join packet sent, waiting for response...")
                        for _ in range(10):
                            await asyncio.sleep(1)
                            if not self.expecting_join_response:
                                break
                        if self.expecting_join_response:
                            self.expecting_join_response = False
                            print(f"[{self.account_uid}] ⏰ Timeout waiting for join response – retrying")
                            await asyncio.sleep(2)
                            continue
                    else:
                        print(f"[{self.account_uid}] ❌ Failed to send join")
                        self.expecting_join_response = False
                        await asyncio.sleep(2)
                        continue

                if self.join_room_id and self.join_auth and self.chat_connected:
                    if not self.join_sent_auth:
                        print(f"[{self.account_uid}] 🔐 Sending channel auth...")
                        auth_pkt = await join_room_channel(self.join_room_id, self.join_auth, self.key, self.iv)
                        print(f"[{self.account_uid}] ➡️ Auth hex: {auth_pkt.hex()[:100]}...")
                        if await self.SEndPacKeT(None, None, 'ChaT', auth_pkt):
                            self.join_sent_auth = True
                            print(f"[{self.account_uid}] ✅ Auth sent")
                        else:
                            print(f"[{self.account_uid}] ❌ Failed to send auth")
                        await asyncio.sleep(1)
                        continue

                    if self.join_sent_auth and not self.join_sent_msg:
                        print(f"[{self.account_uid}] 💬 Sending message: {join_message}")
                        msg_pkt = await send_room_message(self.join_room_id, join_message, self.key, self.iv)
                        print(f"[{self.account_uid}] ➡️ Msg hex: {msg_pkt.hex()[:100]}...")
                        if await self.SEndPacKeT(None, None, 'ChaT', msg_pkt):
                            self.join_sent_msg = True
                            print(f"[{self.account_uid}] ✅ Message sent")
                        else:
                            print(f"[{self.account_uid}] ❌ Failed to send message")
                        await asyncio.sleep(2)
                        continue

                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(1)
                    continue

            elif active and target_uid:
                if not self.room_created:
                    print(f"[{self.account_uid}] 🏗️ Creating room...")
                    self.expecting_create_response = True
                    pkt = await create_custom_room_packet(self.key, self.iv)
                    print(f"[{self.account_uid}] ➡️ Create hex: {pkt.hex()[:100]}...")
                    if await self.SEndPacKeT(None, None, 'OnLine', pkt):
                        print(f"[{self.account_uid}] ✅ Create packet sent, waiting for response...")
                        for _ in range(5):
                            await asyncio.sleep(1)
                            if not self.expecting_create_response:
                                break
                        if self.expecting_create_response:
                            self.expecting_create_response = False
                            if not self.room_created:
                                print(f"[{self.account_uid}] ⏰ Timeout – retrying")
                            else:
                                print(f"[{self.account_uid}] ⏰ Timeout but room_created is True – proceeding")
                    else:
                        print(f"[{self.account_uid}] ❌ Failed to send create")
                        self.expecting_create_response = False
                    await asyncio.sleep(2)
                    continue

                if self.room_created and self.room_id:
                    print(f"[{self.account_uid}] 📨 Sending invite to {target_uid}")
                    pkt = await send_room_invite(int(target_uid), self.key, self.iv)
                    print(f"[{self.account_uid}] ➡️ Invite hex: {pkt.hex()[:100]}...")
                    await self.SEndPacKeT(None, None, 'OnLine', pkt)
                    await asyncio.sleep(2)
                else:
                    await asyncio.sleep(1)

            elif spam_loop_active and spam_room_id and spam_target_uid:
                print(f"[{self.account_uid}] 📨 Sending room spam (room: {spam_room_id}, target: {spam_target_uid})")
                pkt = await Room_Spam(int(spam_room_id), int(spam_target_uid), self.key, self.iv)
                print(f"[{self.account_uid}] ➡️ Spam hex: {pkt.hex()[:100]}...")
                await self.SEndPacKeT(None, None, 'OnLine', pkt)
                await asyncio.sleep(2)
                continue

            else:
                if self.join_room_id and (self.join_sent_auth or self.join_sent_msg):
                    print(f"[{self.account_uid}] 🔴 Leaving room {self.join_room_id}")
                    leave_pkt = await Room_Leave(self.join_room_id, self.key, self.iv)
                    await self.SEndPacKeT(None, None, 'OnLine', leave_pkt)
                    self.join_room_id = None
                    self.join_auth = None
                    self.join_sent_auth = False
                    self.join_sent_msg = False
                    self.expecting_join_response = False
                    self.already_in_room = False

                if self.room_created and self.room_id and (self.room_id != 99999999):
                    print(f"[{self.account_uid}] 🔴 Dismissing room {self.room_id}")
                    dismiss_pkt = await Room_Dismiss(self.room_id, self.key, self.iv)
                    await self.SEndPacKeT(None, None, 'OnLine', dismiss_pkt)
                    self.room_created = False
                    self.room_id = None
                else:
                    self.room_created = False
                    self.room_id = None

                await asyncio.sleep(1)

            if not self.online_connected:
                break
        print(f"[{self.account_uid}] 🔴 room_loop ended")

    # ---------- NEW: completely rewritten MaiiiinE with auto-reconnect ----------
    async def MaiiiinE(self):
        Uid, Pw = self.account_uid, self.password

        while True:   # outer loop: re-login if needed
            try:
                print(f"[{Uid}] 🔑 Logging in...")
                open_id, access_token = await GeNeRaTeAccEss(Uid, Pw)
                if not open_id or not access_token:
                    print(f"[{Uid}] ❌ Login failed, retrying in 5s...")
                    await asyncio.sleep(5)
                    continue

                PyL = await EncryptMajorLoginManual(open_id, access_token, region_code="TW", platform=4)
                MajoRLoGinResPonsE = await MajorLogin(PyL)
                if not MajoRLoGinResPonsE:
                    print(f"[{Uid}] ❌ MajorLogin failed, retrying in 5s...")
                    await asyncio.sleep(5)
                    continue

                MajoRLoGinauTh = await DecRypTMajoRLoGin(MajoRLoGinResPonsE)
                self.key = MajoRLoGinauTh.key
                self.iv = MajoRLoGinauTh.iv
                timestamp = MajoRLoGinauTh.timestamp
                TarGeT = MajoRLoGinauTh.account_uid
                ToKen = MajoRLoGinauTh.token
                self.JWT = ToKen
                self.token = ToKen

                LoGinDaTa = await GetLoginData(MajoRLoGinauTh.url, PyL, ToKen)
                if not LoGinDaTa:
                    print(f"[{Uid}] ❌ GetLoginData failed, retrying in 5s...")
                    await asyncio.sleep(5)
                    continue
                LoGinDaTaUncRypTinG = await DecRypTLoGinDaTa(LoGinDaTa)
                self.region = LoGinDaTaUncRypTinG.Region

                OnLinePorTs = LoGinDaTaUncRypTinG.Online_IP_Port
                ChaTPorTs = LoGinDaTaUncRypTinG.AccountIP_Port
                self.OnLineiP, self.OnLineporT = OnLinePorTs.split(":")
                ChatIP, ChatPort = ChaTPorTs.split(":")
                print(f"[{Uid}] ✅ Online server: {self.OnLineiP}:{self.OnLineporT}")
                print(f"[{Uid}] ✅ Chat server: {ChatIP}:{ChatPort}")

                AutHToKen = await xAuThSTarTuP(int(TarGeT), ToKen, int(timestamp), self.key, self.iv)
                print(f"[{Uid}] 🔑 Auth hex: {AutHToKen[:100]}...")

                # Start chat reconnect loop (already has its own infinite loop)
                asyncio.create_task(self.connect_chat(ChatIP, ChatPort, AutHToKen, self.key, self.iv))

                # Inner online reconnect loop
                while True:
                    try:
                        reader, writer = await asyncio.open_connection(self.OnLineiP, int(self.OnLineporT))
                        self.reader = reader
                        self.online_writer = writer
                        self.online_connected = True

                        writer.write(bytes.fromhex(AutHToKen))
                        await writer.drain()
                        print(f"[{Uid}] ✅ Online connected")

                        # Launch tasks
                        response_task = asyncio.create_task(self.response_reader())
                        room_task = asyncio.create_task(self.room_loop())

                        # Wait until connection is lost (set by response_reader or room_loop)
                        while self.online_connected:
                            await asyncio.sleep(1)

                        # Cancel tasks
                        response_task.cancel()
                        room_task.cancel()
                        try:
                            await response_task
                        except asyncio.CancelledError:
                            pass
                        try:
                            await room_task
                        except asyncio.CancelledError:
                            pass

                        # Clean up writer
                        if self.online_writer:
                            try:
                                self.online_writer.close()
                                await self.online_writer.wait_closed()
                            except:
                                pass
                            self.online_writer = None
                        self.reader = None
                        self.online_connected = False

                        # Reset room state for next connection
                        self.room_created = False
                        self.room_id = None
                        self.expecting_create_response = False
                        self.join_room_id = None
                        self.join_auth = None
                        self.join_sent_auth = False
                        self.join_sent_msg = False
                        self.expecting_join_response = False
                        self.already_in_room = False

                        print(f"[{Uid}] Online connection lost, reconnecting in 5s...")
                        await asyncio.sleep(5)

                    except Exception as e:
                        print(f"[{Uid}] ❌ Online connection error: {e}")
                        self.online_connected = False
                        if self.online_writer:
                            try:
                                self.online_writer.close()
                                await self.online_writer.wait_closed()
                            except:
                                pass
                            self.online_writer = None
                        self.reader = None
                        await asyncio.sleep(5)

            except Exception as e:
                print(f"[{Uid}] ❌ Login/connection error: {e}")
                traceback.print_exc()
                await asyncio.sleep(5)

# ---------- Flask routes (unchanged) ----------
app = Flask(__name__)

@app.route('/Room-Req', methods=['GET'])
def room_req():
    global active, target_uid
    uid = request.args.get('Uid')
    type_ = request.args.get('type')
    if not uid or not type_:
        return jsonify({'status': 'error', 'message': 'Missing Uid or type'}), 400
    if type_ == 'start':
        with lock:
            active = True
            target_uid = uid
        print(f"[Flask] 🟢 START spam to {uid}")
        return jsonify({'status': 'started', 'target': uid})
    elif type_ == 'stop':
        with lock:
            active = False
            target_uid = None
        print("[Flask] 🔴 STOP spam")
        return jsonify({'status': 'stopped'})
    return jsonify({'status': 'error', 'message': 'Invalid type'}), 400

@app.route('/Room-Join', methods=['GET'])
def room_join():
    global join_active, join_target_room_id, join_password, join_message
    room_id = request.args.get('Room_Id')
    type_ = request.args.get('type')
    password = request.args.get('pass', '')
    msg = request.args.get('msg', 'Eren Yeager On Top')

    if not room_id or not type_:
        return jsonify({'status': 'error', 'message': 'Missing Room_Id or type'}), 400

    if type_ == 'start':
        with join_lock:
            join_active = True
            join_target_room_id = room_id
            join_password = password
            join_message = msg
        print(f"[Flask] 🟢 JOIN room {room_id} with pass '{password}', msg: {msg}")
        return jsonify({'status': 'started', 'room': room_id})
    elif type_ == 'stop':
        with join_lock:
            join_active = False
            join_target_room_id = None
            join_password = ""
            join_message = "Eren Yeager On Top"
        print("[Flask] 🔴 STOP join – leaving room")
        return jsonify({'status': 'stopped'})
    else:
        return jsonify({'status': 'error', 'message': 'Invalid type'}), 400

@app.route('/Room-Msg', methods=['GET'])
def room_msg():
    global main_loop
    room_id = request.args.get('room_id')
    msg = request.args.get('msg')
    spam = request.args.get('spam', 1)

    if not room_id or not msg:
        return jsonify({'status': 'error', 'message': 'Missing room_id or msg'}), 400

    try:
        count = int(spam)
    except:
        count = 1
    if count < 1:
        count = 1

    sent = 0
    with clients_lock:
        for client in clients:
            if client.join_room_id == int(room_id) and client.join_sent_msg and client.chat_connected:
                if main_loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        client.send_direct_message(int(room_id), msg, count),
                        main_loop
                    )
                    sent += 1

    if sent == 0:
        return jsonify({'status': 'error', 'message': 'No bot is currently in that room or chat not ready'}), 400

    return jsonify({'status': 'ok', 'sent': sent, 'count': count})

@app.route('/Room-Spam', methods=['GET'])
def room_spam():
    global spam_loop_active, spam_room_id, spam_target_uid
    room_id = request.args.get('room_id')
    target_uid = request.args.get('target_uid')
    type_ = request.args.get('type')

    if type_ == 'start':
        if not room_id or not target_uid:
            return jsonify({'status': 'error', 'message': 'Missing room_id or target_uid'}), 400
        with spam_lock:
            spam_loop_active = True
            spam_room_id = room_id
            spam_target_uid = target_uid
        print(f"[Flask] 🟢 START room spam loop: room={room_id}, target={target_uid}")
        return jsonify({'status': 'started', 'room': room_id, 'target': target_uid})
    elif type_ == 'stop':
        with spam_lock:
            spam_loop_active = False
            spam_room_id = None
            spam_target_uid = None
        print("[Flask] 🔴 STOP room spam loop")
        return jsonify({'status': 'stopped'})
    else:
        return jsonify({'status': 'error', 'message': 'Invalid type'}), 400

@app.route('/Friend-Spam', methods=['GET'])
def friend_spam():
    global main_loop
    target = request.args.get('target')
    if not target:
        return jsonify({'status': 'error', 'message': 'Missing target'}), 400

    sent = 0
    with clients_lock:
        for client in clients:
            if client.online_connected and client.token:
                if main_loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        client.send_friend_request(int(target)),
                        main_loop
                    )
                    sent += 1

    if sent == 0:
        return jsonify({'status': 'error', 'message': 'No online clients with token'}), 400

    return jsonify({'status': 'ok', 'sent': sent, 'target': target})

def run_flask():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

async def spam_main():
    global clients, main_loop
    main_loop = asyncio.get_running_loop()

    accounts = []
    try:
        with open('accounts.txt', 'r', encoding='utf-8') as f:
            for line in f:
                if '|' in line:
                    parts = line.strip().split('|')
                    if len(parts) == 2:
                        uid, pwd = parts[0].strip(), parts[1].strip()
                        if uid and pwd:
                            accounts.append((uid, pwd))
    except FileNotFoundError:
        print("accounts.txt not found.")
        sys.exit(1)

    if not accounts:
        print("No accounts loaded.")
        return

    print(f"Loaded {len(accounts)} accounts.")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    clients = [SpamClient(uid, pwd) for uid, pwd in accounts]
    tasks = [client.MaiiiinE() for client in clients]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    try:
        asyncio.run(spam_main())
    except KeyboardInterrupt:
        print("\nExiting...")