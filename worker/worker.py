import asyncio
import os
import uuid
import io
import mimetypes
import re
import unicodedata
import traceback
import time
from typing import Dict, Set
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import SetTypingRequest
from telethon.tl.types import SendMessageTypingAction, SendMessageRecordVideoAction, SendMessageRecordAudioAction
import httpx
import tempfile
import functools
import json

load_dotenv()

# --- Configuration (remains the same) ---
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not all([API_ID, API_HASH, SUPABASE_SERVICE_KEY]):
    raise ValueError("FATAL: Required environment variables are not set.")

from supabase import create_client
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def sanitize_filename(filename):
    # Normalize unicode to closest ASCII representation
    filename = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore').decode('ascii')
    # Replace any character that is not a letter, number, dot, dash, or underscore with '_'
    filename = re.sub(r'[^A-Za-z0-9._-]', '_', filename)
    return filename

async def account_worker(account: dict):
    account_id = account.get('id')
    session_string = account.get('session_string')
    username = account.get('username', account_id)
    MAX_RETRIES = 3 # Define the maximum number of retries for a failed message
    # Track typing state per chat
    typing_states = {}  # chat_id -> last_typing_time
    if not session_string: print(f"Account '{username}' has no session string. Skipping."); return
    client = TelegramClient(StringSession(session_string), int(API_ID), API_HASH)
    # --- Create a single shared httpx.AsyncClient for the worker lifetime ---
    http_client = httpx.AsyncClient(
        timeout=30.0,
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20
        )
    )
    try:
        await client.start()
    except Exception as e:
        print(f"Failed to start client for '{username}': {e}. Setting status 'requires_reauth'.")
        supabase.table("managed_telegram_accounts").update({"status": "requires_reauth"}).eq("id", account_id).execute(); return
    try:
        print(f"[{username}] Pre-caching entities...")
        async for _ in client.iter_dialogs(): pass
        print(f"[{username}] Entity cache populated.")
    except Exception as e: print(f"[{username}][Warning] Could not pre-cache entities: {e}")
    print(f"✅ Worker started for account: '{username}'")

    @client.on(events.NewMessage(incoming=True))
    async def handle_incoming_message(event):
        if event.message.grouped_id:
            return
        import traceback
        sender = await event.get_sender()
        if not event.is_private or sender is None or sender.bot or sender.is_self:
            return
        message = event.message
        contact_telegram_id = str(event.chat_id)
        temp_path = None
        try:
            contact_response = supabase.table("contacts").select("id, unread_count, message_count_since_avatar_update").eq("managed_account_id", account_id).eq("contact_telegram_id", contact_telegram_id).limit(1).execute()
            contact_record = None
            if not contact_response.data:
                print(f"[{username}] New conversation. Creating contact entry for TG ID: {contact_telegram_id}")
                new_contact_payload = {"managed_account_id": account_id, "contact_telegram_id": contact_telegram_id, "name": getattr(sender, 'first_name', "Unknown"), "message_count_since_avatar_update": 0}
                insert_response = supabase.table("contacts").insert(new_contact_payload).execute()
                contact_record = insert_response.data[0]
            else:
                contact_record = contact_response.data[0]
            AVATAR_UPDATE_THRESHOLD = 20
            current_avatar_check_count = contact_record.get('message_count_since_avatar_update', 0) or 0
            new_avatar_check_count = current_avatar_check_count + 1
            contact_update_payload = {"last_message_preview": message.text[:50] if message.text else "[Media]", "last_message_timestamp": message.date.isoformat(), "unread_count": (contact_record.get('unread_count', 0) or 0) + 1, "message_count_since_avatar_update": new_avatar_check_count}
            if new_avatar_check_count >= AVATAR_UPDATE_THRESHOLD:
                print(f"[{username}] Avatar check threshold reached for {contact_telegram_id}.")
                avatar_temp_path = None
                try:
                    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp_file:
                        avatar_temp_path = tmp_file.name
                    if await client.download_profile_photo(sender, file=avatar_temp_path):
                        storage_path = f"avatars/{contact_telegram_id}.jpg"
                        with open(avatar_temp_path, 'rb') as f:
                            supabase.storage.from_('chat-attachments').upload(path=storage_path, file=f, file_options={"content-type": "image/jpeg", "upsert": "true"})
                        public_url = supabase.storage.from_('chat-attachments').get_public_url(storage_path)
                        contact_update_payload['avatar_url'] = public_url
                        print(f"[{username}] Successfully updated avatar for {contact_telegram_id}.")
                    else:
                        print(f"[{username}] Contact {contact_telegram_id} has no profile photo.")
                except Exception as e:
                    print(f"[{username}][Avatar Error] Could not update avatar: {e}")
                    traceback.print_exc()
                finally:
                    contact_update_payload['message_count_since_avatar_update'] = 0
                    if avatar_temp_path and os.path.exists(avatar_temp_path):
                        os.remove(avatar_temp_path)
            supabase.table("contacts").update(contact_update_payload).eq("id", contact_record['id']).execute()
            message_payload = {"contact_id": contact_record['id'], "chat_id": contact_telegram_id, "managed_account_id": account_id, "sender_name": getattr(sender, 'first_name', "Unknown"), "text": message.text or "", "timestamp": message.date.isoformat(), "is_own_message": False, "is_seen": False, "is_video_note": bool(message.video_note), "is_voice_note": bool(message.voice), "tg_message_id": message.id, "status": "sent"}
            if message.media:
                def upload_in_thread_with_retries(storage_path, temp_path, mime_type):
                    for attempt in range(3):
                        try:
                            with open(temp_path, 'rb') as f:
                                supabase.storage.from_('chat-attachments').upload(path=storage_path, file=f, file_options={"content-type": mime_type, "upsert": "true"})
                            print(f"[{username}] Successfully uploaded {storage_path}")
                            return
                        except httpx.RemoteProtocolError as e:
                            print(f"[{username}][Upload Attempt {attempt + 1} failed]: {e}. Retrying in {2**attempt}s...")
                            time.sleep(2**attempt)
                    raise Exception(f"Failed to upload {storage_path} after multiple retries.")
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    temp_path = tmp_file.name
                await message.download_media(file=temp_path)
                filename = getattr(message.file, 'name', None)
                mime_type = getattr(message.file, 'mime_type', None)
                if not filename:
                    guessed_ext = mimetypes.guess_extension(mime_type or '') or '.dat'
                    filename = f"media_{contact_telegram_id}_{message.id}{guessed_ext}"
                if not mime_type:
                    mime_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
                filename = sanitize_filename(filename)
                storage_path = f"{contact_telegram_id}/{filename}"
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, upload_in_thread_with_retries, storage_path, temp_path, mime_type)
                public_url = supabase.storage.from_('chat-attachments').get_public_url(storage_path)
                message_payload.update({"attachment_url": public_url, "attachment_name": filename, "attachment_type": mime_type, "attachment_size": getattr(message.file, 'size', None)})
            supabase.table("messages").insert(message_payload).execute()
            print(f"[{username}] Saved incoming message from {contact_telegram_id} to contact record {contact_record['id']}.")
        except Exception as e:
            print(f"[{username}][Error] Processing incoming message failed: {e}")
            traceback.print_exc()
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    # --- MODIFIED: The Album handler is now completely different ---
    @client.on(events.Album)
    async def handle_album(event):
        import traceback
        sender = await event.get_sender()
        if not event.is_private or sender is None or sender.bot or sender.is_self:
            return
        contact_telegram_id = str(event.chat_id)
        try:
            contact_response = supabase.table("contacts").select("id, unread_count, message_count_since_avatar_update").eq("managed_account_id", account_id).eq("contact_telegram_id", contact_telegram_id).limit(1).execute()
            contact_record = contact_response.data[0] if contact_response.data else None
            if not contact_record:
                new_contact_payload = {"managed_account_id": account_id, "contact_telegram_id": contact_telegram_id, "name": getattr(sender, 'first_name', "Unknown"), "message_count_since_avatar_update": 0}
                contact_record = supabase.table("contacts").insert(new_contact_payload).execute().data[0]
            album_preview = f"[{len(event.messages)} items] {event.text}" if event.text else f"[{len(event.messages)} media items]"
            contact_update_payload = {"last_message_preview": album_preview[:50], "last_message_timestamp": event.messages[-1].date.isoformat(), "unread_count": (contact_record.get('unread_count', 0) or 0) + 1, "message_count_since_avatar_update": (contact_record.get('message_count_since_avatar_update', 0) or 0) + len(event.messages)}
            supabase.table("contacts").update(contact_update_payload).eq("id", contact_record['id']).execute()
            attachments_data = []
            for message in event.messages:
                temp_path = None
                try:
                    if not message.media: continue
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        temp_path = tmp_file.name
                    await message.download_media(file=temp_path)
                    filename = getattr(message.file, 'name', None)
                    mime_type = getattr(message.file, 'mime_type', None)
                    if not filename:
                        guessed_ext = mimetypes.guess_extension(mime_type or '') or '.dat'
                        filename = f"media_{contact_telegram_id}_{message.id}{guessed_ext}"
                    if not mime_type:
                        mime_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
                    filename = sanitize_filename(filename)
                    storage_path = f"{contact_telegram_id}/{filename}"
                    def upload_in_thread_with_retries(storage_path, temp_path, mime_type):
                        for attempt in range(3):
                            try:
                                with open(temp_path, 'rb') as f:
                                    supabase.storage.from_('chat-attachments').upload(path=storage_path, file=f, file_options={"content-type": mime_type, "upsert": "true"})
                                return
                            except httpx.RemoteProtocolError as e:
                                print(f"[{username}][Upload Attempt {attempt + 1} failed]: {e}. Retrying...")
                                time.sleep(2**attempt)
                        raise Exception(f"Failed to upload {storage_path}")
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, upload_in_thread_with_retries, storage_path, temp_path, mime_type)
                    public_url = supabase.storage.from_('chat-attachments').get_public_url(storage_path)
                    attachments_data.append({
                        "url": public_url,
                        "name": filename,
                        "type": mime_type,
                        "size": getattr(message.file, 'size', None)
                    })
                    print(f"[{username}] Successfully uploaded album item: {filename}")
                finally:
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)
            if attachments_data:
                album_message_payload = {
                    "contact_id": contact_record['id'],
                    "chat_id": contact_telegram_id,
                    "managed_account_id": account_id,
                    "sender_name": getattr(sender, 'first_name', "Unknown"),
                    "text": event.text or "",
                    "timestamp": event.messages[-1].date.isoformat(),
                    "is_own_message": False,
                    "is_seen": False,
                    "is_album": True,
                    "attachments": attachments_data,
                    "tg_message_id": event.messages[0].id,
                    "status": "sent"
                }
                supabase.table("messages").insert(album_message_payload).execute()
                print(f"[{username}] Saved album with {len(attachments_data)} items to a single DB record for contact {contact_record['id']}.")
        except Exception as e:
            print(f"[{username}][Error] Processing album failed: {e}")
            traceback.print_exc()

    async def poll_for_outgoing():
        async def process_one_message(msg_to_send):
            temp_paths_to_clean = []
            try:
                print(f"[{username}] Processing message: {msg_to_send['id']}")
                entity = await client.get_entity(int(msg_to_send['chat_id']))
                
                await client.send_read_acknowledge(entity)
                await asyncio.sleep(2)

                is_album = msg_to_send.get('is_album', False)
                text_caption = msg_to_send.get('text', '')
                sent_msgs = []

                if is_album:
                    attachments = msg_to_send.get('attachments')
                    if not attachments or not isinstance(attachments, list):
                        raise ValueError(f"Message {msg_to_send['id']} is an album but has no valid attachments data.")
                    
                    print(f"[{username}] Preparing to send album with {len(attachments)} items.")
                    files_to_send = []
                    
                    for attachment in attachments:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{attachment.get('name', 'file.dat')}") as tmp_file:
                            temp_path = tmp_file.name
                            temp_paths_to_clean.append(temp_path)
                        async with http_client.stream("GET", attachment['url']) as response_file:
                            response_file.raise_for_status()
                            with open(temp_path, "wb") as out_file:
                                async for chunk in response_file.aiter_bytes():
                                    out_file.write(chunk)
                        files_to_send.append(temp_path)
                    # Only show typing if there is a caption for the album
                    if text_caption:
                        await client(SetTypingRequest(peer=entity, action=SendMessageTypingAction()))
                        await asyncio.sleep(5)
                    sent_msgs = await client.send_file(entity, file=files_to_send, caption=text_caption, parse_mode='md')
                else:
                    attachment_url = msg_to_send.get('attachment_url')
                    # Text-only message
                    if not attachment_url:
                        chat_id = msg_to_send['chat_id']
                        current_time = time.time()
                        last_typing = typing_states.get(chat_id, 0)
                        
                        if current_time - last_typing >= 15:  # 15 seconds since last typing
                            await client(SetTypingRequest(peer=entity, action=SendMessageTypingAction()))
                            typing_states[chat_id] = current_time
                            await asyncio.sleep(5)
                        
                        sent_msg = await client.send_message(entity, text_caption, parse_mode='md')
                        sent_msgs.append(sent_msg)
                    # Single media message
                    else:
                        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                            temp_path = tmp_file.name
                            temp_paths_to_clean.append(temp_path)
                        async with http_client.stream("GET", attachment_url) as response_file:
                            response_file.raise_for_status()
                            with open(temp_path, "wb") as out_file:
                                async for chunk in response_file.aiter_bytes():
                                    out_file.write(chunk)
                        uploaded_file = await client.upload_file(
                            file=temp_path,
                            file_name=msg_to_send.get('attachment_name', 'attachment.dat')
                        )
                        is_video_note = msg_to_send.get('is_video_note', False)
                        is_voice_note = msg_to_send.get('is_voice_note', False)
                        if is_video_note:
                            await client(SetTypingRequest(peer=entity, action=SendMessageRecordVideoAction()))
                            await asyncio.sleep(5)
                        elif is_voice_note:
                            await client(SetTypingRequest(peer=entity, action=SendMessageRecordAudioAction()))
                            await asyncio.sleep(5)
                        elif text_caption: # For regular files, only show typing if there's a caption
                            chat_id = msg_to_send['chat_id']
                            current_time = time.time()
                            last_typing = typing_states.get(chat_id, 0)
                            
                            if current_time - last_typing >= 15:
                                await client(SetTypingRequest(peer=entity, action=SendMessageTypingAction()))
                                typing_states[chat_id] = current_time
                                await asyncio.sleep(5)
                        sent_msg = await client.send_file(
                            entity,
                            file=uploaded_file,
                            caption=text_caption,
                            parse_mode='md',
                            video_note=is_video_note,
                            voice_note=is_voice_note
                        )
                        sent_msgs.append(sent_msg)
                # --- DB Update on Success ---
                first_sent_msg_id = sent_msgs[0].id if sent_msgs else None
                supabase.table("messages").update({
                    "status": "sent", "tg_message_id": first_sent_msg_id
                }).eq("id", msg_to_send["id"]).execute()
                print(f"[{username}] Successfully sent message: {msg_to_send['id']}")
            except Exception as e:
                error_details = traceback.format_exc()
                print(f"[{username}][Error] Sending message {msg_to_send['id']} failed: {e}")
                current_retries = msg_to_send.get('retry_count', 0)
                if current_retries < MAX_RETRIES:
                    supabase.table("messages").update({
                        "status": "pending", "retry_count": current_retries + 1, "last_error": str(e)
                    }).eq("id", msg_to_send["id"]).execute()
                else:
                    supabase.table("messages").update({
                        "status": "failed", "last_error": f"Failed after {MAX_RETRIES} retries: {error_details}"
                    }).eq("id", msg_to_send["id"]).execute()
            finally:
                for path in temp_paths_to_clean:
                    if os.path.exists(path):
                        os.remove(path)
        # MODIFIED main polling loop
        while client.is_connected():
            try:
                # 1. First, perform a lightweight count query to check for work.
                # This is very efficient and avoids downloading full message data just to check.
                count_response = supabase.table("messages").select("id", count='exact') \
                    .eq("status", "pending") \
                    .eq("managed_account_id", account_id) \
                    .execute()

                # 2. Only proceed if there are messages to process.
                if count_response.count == 0:
                    await asyncio.sleep(5)  # Wait peacefully before checking again
                    continue

                print(f"[{username}] Found {count_response.count} pending message(s). Fetching details...")
                # 3. Now fetch the full message data since we know work exists.
                response = supabase.table("messages").select("*") \
                    .eq("status", "pending") \
                    .eq("managed_account_id", account_id) \
                    .order("timestamp", desc=False) \
                    .limit(10) \
                    .execute()

                # This check is good for safety but unlikely to be needed if count > 0
                if not response.data:
                    continue

                messages_to_process = response.data
                message_ids = [msg['id'] for msg in messages_to_process]
                supabase.table("messages").update({"status": "processing"}).in_("id", message_ids).execute()
                tasks = [process_one_message(msg) for msg in messages_to_process]
                await asyncio.gather(*tasks)

            except Exception as e:
                print(f"[{username}][CRITICAL POLLER ERROR] The main polling loop failed: {e}")
                traceback.print_exc()
                await asyncio.sleep(15) # Wait longer if a critical error occurs

    async def delete_marked_messages():
        while True:
            try:
                # 1. First, perform a lightweight count query.
                count_response = supabase.table("messages").select("id", count='exact') \
                    .eq("managed_account_id", account_id) \
                    .eq("marked_for_deletion", True) \
                    .not_.is_("tg_message_id", None) \
                    .eq("deleted_by_worker", False) \
                    .execute()

                # 2. Only proceed if there are messages to delete.
                if count_response.count > 0:
                    print(f"[{username}][Deletion] Found {count_response.count} message(s) marked for deletion. Fetching details...")
                    
                    # 3. Now fetch the full data for the messages we need to delete.
                    response = supabase.table("messages").select("*") \
                        .eq("managed_account_id", account_id) \
                        .eq("marked_for_deletion", True) \
                        .not_.is_("tg_message_id", None) \
                        .eq("deleted_by_worker", False) \
                        .execute()

                    if response.data:
                        for msg in response.data:
                            print(f"[{username}][Deletion] Attempting to delete message: db_id={msg.get('id')}")
                            try:
                                entity = await client.get_entity(int(msg['chat_id']))
                                await client.delete_messages(entity, msg['tg_message_id'])
                                print(f"[{username}][Deletion] Successfully deleted TG message.")
                                supabase.table("messages").update({"deleted_by_worker": True}).eq("id", msg["id"]).execute()
                                print(f"[{username}][Deletion] Marked as deleted_by_worker in DB.")
                            except Exception as e:
                                print(f"[{username}][Deletion][Error] Failed to delete message {msg['tg_message_id']}: {e}")
            
            except Exception as e:
                print(f"[{username}][Deletion][Error] Polling for deletion failed: {e}")
            
            # Wait 5 seconds before the next check, whether work was found or not.
            await asyncio.sleep(5)

    poller_task = asyncio.create_task(poll_for_outgoing())
    delete_task = asyncio.create_task(delete_marked_messages())
    await client.run_until_disconnected()
    poller_task.cancel()
    delete_task.cancel()

ACTIVE_WORKERS: Dict[str, asyncio.Task] = {}

async def main():
    """
    A dynamic worker manager that periodically checks the database and
    starts or stops worker tasks as needed.
    """
    print("🚀 Starting TeleMind Dynamic Worker Manager...")

    try:
        while True:
            try:
                print("Manager: Checking for account updates...")
                # 1. Fetch all 'connected' accounts from the database
                response = supabase.table("managed_telegram_accounts").select("id, session_string, username").eq("status", "connected").execute()
                live_accounts = {acc['id']: acc for acc in (response.data or [])}
                
                # 2. Compare live accounts with running tasks
                live_ids = set(live_accounts.keys())
                running_ids = set(ACTIVE_WORKERS.keys())
                
                # 3. Stop workers for accounts that are no longer 'connected'
                for account_id in running_ids - live_ids:
                    print(f"Stopping worker for disconnected account: {account_id}")
                    ACTIVE_WORKERS.pop(account_id).cancel()

                # 4. Start workers for new 'connected' accounts
                for account_id in live_ids - running_ids:
                    account = live_accounts[account_id]
                    print(f"Starting new worker for account: {account.get('username', account_id)}")
                    ACTIVE_WORKERS[account_id] = asyncio.create_task(account_worker(account))

            except Exception as e:
                print(f"FATAL error in manager loop: {e}. Retrying in 60 seconds.")
            
            # --- Clean up completed/cancelled tasks ---
            for acc_id in list(ACTIVE_WORKERS.keys()):
                if ACTIVE_WORKERS[acc_id].done():
                    print(f"Worker for {acc_id} has finished. Removing from active list.")
                    try:
                        # Check task result to log potential exceptions
                        ACTIVE_WORKERS[acc_id].result()
                    except asyncio.CancelledError:
                        pass # Task was cancelled as expected.
                    except Exception as e:
                        print(f"Worker for {acc_id} crashed with an exception: {e}")
                    del ACTIVE_WORKERS[acc_id]
            
            print(f"Manager: {len(ACTIVE_WORKERS)} workers running. Next check in 60 seconds.")
            await asyncio.sleep(60)

    except asyncio.CancelledError:
        # This is expected when Ctrl+C is pressed.
        # We can just let it pass and proceed to the finally block.
        pass
        
    finally:
        print("\nShutting down workers...")
        tasks_to_cancel = list(ACTIVE_WORKERS.values())
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            
            # Wait for all tasks to be cancelled
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            print("All workers have been shut down.")
        else:
            print("No active workers to shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting.")

    