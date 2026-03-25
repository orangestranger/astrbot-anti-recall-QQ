import asyncio
import json
import os
import shutil
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

from astrbot.api import logger
from astrbot.api import message_components as Comp
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.platform import MessageType
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType


async def delayed_delete(delay: int, path: Path):
    await asyncio.sleep(delay)
    try:
        path.unlink(missing_ok=True)
        logger.debug(f"[AntiRevoke] Deleted expired cache file: {path.name}")
    except Exception:
        logger.error(f"[AntiRevoke] Failed to delete cache file ({path}): {traceback.format_exc()}")


async def _cleanup_local_files(file_paths: List[str]):
    if not file_paths:
        return
    await asyncio.sleep(1)
    for abs_path in file_paths:
        try:
            os.remove(abs_path)
            logger.debug(f"[AntiRevoke] Cleaned up local file: {os.path.basename(abs_path)}")
        except Exception as e:
            logger.error(f"[AntiRevoke] Failed to clean up local file ({abs_path}): {e}")


def get_value(obj, key, default=None):
    try:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)
    except Exception:
        return default


def _extract_segment_data(raw_message: dict, segment_type: str) -> List[Dict]:
    message_list = raw_message.get("message", []) if isinstance(raw_message, dict) else []
    if not isinstance(message_list, list):
        return []
    result = []
    for segment in message_list:
        if isinstance(segment, dict) and segment.get("type") == segment_type:
            result.append(segment.get("data", {}) or {})
    return result


def _find_forward_segment(raw_message: dict) -> Optional[Dict]:
    for data in _extract_segment_data(raw_message, "forward"):
        if data:
            return data
    return None


def _normalize_forward_messages(messages) -> List[Dict]:
    normalized = []
    if not isinstance(messages, list):
        return normalized

    for item in messages:
        if not isinstance(item, dict):
            continue

        if item.get("type") == "node" and isinstance(item.get("data"), dict):
            normalized.append(item)
            continue

        sender = item.get("sender", {}) or {}
        content = item.get("content") or item.get("message") or []

        if isinstance(content, str):
            content = [{"type": "text", "data": {"text": content}}]

        if not isinstance(content, list):
            continue

        user_id = sender.get("user_id") or item.get("user_id") or item.get("uin") or "0"
        nickname = (
            sender.get("nickname")
            or sender.get("card")
            or item.get("nickname")
            or item.get("name")
            or str(user_id)
        )

        normalized.append(
            {
                "type": "node",
                "data": {
                    "user_id": str(user_id),
                    "nickname": str(nickname),
                    "content": content,
                },
            }
        )

    return normalized


def _serialize_components(components: list) -> List[Dict]:
    serialized_list = []
    for comp in components:
        try:
            comp_dict = {k: v for k, v in comp.__dict__.items() if not k.startswith("_")}
            comp_type_name = getattr(comp.type, "name", "unknown")
            comp_dict["type"] = comp_type_name
            serialized_list.append(comp_dict)
        except Exception:
            serialized_list.append({"type": "Unknown", "data": f"<{str(comp)}>"})
    return serialized_list


def _deserialize_components(comp_dicts: List[Dict]) -> List:
    components = []
    component_map = {
        "Plain": Comp.Plain,
        "Text": Comp.Plain,
        "Image": Comp.Image,
        "Face": Comp.Face,
        "At": Comp.At,
        "Video": Comp.Video,
        "Record": Comp.Record,
        "File": Comp.File,
        "Json": Comp.Json,
    }

    for comp_dict in comp_dicts:
        data_to_construct = comp_dict.copy()
        comp_type_name = data_to_construct.pop("type", None)
        if not comp_type_name:
            logger.warning("[AntiRevoke] Encountered component data without type during deserialization")
            continue

        cls = component_map.get(comp_type_name)
        if cls:
            try:
                if "file_" in data_to_construct:
                    data_to_construct["file"] = data_to_construct.pop("file_")
                components.append(cls(**data_to_construct))
            except Exception as e:
                logger.error(f"[AntiRevoke] Failed to deserialize component {comp_type_name}: {e}")
        elif comp_type_name != "Forward":
            logger.warning(f"[AntiRevoke] Unknown component type during deserialization: {comp_type_name}")

    return components


async def _download_binary_file(
    session: aiohttp.ClientSession, url: str, save_path: Path, timeout: int = 60
) -> bool:
    try:
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://qzone.qq.com/"}
        async with session.get(url, headers=headers, timeout=timeout) as response:
            response.raise_for_status()
            with open(save_path, "wb") as f:
                f.write(await response.read())
        return True
    except Exception as e:
        logger.error(f"[AntiRevoke] Failed to download file ({url}): {e}")
        if save_path.exists():
            save_path.unlink(missing_ok=True)
        return False


async def _download_and_cache_image(
    session: aiohttp.ClientSession, component: Comp.Image, temp_path: Path
) -> Optional[str]:
    image_url = getattr(component, "url", None)
    if not image_url:
        return None

    file_extension = ".jpg"
    if str(image_url).lower().endswith(".png"):
        file_extension = ".png"

    file_name = f"forward_{int(time.time() * 1000)}{file_extension}"
    temp_file_path = temp_path / file_name

    try:
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://qzone.qq.com/"}
        async with session.get(image_url, headers=headers, timeout=15) as response:
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            if "image" not in content_type and "octet-stream" not in content_type:
                logger.warning(f"[AntiRevoke] URL did not return an image content type: {content_type}")
                return None
            with open(temp_file_path, "wb") as f:
                f.write(await response.read())
        return str(temp_file_path.absolute())
    except Exception as e:
        logger.error(f"[AntiRevoke] Failed to cache image ({image_url}): {e}")
        if temp_file_path.exists():
            temp_file_path.unlink(missing_ok=True)
        return None


async def _download_record_to_cache(
    session: aiohttp.ClientSession, source_url: str, save_dir: Path, timestamp_ms: int
) -> Optional[str]:
    suffix = Path(source_url.split("?")[0]).suffix or ".amr"
    save_path = save_dir / f"{timestamp_ms}{suffix}"
    ok = await _download_binary_file(session, source_url, save_path, timeout=120)
    if not ok:
        return None
    try:
        os.chmod(save_path, 0o644)
    except Exception:
        pass
    return str(save_path.absolute())


async def _process_component_and_get_gocq_part(
    comp,
    session: aiohttp.ClientSession,
    temp_path: Path,
    local_files_to_cleanup: List[str],
    local_file_map: Dict = None,
) -> List[Dict]:
    gocq_parts = []
    comp_type_name = getattr(comp.type, "name", "unknown")

    if comp_type_name in ["Plain", "Text"]:
        text = getattr(comp, "text", "")
        if text:
            gocq_parts.append({"type": "text", "data": {"text": text}})
    elif comp_type_name == "Face":
        face_id = getattr(comp, "id", None)
        if face_id is not None:
            gocq_parts.append({"type": "face", "data": {"id": int(face_id)}})
    elif comp_type_name == "At":
        qq = getattr(comp, "qq", "unknown")
        name = getattr(comp, "name", f"@{{{qq}}}")
        gocq_parts.append({"type": "text", "data": {"text": f"@{name}({qq})"}})
    elif comp_type_name == "Image":
        local_path = await _download_and_cache_image(session, comp, temp_path)
        if local_path:
            local_files_to_cleanup.append(local_path)
            gocq_parts.append({"type": "image", "data": {"file": local_path}})
        else:
            image_url = getattr(comp, "url", None)
            if image_url:
                gocq_parts.append({"type": "image", "data": {"file": image_url}})
            else:
                gocq_parts.append({"type": "text", "data": {"text": "[image restore failed]"}})
    elif comp_type_name == "Video":
        cached_video_path_str = getattr(comp, "file", None)
        if cached_video_path_str and cached_video_path_str.startswith("[video too large"):
            gocq_parts.append({"type": "text", "data": {"text": cached_video_path_str}})
        elif cached_video_path_str and Path(cached_video_path_str).exists():
            absolute_path = str(Path(cached_video_path_str).absolute())
            gocq_parts.append({"type": "video", "data": {"file": f"file:///{absolute_path}"}})
        else:
            gocq_parts.append({"type": "text", "data": {"text": "[video cache missing]"}})
    elif comp_type_name == "Record":
        cached_voice_path_str = getattr(comp, "file", None)
        cached_voice_url = getattr(comp, "url", None)
        if cached_voice_path_str and Path(cached_voice_path_str).exists():
            absolute_path = str(Path(cached_voice_path_str).absolute())
            gocq_parts.append({"type": "record", "data": {"file": f"file:///{absolute_path}"}})
        elif cached_voice_url and str(cached_voice_url).startswith(("http://", "https://")):
            gocq_parts.append({"type": "record", "data": {"file": cached_voice_url}})
        else:
            gocq_parts.append({"type": "text", "data": {"text": "[voice restore failed]"}})
    elif comp_type_name == "File":
        unique_key = getattr(comp, "url", None)
        cached_file_path_str = local_file_map.get(unique_key) if local_file_map and unique_key else None
        if cached_file_path_str and Path(cached_file_path_str).exists():
            absolute_path = str(Path(cached_file_path_str).absolute())
            original_filename = Path(cached_file_path_str).name.split("_", 1)[-1]
            gocq_parts.append(
                {"type": "file", "data": {"file": f"file:///{absolute_path}", "name": original_filename}}
            )
        else:
            gocq_parts.append({"type": "text", "data": {"text": "[file cache missing]"}})
    elif comp_type_name == "Forward":
        forward_text = getattr(comp, "data", None) or "[forward message]"
        if isinstance(forward_text, dict):
            forward_text = json.dumps(forward_text, ensure_ascii=False)
        gocq_parts.append({"type": "text", "data": {"text": str(forward_text)}})
    elif comp_type_name == "Json":
        json_data = getattr(comp, "data", "{}")
        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)
        try:
            json.loads(json_data)
            gocq_parts.append({"type": "json", "data": {"data": json_data}})
        except Exception:
            gocq_parts.append({"type": "text", "data": {"text": "[json restore failed]"}})

    return gocq_parts


@register(
    "astrbot_plugin_anti_revoke",
    "orangestranger",
    "QQ anti-recall plugin with image, voice, and forward support",
    "1.2.1",
    "https://github.com/orangestranger/astrbot-anti-recall-QQ",
)
class AntiRevoke(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        config = config or {}
        self.monitor_groups = [str(g) for g in config.get("monitor_groups", []) or []]
        self.target_receivers = [str(r) for r in config.get("target_receivers", []) or []]
        self.target_groups = [str(g) for g in config.get("target_groups", []) or []]
        self.ignore_senders = [str(s) for s in config.get("ignore_senders", []) or []]
        self.instance_id = "AntiRevoke"
        self.cache_expiration_time = int(config.get("cache_expiration_time", 300))
        self.file_size_threshold_mb = int(config.get("file_size_threshold_mb", 300))
        self.context = context

        self.temp_path = Path(StarTools.get_data_dir("astrbot_plugin_anti_revoke"))
        self.temp_path.mkdir(exist_ok=True)
        self.video_cache_path = self.temp_path / "videos"
        self.video_cache_path.mkdir(exist_ok=True)
        self.voice_cache_path = self.temp_path / "voices"
        self.voice_cache_path.mkdir(exist_ok=True)
        self.file_cache_path = self.temp_path / "files"
        self.file_cache_path.mkdir(exist_ok=True)
        self._cleanup_cache_on_startup()

    def _cleanup_cache_on_startup(self):
        now = time.time()
        expired_count = 0
        for cache_dir in [self.video_cache_path, self.voice_cache_path, self.file_cache_path, self.temp_path]:
            for file in cache_dir.glob("*"):
                if file.is_dir():
                    continue
                try:
                    if now - file.stat().st_mtime > self.cache_expiration_time:
                        file.unlink(missing_ok=True)
                        expired_count += 1
                except Exception:
                    continue
        logger.info(f"[{self.instance_id}] Cache cleanup finished, removed {expired_count} expired files")

    def _build_targets(self):
        return [("private", tid) for tid in self.target_receivers] + [("group", tid) for tid in self.target_groups]

    async def _resolve_group_user_info(self, client, group_id: str, sender_id: str, operator_id: str):
        group_name = str(group_id)
        member_nickname = str(sender_id)
        operator_nickname = str(operator_id)

        try:
            group_info = await client.api.call_action("get_group_info", group_id=int(group_id))
            group_name = group_info.get("group_name", group_name)
        except Exception:
            pass

        try:
            member_info = await client.api.call_action(
                "get_group_member_info", group_id=int(group_id), user_id=int(sender_id)
            )
            member_nickname = member_info.get("card") or member_info.get("nickname") or member_nickname
        except Exception:
            pass

        try:
            operator_info = await client.api.call_action(
                "get_group_member_info", group_id=int(group_id), user_id=int(operator_id)
            )
            operator_nickname = operator_info.get("card") or operator_info.get("nickname") or operator_nickname
        except Exception:
            pass

        return group_name, member_nickname, operator_nickname

    def _create_recall_notification_header(
        self,
        group_name: str,
        group_id: str,
        member_nickname: str,
        sender_id: str,
        operator_nickname: str,
        operator_id: str,
        timestamp: int,
    ) -> str:
        message_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp)) if timestamp else "unknown"
        if operator_id == sender_id:
            return (
                f"[Recall Alert]\nGroup: {group_name} ({group_id})\n"
                f"Sender: {member_nickname} ({sender_id})\nTime: {message_time_str}"
            )
        return (
            f"[Recall Alert]\nGroup: {group_name} ({group_id})\nSender: {member_nickname} ({sender_id})\n"
            f"Operator: {operator_nickname} ({operator_id})\nTime: {message_time_str}"
        )

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=20)
    async def handle_message_cache(self, event: AstrMessageEvent):
        group_id = str(event.get_group_id())
        message_id = str(event.message_obj.message_id)
        if event.get_message_type() != MessageType.GROUP_MESSAGE or group_id not in self.monitor_groups:
            return None

        try:
            raw_message = event.message_obj.raw_message
            if not isinstance(raw_message, dict):
                raw_message = {}

            forward_segment_data = _find_forward_segment(raw_message)
            forward_data = None
            if forward_segment_data:
                try:
                    client = event.bot
                    forward_id = (
                        forward_segment_data.get("id")
                        or forward_segment_data.get("res_id")
                        or forward_segment_data.get("message_id")
                    )
                    if forward_id:
                        forward_result = await client.api.call_action("get_forward_msg", message_id=str(forward_id))
                        raw_messages = []
                        if isinstance(forward_result, dict):
                            raw_messages = (
                                forward_result.get("messages")
                                or forward_result.get("data", {}).get("messages")
                                or forward_result.get("data")
                                or []
                            )
                        messages = _normalize_forward_messages(raw_messages)
                        if messages:
                            forward_data = {"forward_id": str(forward_id), "messages": messages}
                        else:
                            logger.warning(
                                f"[{self.instance_id}] get_forward_msg returned empty or unsupported data: {forward_result}"
                            )
                except Exception as e:
                    logger.warning(f"[{self.instance_id}] Failed to cache forward nodes directly: {e}")

            message_obj = event.get_messages()
            timestamp_ms = int(time.time() * 1000)
            components = (
                message_obj.components
                if isinstance(message_obj, MessageChain)
                else message_obj
                if isinstance(message_obj, list)
                else []
            )
            components = [comp for comp in components if getattr(comp.type, "name", "unknown") != "Reply"]
            if not components and not forward_data:
                return None

            raw_file_names = []
            raw_file_sizes = {}
            raw_video_sizes = {}
            message_list = raw_message.get("message", [])
            if isinstance(message_list, list):
                for segment in message_list:
                    if not isinstance(segment, dict):
                        continue
                    if segment.get("type") == "file":
                        file_name = segment.get("data", {}).get("file")
                        file_size = segment.get("data", {}).get("file_size")
                        if file_name:
                            raw_file_names.append(file_name)
                        if file_size:
                            try:
                                raw_file_sizes[file_name] = int(file_size) if isinstance(file_size, str) else file_size
                            except ValueError:
                                pass
                    elif segment.get("type") == "video":
                        file_id = segment.get("data", {}).get("file")
                        file_size = segment.get("data", {}).get("file_size")
                        if file_id and file_size:
                            try:
                                raw_video_sizes[file_id] = int(file_size) if isinstance(file_size, str) else file_size
                            except ValueError:
                                pass

            local_file_map = {}
            has_downloadable_content = any(
                getattr(comp.type, "name", "") in ["Video", "Record", "File"] for comp in components
            )

            if has_downloadable_content:
                client = event.bot
                for comp in components:
                    comp_type_name = getattr(comp.type, "name", "unknown")

                    if comp_type_name == "Video":
                        file_id = getattr(comp, "file", None)
                        if not file_id:
                            continue
                        video_size = raw_video_sizes.get(file_id)
                        if video_size and self.file_size_threshold_mb > 0:
                            video_size_mb = video_size / (1024 * 1024)
                            if video_size_mb > self.file_size_threshold_mb:
                                setattr(comp, "file", f"[video too large to cache: {video_size_mb:.2f} MB]")
                                continue
                        try:
                            ret = await client.api.call_action("get_file", **{"file_id": file_id})
                            download_url = ret.get("url")
                            if not download_url:
                                setattr(comp, "file", "Error: no video URL")
                                continue
                            original_filename = getattr(comp, "name", file_id.split("/")[-1]) or f"{timestamp_ms}.mp4"
                            dest_path = self.video_cache_path / f"{timestamp_ms}_{original_filename}"
                            async with aiohttp.ClientSession() as session:
                                ok = await _download_binary_file(session, download_url, dest_path, timeout=120)
                            if ok:
                                setattr(comp, "file", str(dest_path.absolute()))
                                asyncio.create_task(delayed_delete(self.cache_expiration_time, dest_path))
                            else:
                                setattr(comp, "file", "Error: video download failed")
                        except Exception as e:
                            logger.error(f"[{self.instance_id}] Failed to cache video: {e}\n{traceback.format_exc()}")
                            setattr(comp, "file", "Error: video cache exception")

                    elif comp_type_name == "Record":
                        file_id = getattr(comp, "file", None)
                        voice_url = getattr(comp, "url", None)
                        raw_record_segments = _extract_segment_data(raw_message, "record")
                        if not voice_url and raw_record_segments:
                            voice_url = raw_record_segments[0].get("url")
                        try:
                            cached_path = None
                            if voice_url and str(voice_url).startswith(("http://", "https://")):
                                async with aiohttp.ClientSession() as session:
                                    cached_path = await _download_record_to_cache(
                                        session, voice_url, self.voice_cache_path, timestamp_ms
                                    )
                            if not cached_path and file_id:
                                ret = await client.api.call_action("get_file", **{"file_id": file_id})
                                local_path = ret.get("file")
                                if local_path and os.path.exists(local_path):
                                    original_suffix = Path(local_path).suffix or ".amr"
                                    permanent_path = self.voice_cache_path / f"{timestamp_ms}{original_suffix}"
                                    shutil.copy(local_path, permanent_path)
                                    os.chmod(permanent_path, 0o644)
                                    cached_path = str(permanent_path.absolute())
                            if cached_path:
                                setattr(comp, "file", cached_path)
                                if voice_url:
                                    setattr(comp, "url", voice_url)
                                asyncio.create_task(delayed_delete(self.cache_expiration_time, Path(cached_path)))
                            else:
                                if voice_url:
                                    setattr(comp, "url", voice_url)
                                setattr(comp, "file", "Error: voice cache failed")
                        except Exception as e:
                            logger.error(f"[{self.instance_id}] Failed to cache record: {e}\n{traceback.format_exc()}")
                            if voice_url:
                                setattr(comp, "url", voice_url)
                            setattr(comp, "file", "Error: voice cache exception")

                    elif comp_type_name == "File":
                        try:
                            original_filename = raw_file_names[0] if raw_file_names else None
                            file_size = raw_file_sizes.get(original_filename) if original_filename else None
                            if file_size and self.file_size_threshold_mb > 0:
                                file_size_mb = file_size / (1024 * 1024)
                                if file_size_mb > self.file_size_threshold_mb:
                                    unique_key = getattr(comp, "url", None)
                                    if unique_key:
                                        local_file_map[unique_key] = f"[file too large to cache: {file_size_mb:.2f} MB]"
                                    if raw_file_names:
                                        raw_file_names.pop(0)
                                    continue
                            temp_file_path = await comp.get_file()
                            if not temp_file_path or not os.path.exists(temp_file_path):
                                continue
                            if not original_filename and raw_file_names:
                                original_filename = raw_file_names.pop(0)
                            if not original_filename:
                                original_filename = getattr(comp, "name", Path(temp_file_path).name)
                            permanent_path = self.file_cache_path / f"{timestamp_ms}_{original_filename}"
                            shutil.copy(temp_file_path, permanent_path)
                            unique_key = getattr(comp, "url", None)
                            if unique_key:
                                local_file_map[unique_key] = str(permanent_path)
                                asyncio.create_task(delayed_delete(self.cache_expiration_time, permanent_path))
                        except Exception as e:
                            logger.error(f"[{self.instance_id}] Failed to cache file: {e}\n{traceback.format_exc()}")

            file_path = self.temp_path / f"{timestamp_ms}_{group_id}_{message_id}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                data_to_save = {
                    "components": _serialize_components(components),
                    "sender_id": event.get_sender_id(),
                    "timestamp": event.message_obj.timestamp,
                    "local_file_map": local_file_map,
                    "forward_data": forward_data,
                }
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)

            asyncio.create_task(delayed_delete(self.cache_expiration_time, file_path))
        except Exception as e:
            logger.error(f"[{self.instance_id}] Failed to cache message (ID: {message_id}): {e}\n{traceback.format_exc()}")
        return None

    @filter.platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=10)
    async def handle_recall_event(self, event: AstrMessageEvent):
        raw_message = event.message_obj.raw_message
        post_type = get_value(raw_message, "post_type")
        if post_type != "notice" or get_value(raw_message, "notice_type") != "group_recall":
            return None

        group_id = str(get_value(raw_message, "group_id"))
        message_id = str(get_value(raw_message, "message_id"))
        operator_id = str(get_value(raw_message, "operator_id"))
        if group_id not in self.monitor_groups or not message_id:
            return None

        file_path = next(self.temp_path.glob(f"*_{group_id}_{message_id}.json"), None)
        cached_data = None
        if file_path and file_path.exists():
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
            except Exception as e:
                logger.warning(f"[{self.instance_id}] Failed to read cache file: {e}")

        if not cached_data:
            logger.warning(f"[{self.instance_id}] Message cache not found (ID: {message_id})")
            return None

        local_files_to_cleanup = []
        try:
            sender_id = str(cached_data["sender_id"])
            if sender_id in self.ignore_senders:
                return None

            timestamp = cached_data.get("timestamp")
            client = event.bot
            group_name, member_nickname, operator_nickname = await self._resolve_group_user_info(
                client, group_id, sender_id, operator_id
            )

            forward_data = cached_data.get("forward_data")
            if forward_data and forward_data.get("messages"):
                targets = self._build_targets()
                forward_messages = forward_data.get("messages", [])
                logger.info(
                    f"[{self.instance_id}] Sending cached forward message, nodes={len(forward_messages)}, "
                    f"first={json.dumps(forward_messages[:1], ensure_ascii=False)}"
                )
                for target_type, target_id in targets:
                    target_id_str = str(target_id)
                    header = self._create_recall_notification_header(
                        group_name,
                        group_id,
                        member_nickname,
                        sender_id,
                        operator_nickname,
                        operator_id,
                        timestamp,
                    )
                    notification_text = f"{header}\n--------------------\nRecalled forward message:"
                    try:
                        if target_type == "private":
                            await client.send_private_msg(user_id=int(target_id_str), message=notification_text)
                            await asyncio.sleep(0.5)
                            await client.api.call_action(
                                "send_private_forward_msg",
                                user_id=int(target_id_str),
                                messages=forward_messages,
                            )
                        else:
                            await client.send_group_msg(group_id=int(target_id_str), message=notification_text)
                            await asyncio.sleep(0.5)
                            await client.api.call_action(
                                "send_group_forward_msg",
                                group_id=int(target_id_str),
                                messages=forward_messages,
                            )
                    except Exception as e:
                        logger.error(f"[{self.instance_id}] Failed to send cached forward message to {target_type} {target_id_str}: {e}")
                return None

            local_file_map = cached_data.get("local_file_map", {})
            cached_components_data = cached_data.get("components", [])
            components = _deserialize_components(cached_components_data)

            special_components = [
                comp for comp in components if getattr(comp.type, "name", "unknown") in ["Video", "Record", "Json", "File", "Forward"]
            ]
            other_components = [
                comp for comp in components if getattr(comp.type, "name", "unknown") not in ["Video", "Record", "Json", "File", "Forward"]
            ]

            async with aiohttp.ClientSession() as session:
                for target_type, target_id in self._build_targets():
                    target_id_str = str(target_id)
                    notification_prefix = self._create_recall_notification_header(
                        group_name, group_id, member_nickname, sender_id, operator_nickname, operator_id, timestamp
                    )

                    if not special_components:
                        message_parts = []
                        for comp in other_components:
                            message_parts.extend(
                                await _process_component_and_get_gocq_part(
                                    comp, session, self.temp_path, local_files_to_cleanup, local_file_map
                                )
                            )
                        final_prefix_text = f"{notification_prefix}\n--------------------\n"
                        gocq_content_array = [{"type": "text", "data": {"text": final_prefix_text}}]
                        gocq_content_array.extend(message_parts)
                        if target_type == "private":
                            await client.send_private_msg(user_id=int(target_id_str), message=gocq_content_array)
                        else:
                            await client.send_group_msg(group_id=int(target_id_str), message=gocq_content_array)
                    else:
                        final_notification_text = f"{notification_prefix}\n--------------------\nContent follows in separate messages."
                        if target_type == "private":
                            await client.send_private_msg(user_id=int(target_id_str), message=final_notification_text)
                        else:
                            await client.send_group_msg(group_id=int(target_id_str), message=final_notification_text)
                        await asyncio.sleep(0.5)

                        if other_components:
                            message_parts = []
                            for comp in other_components:
                                message_parts.extend(
                                    await _process_component_and_get_gocq_part(
                                        comp, session, self.temp_path, local_files_to_cleanup, local_file_map
                                    )
                                )
                            if message_parts:
                                if target_type == "private":
                                    await client.send_private_msg(user_id=int(target_id_str), message=message_parts)
                                else:
                                    await client.send_group_msg(group_id=int(target_id_str), message=message_parts)

                        for comp in special_components:
                            await asyncio.sleep(0.5)
                            parts = await _process_component_and_get_gocq_part(
                                comp, session, self.temp_path, local_files_to_cleanup, local_file_map
                            )
                            if target_type == "private":
                                await client.send_private_msg(user_id=int(target_id_str), message=parts)
                            else:
                                await client.send_group_msg(group_id=int(target_id_str), message=parts)
        finally:
            if local_files_to_cleanup:
                asyncio.create_task(_cleanup_local_files(local_files_to_cleanup))
            if file_path:
                asyncio.create_task(delayed_delete(0, file_path))
        return None
