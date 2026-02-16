"""Mastodon channel implementation using Mastodon.py (Fediverse DMs)."""

from __future__ import annotations

import asyncio
import re
from html import unescape
from typing import Any

from loguru import logger
from mastodon import Mastodon

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import MastodonConfig


def _strip_html(html: str) -> str:
    """Remove HTML tags and decode entities from Mastodon status content."""
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", "", html)
    return unescape(text).strip()


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Get attribute from Mastodon API response (dict or object)."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _make_session_with_proxy(proxy: str | None):
    """Return a requests.Session with proxy set, or None."""
    if not proxy or not proxy.strip():
        return None
    try:
        import requests
        session = requests.Session()
        session.proxies = {"http": proxy, "https": proxy}
        return session
    except Exception as e:
        logger.warning(f"Mastodon: could not create proxy session: {e}")
        return None


class MastodonChannel(BaseChannel):
    """
    Mastodon channel using DMs (conversations) via polling.
    Talks to the bot over the Fediverse by direct message.
    """

    name = "mastodon"

    def __init__(self, config: MastodonConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: MastodonConfig = config
        self._client: Mastodon | None = None
        self._me_id: str | None = None
        self._reply_to_id: dict[str, str] = {}  # chat_id (conversation id) -> status id to reply to
        self._seen_status_ids: set[str] = set()
        self._first_poll_done: bool = False  # skip dispatching on first poll (seed seen set with history)
        self._poll_task: asyncio.Task | None = None

    def _client_sync(self):
        """Create Mastodon client (sync); use from thread."""
        base = (self.config.base_url or "https://mastodon.social").rstrip("/")
        session = _make_session_with_proxy(self.config.proxy)
        return Mastodon(
            access_token=self.config.token,
            api_base_url=base,
            session=session,
        )

    async def start(self) -> None:
        """Start polling for Mastodon DMs."""
        if not self.config.token:
            logger.error("Mastodon access token not configured")
            return

        self._running = True
        try:
            self._client = await asyncio.to_thread(self._client_sync)
            me = await asyncio.to_thread(self._client.me)
            self._me_id = str(_get(me, "id", ""))
            if not self._me_id:
                logger.error("Mastodon me() did not return an id")
                self._running = False
                return
            logger.info(f"Mastodon channel connected as {_get(me, 'username', '?')}@{self.config.base_url or 'mastodon.social'}")
        except Exception as e:
            logger.error(f"Mastodon failed to connect: {e}")
            self._running = False
            return

        self._poll_task = asyncio.create_task(self._poll_loop())

        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """Stop the channel and polling."""
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._client = None
        self._me_id = None
        self._reply_to_id.clear()
        self._seen_status_ids.clear()
        self._first_poll_done = False

    async def send(self, msg: OutboundMessage) -> None:
        """Send a DM reply in the conversation identified by chat_id."""
        if not self._client:
            logger.warning("Mastodon client not running")
            return

        in_reply_to_id = self._reply_to_id.get(msg.chat_id) or (msg.metadata or {}).get("in_reply_to_id")
        if not in_reply_to_id:
            logger.warning(f"Mastodon: no reply target for chat_id={msg.chat_id}, skipping send")
            return

        try:
            await asyncio.to_thread(
                self._client.status_post,
                msg.content,
                in_reply_to_id=in_reply_to_id,
                visibility="direct",
            )
        except Exception as e:
            logger.error(f"Mastodon send error: {e}")

    async def _poll_loop(self) -> None:
        """Poll conversations, home timeline, and notifications for DMs."""
        interval = max(10, self.config.poll_interval_seconds)
        poll_count = 0
        logger.info(f"Mastodon poll loop started (interval={interval}s)")
        while self._running and self._client:
            try:
                poll_count += 1
                seed_only = not self._first_poll_done
                conv_n, home_n, home_direct, notif_n, notif_direct = await self._poll_once(seed_only=seed_only)
                if seed_only:
                    self._first_poll_done = True
                    logger.info("Mastodon: first poll done, seeded seen set (only new DMs will be processed from now)")
                if poll_count <= 5 or poll_count % 10 == 0 or (home_n + notif_n) > 0:
                    logger.info(
                        f"Mastodon poll #{poll_count}: home={home_n} (direct={home_direct}) notifs={notif_n} (direct={notif_direct})"
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Mastodon poll error: {e}", exc_info=True)
            await asyncio.sleep(interval)

    async def _poll_once(self, seed_only: bool = False) -> tuple[int, int, int, int, int]:
        """Run one poll cycle; return (convs, home_count, home_direct, notif_count, notif_direct).
        When seed_only=True, only add status ids to _seen_status_ids (no dispatch) so past DMs are ignored."""
        conv_n = 0
        home_n = 0
        home_direct = 0
        notif_n = 0
        notif_direct = 0

        if not self._client or not self._me_id:
            return (conv_n, home_n, home_direct, notif_n, notif_direct)

        try:
            convs = await asyncio.to_thread(self._client.conversations, limit=40)
            conv_n = len(convs or [])
        except Exception as e:
            logger.debug(f"Mastodon conversations: {e}")
            convs = []
        for conv in convs or []:
            if not self._running:
                break
            last = _get(conv, "last_status")
            if not last:
                continue
            account = _get(last, "account")
            if not account:
                continue
            author_id = str(_get(account, "id", ""))
            if author_id == self._me_id:
                continue
            acct = str(_get(account, "acct", author_id))
            conv_id = str(_get(conv, "id", ""))
            if not conv_id:
                continue
            if seed_only:
                sid = str(_get(last, "id", ""))
                if sid:
                    self._seen_status_ids.add(sid)
                    self._reply_to_id[conv_id] = sid
            else:
                await self._process_incoming_dm(status=last, chat_id=conv_id, author_id=author_id, acct=acct)
            try:
                await asyncio.to_thread(self._client.conversations_read, conv_id)
            except Exception:
                pass

        try:
            statuses = await asyncio.to_thread(self._client.timeline_home, limit=40)
            statuses = statuses or []
            home_n = len(statuses)
        except Exception as e:
            logger.warning(f"Mastodon timeline_home: {e}")
            statuses = []
        for status in statuses:
            if not self._running:
                break
            visibility = (_get(status, "visibility", "") or "").lower()
            if visibility != "direct":
                continue
            home_direct += 1
            account = _get(status, "account")
            if not account:
                continue
            author_id = str(_get(account, "id", ""))
            acct = str(_get(account, "acct", author_id))
            if author_id == self._me_id:
                continue
            chat_id = f"dm:{author_id}"
            if seed_only:
                sid = str(_get(status, "id", ""))
                if sid:
                    self._seen_status_ids.add(sid)
                    self._reply_to_id[chat_id] = sid
            else:
                await self._process_incoming_dm(status=status, chat_id=chat_id, author_id=author_id, acct=acct)

        try:
            notifs = await asyncio.to_thread(self._client.notifications, limit=40)
            notifs = notifs or []
            notif_n = len(notifs)
        except Exception as e:
            logger.warning(f"Mastodon notifications: {e}")
            return (conv_n, home_n, home_direct, notif_n, notif_direct)
        for n in notifs:
            if not self._running:
                break
            if _get(n, "type") != "mention":
                continue
            status = _get(n, "status")
            if not status:
                continue
            visibility = (_get(status, "visibility", "") or "").lower()
            if visibility != "direct":
                continue
            notif_direct += 1
            account = _get(status, "account")
            if not account:
                continue
            author_id = str(_get(account, "id", ""))
            acct = str(_get(account, "acct", author_id))
            if author_id == self._me_id:
                continue
            chat_id = f"dm:{author_id}"
            if seed_only:
                sid = str(_get(status, "id", ""))
                if sid:
                    self._seen_status_ids.add(sid)
                    self._reply_to_id[chat_id] = sid
            else:
                await self._process_incoming_dm(status=status, chat_id=chat_id, author_id=author_id, acct=acct)

        return (conv_n, home_n, home_direct, notif_n, notif_direct)

    async def _process_incoming_dm(
        self,
        *,
        status: Any,
        chat_id: str,
        author_id: str,
        acct: str,
    ) -> None:
        """Process one incoming DM (shared by conversations and notifications)."""
        status_id = str(_get(status, "id", ""))
        if not status_id or status_id in self._seen_status_ids:
            return
        self._seen_status_ids.add(status_id)
        if len(self._seen_status_ids) > 500:
            self._seen_status_ids.clear()

        content_html = _get(status, "content", "") or ""
        content = _strip_html(content_html).strip() or "[empty]"
        sender_id = f"{author_id}|{acct}"
        self._reply_to_id[chat_id] = status_id

        logger.info(f"Mastodon DM from {acct}: {content[:80]}{'...' if len(content) > 80 else ''}")
        await self._handle_message(
            sender_id=sender_id,
            chat_id=chat_id,
            content=content,
            metadata={
                "conversation_id": chat_id,
                "status_id": status_id,
                "acct": acct,
                "in_reply_to_id": status_id,
            },
        )

