from __future__ import annotations

import datetime
import html
import logging
import re
from typing import TYPE_CHECKING

from telethon.events import NewMessage, StopPropagation

from fa_search_bot.functionalities.functionalities import BotFunctionality
from fa_search_bot.subscriptions.query_parser import InvalidQueryException
from fa_search_bot.subscriptions.subscription import Subscription

if TYPE_CHECKING:
    from typing import List

    from fa_search_bot.subscriptions.subscription_watcher import SubscriptionWatcher, SubscriptionAlreadyPaused, \
    SubscriptionAlreadyRunning

logger = logging.getLogger(__name__)


class SubscriptionFunctionality(BotFunctionality):
    add_sub_cmd = "add_subscription"
    remove_sub_cmd = "remove_subscription"
    list_sub_cmd = "list_subscriptions"
    pause_cmds = ["pause", "suspend"]
    resume_cmds = ["unpause", "resume"]
    USE_CASE_ADD = "subscription_add"
    USE_CASE_REMOVE = "subscription_remove"
    USE_CASE_LIST = "subscription_list"
    USE_CASE_PAUSE_DEST = "subscription_dest_pause"
    USE_CASE_PAUSE_SUB = "subscription_pause"
    USE_CASE_RESUME_DEST = "subscription_dest_resume"
    USE_CASE_RESUME_SUB = "subscription_resume"

    def __init__(self, watcher: SubscriptionWatcher):
        commands = [self.add_sub_cmd, self.remove_sub_cmd, self.list_sub_cmd] + self.pause_cmds + self.resume_cmds
        commands_pattern = re.compile(r"^/(" + "|".join(re.escape(c) for c in commands) + ")")
        super().__init__(NewMessage(pattern=commands_pattern, incoming=True))
        self.watcher = watcher

    @property
    def usage_labels(self) -> List[str]:
        return [
            self.USE_CASE_ADD,
            self.USE_CASE_REMOVE,
            self.USE_CASE_LIST,
            self.USE_CASE_PAUSE_DEST,
            self.USE_CASE_PAUSE_SUB,
            self.USE_CASE_RESUME_DEST,
            self.USE_CASE_RESUME_SUB,
        ]

    async def call(self, event: NewMessage.Event) -> None:
        message_text = event.text
        command = message_text.split()[0]
        args = message_text[len(command) :].strip()
        await event.reply(await self._route_command(event.chat_id, event.sender_id, command, args), parse_mode="html")
        raise StopPropagation

    async def _route_command(self, destination: int, sender_id: int, command: str, args: str) -> str:
        if command.startswith("/" + self.add_sub_cmd):
            return await self._add_sub(destination, args, sender_id)
        elif command.startswith("/" + self.remove_sub_cmd):
            return await self._remove_sub(destination, args)
        elif command.startswith("/" + self.list_sub_cmd):
            return self._list_subs(destination)
        elif any(command.startswith("/" + cmd) for cmd in self.pause_cmds):
            if args:
                return await self._pause_subscription(destination, args)
            return await self._pause_destination(destination)
        elif any(command.startswith("/" + cmd) for cmd in self.resume_cmds):
            if args:
                return await self._resume_subscription(destination, args)
            return await self._resume_destination(destination)
        else:
            return "I do not understand."

    async def _add_sub(self, destination: int, query: str, creator_id: int) -> str:
        self.usage_counter.labels(function=self.USE_CASE_ADD).inc()
        if query == "":
            return "Please specify the subscription query you wish to add."
        try:
            new_sub = Subscription(query, destination)
        except InvalidQueryException as e:
            logger.error("Failed to parse new subscription query: %s", query, exc_info=e)
            return f"Failed to parse subscription query: {html.escape(str(e))}"
        new_sub.creator_id = creator_id
        new_sub.creation_date = datetime.datetime.now(tz=datetime.timezone.utc)
        if new_sub in self.watcher.subscriptions:
            return f'A subscription already exists for "{html.escape(query)}".'
        await self.watcher.add_subscription(new_sub)
        return f'Added subscription: "{html.escape(query)}".\n{self._list_subs(destination)}'

    async def _remove_sub(self, destination: int, query: str) -> str:
        self.usage_counter.labels(function=self.USE_CASE_REMOVE).inc()
        old_sub = Subscription(query, destination)
        try:
            await self.watcher.remove_subscription(old_sub)
            return f'Removed subscription: "{html.escape(query)}".\n{self._list_subs(destination)}'
        except KeyError:
            return f'There is not a subscription for "{html.escape(query)}" in this chat.'

    def _list_subs(self, destination: int) -> str:
        self.usage_counter.labels(function=self.USE_CASE_LIST).inc()
        subs = [sub for sub in self.watcher.subscriptions if sub.destination == destination]
        subs.sort(key=lambda sub: sub.query_str.casefold())
        sub_list_entries = []
        for sub in subs:
            sub_title = f"- {html.escape(sub.query_str)}"
            if sub.paused:
                sub_title = f"- ⏸<s>{html.escape(sub.query_str)}</s>"
            sub_list_entries.append(sub_title)
        subs_list = "\n".join(sub_list_entries)
        return f"Current subscriptions in this chat:\n{subs_list}"

    async def _pause_destination(self, chat_id: int) -> str:
        self.usage_counter.labels(function=self.USE_CASE_PAUSE_DEST).inc()
        try:
            await self.watcher.pause_destination(chat_id)
        except KeyError:
            return "There are no subscriptions posting here to pause."
        except SubscriptionAlreadyPaused:
            return "All subscriptions are already paused."
        return f"Paused all subscriptions.\n{self._list_subs(chat_id)}"

    async def _pause_subscription(self, chat_id: int, sub_name: str) -> str:
        self.usage_counter.labels(function=self.USE_CASE_PAUSE_SUB).inc()
        pause_sub = Subscription(sub_name, chat_id)
        try:
            await self.watcher.pause_subscription(pause_sub)
        except KeyError:
            return f'There is not a subscription for "{html.escape(sub_name)}" in this chat.'
        except SubscriptionAlreadyPaused:
            return f'Subscription for "{html.escape(sub_name)}" is already paused.'
        return f'Paused subscription: "{html.escape(sub_name)}".\n{self._list_subs(chat_id)}'

    async def _resume_destination(self, chat_id: int) -> str:
        self.usage_counter.labels(function=self.USE_CASE_RESUME_DEST).inc()
        try:
            await self.watcher.resume_destination(chat_id)
        except KeyError:
            return "There are no subscriptions posting here to resume."
        except SubscriptionAlreadyRunning:
            return "All subscriptions are already running."
        return f"Resumed all subscriptions.\n{self._list_subs(chat_id)}"

    async def _resume_subscription(self, chat_id: int, sub_name: str) -> str:
        self.usage_counter.labels(function=self.USE_CASE_RESUME_SUB).inc()
        pause_sub = Subscription(sub_name, chat_id)
        try:
            await self.watcher.resume_subscription(pause_sub)
        except KeyError:
            return f'There is not a subscription for "{html.escape(sub_name)}" in this chat.'
        except SubscriptionAlreadyRunning:
            return f'Subscription for "{html.escape(sub_name)}" is already running.'
        return f'Resumed subscription: "{html.escape(sub_name)}".\n{self._list_subs(chat_id)}'


class BlocklistFunctionality(BotFunctionality):
    add_block_tag_cmd = "add_blocklisted_tag"
    add_block_tag_cmd_short = "add_block"
    remove_block_tag_cmd = "remove_blocklisted_tag"
    remove_block_tag_cmd_short = "remove_block"
    list_block_tag_cmd = "list_blocklisted_tags"
    list_block_tag_cmd_short = "list_blocks"
    USE_CASE_ADD = "block_add"
    USE_CASE_REMOVE = "block_remove"
    USE_CASE_LIST = "block_list"

    def __init__(self, watcher: SubscriptionWatcher):
        commands = [
            self.add_block_tag_cmd,
            self.add_block_tag_cmd_short,
            self.remove_block_tag_cmd,
            self.remove_block_tag_cmd_short,
            self.list_block_tag_cmd,
            self.list_block_tag_cmd_short,
        ]
        commands_pattern = re.compile(r"^/(" + "|".join(re.escape(c) for c in commands) + ")")
        super().__init__(NewMessage(pattern=commands_pattern, incoming=True))
        self.watcher = watcher

    @property
    def usage_labels(self) -> List[str]:
        return [self.USE_CASE_ADD, self.USE_CASE_REMOVE, self.USE_CASE_LIST]

    async def call(self, event: NewMessage.Event) -> None:
        message_text = event.text
        destination = event.chat_id
        command = message_text.split()[0]
        args = message_text[len(command) :].strip()
        if command.startswith("/" + self.add_block_tag_cmd) or command.startswith("/" + self.add_block_tag_cmd_short):
            await event.reply(await self._add_to_blocklist(destination, args))
        elif command.startswith("/" + self.remove_block_tag_cmd) or command.startswith("/" + self.remove_block_tag_cmd_short):
            await event.reply(await self._remove_from_blocklist(destination, args))
        elif command.startswith("/" + self.list_block_tag_cmd) or command.startswith("/" + self.list_block_tag_cmd_short):
            await event.reply(self._list_blocklisted_tags(destination))
        else:
            await event.reply("I do not understand.")
        raise StopPropagation

    async def _add_to_blocklist(self, destination: int, query: str) -> str:
        self.usage_counter.labels(function=self.USE_CASE_ADD).inc()
        if query == "":
            return "Please specify the tag you wish to add to blocklist."
        try:
            await self.watcher.add_to_blocklist(destination, query)
        except InvalidQueryException as e:
            return f"Failed to parse blocklist query: {e}"
        return f'Added tag to blocklist: "{query}".\n{self._list_blocklisted_tags(destination)}'

    async def _remove_from_blocklist(self, destination: int, query: str) -> str:
        self.usage_counter.labels(function=self.USE_CASE_REMOVE).inc()
        try:
            await self.watcher.remove_from_blocklist(destination, query)
            return f'Removed tag from blocklist: "{query}".\n{self._list_blocklisted_tags(destination)}'
        except KeyError:
            return f'The tag "{query}" is not on the blocklist for this chat.'

    def _list_blocklisted_tags(self, destination: int) -> str:
        self.usage_counter.labels(function=self.USE_CASE_LIST).inc()
        blocklist = self.watcher.blocklists.get(destination)
        tags_list = "\n".join([f"- {tag}" for tag in blocklist.blocklists.keys()])
        return f"Current blocklist for this chat:\n{tags_list}"
