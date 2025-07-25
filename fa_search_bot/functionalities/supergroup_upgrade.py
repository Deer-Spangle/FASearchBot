from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from telethon import events
from telethon.events import StopPropagation
from telethon.tl import types

from fa_search_bot.functionalities.functionalities import BotFunctionality

if TYPE_CHECKING:
    from typing import List

    from telethon.tl.types import UpdateNewChannelMessage

    from fa_search_bot.subscriptions.subscription_watcher import SubscriptionWatcher

logger = logging.getLogger(__name__)


def filter_migration(event: UpdateNewChannelMessage) -> bool:
    if event.message and event.message.action is not None:
        return isinstance(event.message.action, types.MessageActionChannelMigrateFrom)
    return False


class SupergroupUpgradeFunctionality(BotFunctionality):
    def __init__(self, watcher: SubscriptionWatcher):
        super().__init__(events.Raw(types.UpdateNewChannelMessage, func=filter_migration))
        self.watcher = watcher

    @property
    def usage_labels(self) -> List[str]:
        return ["supergroup_upgrade"]

    async def call(self, event: types.UpdateNewChannelMessage) -> None:
        old_chat_id = -1 * event.message.action.chat_id
        new_chat_id = int("-100" + str(event.message.to_id.channel_id))
        # Log the upgrade
        logger.info("Migration from chat ID: %s to chat ID: %s", old_chat_id, new_chat_id)
        self.usage_counter.labels(function="supergroup_upgrade").inc()
        # Upgrade subscriptions and block queries
        await self.watcher.migrate_chat(old_chat_id, new_chat_id)
        raise StopPropagation
