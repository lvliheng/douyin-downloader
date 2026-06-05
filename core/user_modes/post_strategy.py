from __future__ import annotations

from typing import Any, Dict, List

from core.user_modes.base_strategy import BaseUserModeStrategy
from utils.logger import setup_logger

logger = setup_logger("PostUserModeStrategy")


class PostUserModeStrategy(BaseUserModeStrategy):
    mode_name = "post"
    api_method_name = "get_user_post"

    async def collect_items(self, sec_uid: str, user_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        fetcher = getattr(self.downloader.api_client, self.api_method_name, None)
        if not callable(fetcher):
            logger.error("API client missing get_user_post")
            return []

        aweme_list: List[Dict[str, Any]] = []
        max_cursor = 0
        has_more = True
        pagination_restricted = False

        number_limit = int(self.downloader.config.get("number", {}).get(self.mode_name, 0) or 0)
        media_filter_enabled = self._media_type_filter_enabled()
        last_video_id = self._get_last_video_id()
        anchor_mode = False
        if last_video_id and self.downloader.database and user_info.get("uid"):
            count = await self.downloader.database.get_aweme_count_by_author(str(user_info.get("uid")))
            anchor_mode = count > 0
        anchor_found = False

        self.downloader._progress_update_step("拉取作品列表", "分页抓取中")

        while has_more:
            await self.downloader.rate_limiter.acquire()
            request_cursor = max_cursor
            page_data = await fetcher(sec_uid, request_cursor, 20)
            page = self._normalize_page_data(page_data)
            page_items = self.select_items(page)

            if not page_items:
                if page.get("status_code") == 0:
                    pagination_restricted = True
                    logger.warning(
                        "User post page empty at cursor=%s (status_code=0); "
                        "will attempt browser fallback",
                        request_cursor,
                    )
                break

            # Anchor handling: if configured and we haven't found it yet,
            # check whether this page contains the last_video_id anchor.
            if anchor_mode and not anchor_found:
                last_index = self._find_last_video_index(page_items, last_video_id)
                if last_index != -1:
                    anchor_found = True
                    # Keep older items after the anchor.
                    page_items = page_items[last_index + 1:]

            if not page_items:
                if anchor_found:
                    # Anchor found at the head of a page but there are no older items
                    # on this page. Continue to next page to fetch historical records.
                    pass
                else:
                    break

            page_items = self._filter_pinned_items(page_items)
            aweme_list.extend(page_items)

            self.downloader._progress_update_step("拉取作品列表", f"已抓取 {len(aweme_list)} 条")

            has_more = bool(page.get("has_more", False))
            max_cursor = int(page.get("max_cursor", 0) or 0)
            if has_more and max_cursor == request_cursor:
                logger.warning(
                    "max_cursor did not advance (%s), stop paging to avoid loop",
                    max_cursor,
                )
                pagination_restricted = True
                break

            # Only apply the configured `number_limit` once anchor search is
            # complete (or when anchor mode is not active) to avoid truncating
            # the result before we locate the `last_video_id`.
            if number_limit > 0 and (not anchor_mode or anchor_found):
                if media_filter_enabled:
                    if len(self._filter_by_media_type(aweme_list)) >= number_limit:
                        break
                elif len(aweme_list) >= number_limit:
                    aweme_list = aweme_list[:number_limit]
                    break

        if pagination_restricted:
            self.downloader._progress_update_step("拉取作品列表", "分页受限，尝试浏览器回补")
            await self.downloader._recover_user_post_with_browser(sec_uid, user_info, aweme_list)
            if not aweme_list:
                raise RuntimeError(
                    "抖音接口未返回作品列表（可能触发了反爬限制），"
                    "请稍后重试或尝试重新登录抖音刷新 Cookie"
                )
        return aweme_list
