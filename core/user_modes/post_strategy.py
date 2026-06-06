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
        latest_id = str(self.downloader.config.get("latest_id") or "").strip()
        last_id = str(self.downloader.config.get("last_id") or "").strip()

        # mode determination (set after first page)
        new_content_mode = False
        backfill_mode = False
        newest_seen_id = ""
        latest_id_found = False
        last_id_found = False

        self.downloader._progress_update_step("拉取作品列表", "分页抓取中")

        while has_more:
            await self.downloader.rate_limiter.acquire()
            request_cursor = max_cursor
            page_data = await fetcher(sec_uid, request_cursor, 20)
            page = self._normalize_page_data(page_data)
            page_items = self.select_items(page)
            page_items = self._filter_pinned_items(page_items)

            if not page_items:
                if page.get("status_code") == 0:
                    pagination_restricted = True
                    logger.warning(
                        "User post page empty at cursor=%s (status_code=0); "
                        "will attempt browser fallback",
                        request_cursor,
                    )
                break

            # Determine mode on first page
            if newest_seen_id == "" and page_items:
                newest_seen_id = str(page_items[0].get("aweme_id") or "").strip()
                if latest_id:
                    new_content_mode = (newest_seen_id != latest_id)
                    backfill_mode = (newest_seen_id == latest_id)
                else:
                    new_content_mode = True
                    backfill_mode = False

            # ── Filter items for current mode ──────────────────────────────

            # New content mode: take items newer than latest_id
            if new_content_mode and not latest_id_found and latest_id:
                lidx = self._find_last_video_index(page_items, latest_id)
                if lidx != -1:
                    latest_id_found = True
                    page_items = page_items[:lidx]

            # Backfill mode: take items older than last_id
            if backfill_mode and not last_id_found and last_id:
                lidx = self._find_last_video_index(page_items, last_id)
                if lidx != -1:
                    last_id_found = True
                    page_items = page_items[lidx + 1:]
                else:
                    page_items = []

            # Decide whether to stop or continue
            if not page_items:
                # Continue paginating if boundary not found yet
                if (new_content_mode and latest_id and not latest_id_found):
                    pass
                elif (backfill_mode and last_id and not last_id_found):
                    pass
                elif new_content_mode and not latest_id and not aweme_list:
                    pass
                else:
                    break

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

            # Apply number_limit after we've found the boundary (or if no boundary needed)
            can_limit = (
                (new_content_mode and (not latest_id or latest_id_found)) or
                (backfill_mode and last_id_found)
            )
            if number_limit > 0 and can_limit:
                if media_filter_enabled:
                    if len(self._filter_by_media_type(aweme_list)) >= number_limit:
                        break
                elif len(aweme_list) >= number_limit:
                    aweme_list = aweme_list[:number_limit]
                    break

            # Stop paginating once boundary is found in any mode
            if new_content_mode and latest_id_found:
                break
            if backfill_mode and last_id_found:
                break

        if pagination_restricted:
            self.downloader._progress_update_step("拉取作品列表", "分页受限，尝试浏览器回补")
            await self.downloader._recover_user_post_with_browser(sec_uid, user_info, aweme_list)
            if not aweme_list:
                raise RuntimeError(
                    "抖音接口未返回作品列表（可能触发了反爬限制），"
                    "请稍后重试或尝试重新登录抖音刷新 Cookie"
                )

        # Determine suggested update field/value
        self._suggested_update_field = ""
        self._suggested_update_value = ""
        if aweme_list:
            first_id = str(aweme_list[0].get("aweme_id") or "").strip()
            if backfill_mode and last_id_found:
                self._suggested_update_field = "last_id"
                self._suggested_update_value = first_id
            elif new_content_mode:
                self._suggested_update_field = "latest_id"
                self._suggested_update_value = first_id

        return aweme_list
