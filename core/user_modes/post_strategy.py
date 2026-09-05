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

        number_limit = int(self.downloader.config.get("number", {}).get(self.mode_name, 0) or 0)
        latest_video_id = str(self.downloader.config.get("latest_video_id") or "").strip()
        last_video_id = str(self.downloader.config.get("last_video_id") or "").strip()

        # ── First run (no boundaries) ──
        if not latest_video_id and not last_video_id:
            return await self._collect_first_run(sec_uid, user_info, number_limit)

        # ── Boundary-based pagination ──
        aweme_list: List[Dict[str, Any]] = []
        max_cursor = 0
        has_more = True
        pagination_restricted = False
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

            for item in page_items:
                item_id = str(item.get("aweme_id") or "").strip()
                if not latest_id_found and latest_video_id and item_id == latest_video_id:
                    latest_id_found = True
                if not last_id_found and last_video_id and item_id == last_video_id:
                    last_id_found = True

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

            need_latest = bool(latest_video_id) and not latest_id_found
            need_last = bool(last_video_id) and not last_id_found
            if not need_latest and not need_last:
                break

        if pagination_restricted:
            self.downloader._progress_update_step("拉取作品列表", "分页受限，尝试浏览器回补")
            await self.downloader._recover_user_post_with_browser(sec_uid, user_info, aweme_list)
            if not aweme_list:
                raise RuntimeError(
                    "抖音接口未返回作品列表（可能触发了反爬限制），"
                    "请稍后重试或尝试重新登录抖音刷新 Cookie"
                )

        # ── Find boundary positions in the flat list ──
        latest_pos = -1
        last_pos = -1
        for i, item in enumerate(aweme_list):
            item_id = str(item.get("aweme_id") or "").strip()
            if latest_pos == -1 and latest_video_id and item_id == latest_video_id:
                latest_pos = i
            if last_pos == -1 and last_video_id and item_id == last_video_id:
                last_pos = i

        # ── Slice by boundaries ──
        result: List[Dict[str, Any]] = []

        # 锚点判定：配置了但整个作品列表里没找到，视为该视频已删除/不可见。
        # 仅在完整翻页（未被反爬截断）后才可信，否则会给下游造成误报。
        latest_missing = bool(latest_video_id) and latest_pos == -1
        last_missing = bool(last_video_id) and last_pos == -1
        if latest_missing or last_missing:
            logger.warning(
                "Anchor not found: latest_missing=%s last_missing=%s",
                latest_missing,
                last_missing,
            )
        if not pagination_restricted:
            if latest_missing:
                print(
                    f"[anchor] latest_video_id={latest_video_id} missing "
                    f"head={str((aweme_list[0].get('aweme_id') if aweme_list else '') or '')}"
                )
            if last_missing:
                print(
                    f"[anchor] last_video_id={last_video_id} missing "
                    f"tail={str((aweme_list[-1].get('aweme_id') if aweme_list else '') or '')}"
                )

        # Items NEWER than latest_video_id (before it in the list)
        if latest_pos > 0:
            print('latest_pos:', latest_pos)
            new_items = aweme_list[:latest_pos]
            new_items.reverse()
            result.extend(new_items)
        elif latest_missing:
            # latest 锚点已失效：回退到 last 锚点为止的整段较新作品。
            # 若 last 也失效，则回退整份列表，交给 number_limit 限制单次拉取量。
            if last_pos > 0:
                new_items = aweme_list[:last_pos]
                new_items.reverse()
                result.extend(new_items)
            else:
                new_items = list(aweme_list)
                new_items.reverse()
                result.extend(new_items)

        # Items OLDER than last_video_id (after it in the list)
        if last_pos != -1 and last_pos < len(aweme_list) - 1:
            print('last_pos:', last_pos)
            old_items = aweme_list[last_pos + 1:]
            result.extend(old_items)
        elif last_missing and not (latest_missing and last_pos == -1):
            # last 锚点已失效：回退到 latest 锚点之后的整段较老作品。
            if latest_pos != -1 and latest_pos < len(aweme_list) - 1:
                old_items = aweme_list[latest_pos + 1:]
                result.extend(old_items)
            elif latest_pos == -1:
                old_items = list(aweme_list)
                old_items.reverse()
                result.extend(old_items)

        if number_limit > 0:
            result = result[:number_limit]

        print('collect_items result \n')
        for item in result:
            logger.debug(
                "Item %s: id=%s, title=%s, date=%s",
                self.mode_name,
                item.get("aweme_id"),
                (item.get("desc") or "no_title")[:40],
                item.get("create_time"),
            )
        return result

    async def _collect_first_run(
        self, sec_uid: str, user_info: Dict[str, Any], number_limit: int
    ) -> List[Dict[str, Any]]:
        fetcher = getattr(self.downloader.api_client, self.api_method_name, None)
        aweme_list: List[Dict[str, Any]] = []
        max_cursor = 0
        has_more = True
        pagination_restricted = False

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

            if number_limit > 0 and len(aweme_list) >= number_limit:
                break

        if pagination_restricted:
            self.downloader._progress_update_step("拉取作品列表", "分页受限，尝试浏览器回补")
            await self.downloader._recover_user_post_with_browser(sec_uid, user_info, aweme_list)
            if not aweme_list:
                raise RuntimeError(
                    "抖音接口未返回作品列表（可能触发了反爬限制），"
                    "请稍后重试或尝试重新登录抖音刷新 Cookie"
                )

        if number_limit > 0:
            aweme_list = aweme_list[:number_limit]

        print('_collect_first_run result \n')
        for item in aweme_list:
            logger.debug(
                "Item %s: id=%s, title=%s, date=%s",
                self.mode_name,
                item.get("aweme_id"),
                (item.get("desc") or "no_title")[:40],
                item.get("create_time"),
            )
        return aweme_list
