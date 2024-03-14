import asyncio
import logging
import os
from abc import ABC
from asyncio import Task
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import BrowserContext, BrowserType, Page, async_playwright

from client import Client
from login import Login
from utils import convert_cookies


class Crawler(ABC):
    context_page: Page
    dy_client: Client
    browser_context: BrowserContext

    def __init__(
        self,
        login_phone: Optional[str] = None,
        search_keywords: Optional[List[str]] = None,
        awemes: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"  # fixed
        self.index_url = "https://www.douyin.com"
        self.login_phone = login_phone
        self.search_keywords = search_keywords
        self.awemes = awemes
        self.headless = bool(kwargs.get("headless")) or True
        self.login_type = kwargs.get("login_type") or "qrcode"
        self.cookie_str = kwargs.get("cookies")
        self.max_note_count = kwargs.get("max_note_count") or 32
        self.max_comments = kwargs.get("max_comments") or 0
        self.search_comment_keywords = kwargs.get("search_comment_keywords")
        self.save_login_state = kwargs.get("save_login_state") or True
        self.logger = kwargs.get("logger") or logging.getLogger(self.__class__.__name__)

    async def start(self) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None

        async with async_playwright() as playwright:
            # Launch a browser context.
            chromium = playwright.chromium
            self.browser_context = await self.launch_browser(
                chromium, None, self.user_agent, headless=self.headless
            )
            # stealth.min.js is a js script to prevent the website from detecting the crawler.
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)

            self.dy_client = await self.create_douyin_client(httpx_proxy_format)

            if self.login_phone is None:
                self.logger.info("will not attempt to login")
            else:
                if not await self.dy_client.pong(browser_context=self.browser_context):
                    login_obj = Login(
                        login_type=self.login_type,
                        login_phone=self.login_phone,
                        browser_context=self.browser_context,
                        context_page=self.context_page,
                        cookie_str=self.cookie_str,
                    )
                    await login_obj.begin()
                    await self.dy_client.update_cookies(
                        browser_context=self.browser_context
                    )

            if self.search_keywords and len(self.search_keywords) > 0:
                # Search for notes and retrieve their comment information.
                await self.search(self.search_keywords)
            elif self.awemes and len(self.awemes) > 0:
                # Get the information and comments of the specified post
                await self.get_specified_awemes(self.awemes)
            else:
                self.logger.info("do nothing.")

            self.logger.info("crawler finished ...")

    async def search(self, keywords: Tuple[str]) -> None:
        self.logger.info(f"searching for keywords {keywords}")
        for keyword in keywords:
            aweme_list: List[str] = []
            dy_limit_count = 10
            page = 0
            while (page + 1) * dy_limit_count <= self.max_note_count:
                try:
                    posts_res = await self.dy_client.search_info_by_keyword(
                        keyword=keyword, offset=page * dy_limit_count
                    )
                except Exception as e:
                    self.logger.error(f"failed to search {keyword}: {e}")
                    break
                page += 1
                for post_item in posts_res.get("data"):
                    try:
                        aweme_info: Dict = (
                            post_item.get("aweme_info")
                            or post_item.get("aweme_mix_info", {}).get("mix_items")[0]
                        )
                    except TypeError:
                        continue
                    aweme_list.append(aweme_info.get("aweme_id", ""))
            self.logger.info(f"search keyword:{keyword}, aweme_list:{aweme_list}")
            await self.batch_get_note_comments(aweme_list)

    async def get_specified_awemes(self, aweme_list: List[str]):
        """Get the information and comments of the specified post"""
        semaphore = asyncio.Semaphore(os.cpu_count())
        task_list = [
            self.get_aweme_detail(aweme_id=aweme_id, semaphore=semaphore)
            for aweme_id in aweme_list
        ]
        aweme_details = await asyncio.gather(*task_list)
        for aweme_detail in aweme_details:
            if aweme_detail is not None:
                self.logger.info(aweme_detail)
        await self.batch_get_note_comments(aweme_list)

    async def get_aweme_detail(
        self, aweme_id: str, semaphore: asyncio.Semaphore
    ) -> Any:
        """Get note detail"""
        async with semaphore:
            try:
                return await self.dy_client.get_video_by_id(aweme_id)
            except Exception as ex:
                self.logger.error(f"failed to get aweme detail error: {ex}")
                return None

    async def batch_get_note_comments(self, aweme_list: List[str]) -> None:
        task_list: List[Task] = []
        semaphore = asyncio.Semaphore(os.cpu_count())
        for aweme_id in aweme_list:
            task = asyncio.create_task(
                self.get_comments(aweme_id, semaphore, max_comments=self.max_comments),
                name=aweme_id,
            )
            task_list.append(task)
        await asyncio.wait(task_list)

    async def get_comments(
        self, aweme_id: str, semaphore: asyncio.Semaphore, max_comments: int = None
    ) -> None:
        async with semaphore:
            try:
                # 将关键词列表传递给 get_aweme_all_comments 方法
                async for comments in self.dy_client.get_aweme_all_comments(
                    aweme_id=aweme_id,
                    max_comments=max_comments,  # 最大数量
                    keywords=self.search_comment_keywords,  # 关键词列表
                ):
                    self.logger.info(comments)
            except Exception as e:
                self.logger.error(
                    f"failed to fetch comments for aweme_id: {aweme_id}, error: {e}"
                )

    async def create_douyin_client(self, httpx_proxy: Optional[str]) -> Client:
        """Create douyin client"""
        cookie_str, cookie_dict = convert_cookies(await self.browser_context.cookies())  # type: ignore
        douyin_client = Client(
            proxies=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": cookie_str,
                "Host": "www.douyin.com",
                "Origin": "https://www.douyin.com/",
                "Referer": "https://www.douyin.com/",
                "Content-Type": "application/json;charset=UTF-8",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return douyin_client

    async def launch_browser(
        self,
        chromium: BrowserType,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        if self.save_login_state:
            user_data_dir = os.path.join(
                os.getcwd(),
                "browser_data",
            )  # type: ignore
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                accept_downloads=True,
                headless=headless,
                proxy=playwright_proxy,  # type: ignore
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent,
            )  # type: ignore
            return browser_context
        else:
            browser = await chromium.launch(headless=headless, proxy=playwright_proxy)  # type: ignore
            browser_context = await browser.new_context(
                viewport={"width": 1920, "height": 1080}, user_agent=user_agent
            )
            return browser_context

    async def close(self) -> None:
        """Close browser context"""
        await self.browser_context.close()
        self.logger.info("close browser context ...")
