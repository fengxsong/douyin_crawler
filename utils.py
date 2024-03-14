import base64
import logging
import os
import random
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import cv2
import httpx
import numpy as np
from PIL import Image, ImageDraw
from playwright.async_api import Cookie, Page


def init_logging(lvl: Any, persistent: bool = False) -> logging.Logger:
    defaultFormat = "%(asctime)s %(name)s %(levelname)s %(message)s"
    logging.basicConfig(level=lvl, format=defaultFormat)

    logFormatter = logging.Formatter(defaultFormat)
    logger = logging.getLogger("crawler")
    logger.setLevel(lvl)

    if persistent:
        fh = logging.FileHandler(os.path.join(os.getcwd(), "crawler.log"))
        fh.setFormatter(logFormatter)
        logger.addHandler(fh)
    return logger


def convert_cookies(cookies: Optional[List[Cookie]]) -> Tuple[str, Dict]:
    if not cookies:
        return "", {}
    cookies_str = ";".join(
        [f"{cookie.get('name')}={cookie.get('value')}" for cookie in cookies]
    )
    cookie_dict = dict()
    for cookie in cookies:
        cookie_dict[cookie.get("name")] = cookie.get("value")
    return cookies_str, cookie_dict


def get_user_agent() -> str:
    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36",
    ]
    return random.choice(ua_list)


def _get_tracks(distance, seconds, ease_func) -> Tuple[List[int], List[int]]:
    tracks = [0]
    offsets = [0]
    for t in np.arange(0.0, seconds, 0.1):
        ease = globals()[ease_func]
        offset = round(ease(t / seconds) * distance)
        tracks.append(offset - offsets[-1])
        offsets.append(offset)
    return offsets, tracks


def get_tracks(distance: int, level: str = "easy") -> List[int]:
    if level == "easy":
        return get_track_simple(distance)
    else:
        _, tricks = _get_tracks(distance, seconds=2, ease_func="ease_out_expo")
        return tricks


async def find_login_qrcode(logger: logging.Logger, page: Page, selector: str) -> str:
    """find login qrcode image from target selector"""
    try:
        elements = await page.wait_for_selector(
            selector=selector,
        )
        login_qrcode_img = str(await elements.get_property("src"))
        if "http://" in login_qrcode_img or "https://" in login_qrcode_img:
            async with httpx.AsyncClient(follow_redirects=True) as ac:
                resp = await ac.get(
                    login_qrcode_img, headers={"User-Agent": get_user_agent()}
                )
                if resp.status_code == 200:
                    image_data = resp.content
                    base64_image = base64.b64encode(image_data).decode("utf-8")
                    return base64_image
                raise Exception(
                    f"fetch login image url failed, response message:{resp.text}"
                )
        return login_qrcode_img
    except Exception as e:
        logger.error(e)
        return ""


def show_qrcode(qr_code) -> None:  # type: ignore
    """parse base64 encode qrcode image and show it"""
    if "," in qr_code:
        qr_code = qr_code.split(",")[1]
    qr_code = base64.b64decode(qr_code)
    image = Image.open(BytesIO(qr_code))

    # Add a square border around the QR code and display it within the border to improve scanning accuracy.
    width, height = image.size
    new_image = Image.new("RGB", (width + 20, height + 20), color=(255, 255, 255))
    new_image.paste(image, (10, 10))
    draw = ImageDraw.Draw(new_image)
    draw.rectangle((0, 0, width + 19, height + 19), outline=(0, 0, 0), width=1)
    new_image.show()


def convert_str_cookie_to_dict(cookie_str: str) -> Dict:
    cookie_dict: Dict[str, str] = dict()
    if not cookie_str:
        return cookie_dict
    for cookie in cookie_str.split(";"):
        cookie = cookie.strip()
        if not cookie:
            continue
        cookie_list = cookie.split("=")
        if len(cookie_list) != 2:
            continue
        cookie_value = cookie_list[1]
        if isinstance(cookie_value, list):
            cookie_value = "".join(cookie_value)
        cookie_dict[cookie_list[0]] = cookie_value
    return cookie_dict


class Slide:
    def __init__(self, gap, bg, gap_size=None, bg_size=None, out=None):
        """
        :param gap: 缺口图片链接或者url
        :param bg: 带缺口的图片链接或者url
        """
        self.img_dir = os.path.join(os.getcwd(), "temp_image")
        if not os.path.exists(self.img_dir):
            os.makedirs(self.img_dir)

        bg_resize = bg_size if bg_size else (340, 212)
        gap_size = gap_size if gap_size else (68, 68)
        self.bg = self.check_is_img_path(bg, "bg", resize=bg_resize)
        self.gap = self.check_is_img_path(gap, "gap", resize=gap_size)
        self.out = out if out else os.path.join(self.img_dir, "out.jpg")

    @staticmethod
    def check_is_img_path(img, img_type, resize):
        if img.startswith("http"):
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;"
                "q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9,en-GB;q=0.8,en;q=0.7,ja;q=0.6",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "Host": urlparse(img).hostname,
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/91.0.4472.164 Safari/537.36",
            }
            img_res = httpx.get(img, headers=headers)
            if img_res.status_code == 200:
                img_path = f"./temp_image/{img_type}.jpg"
                image = np.asarray(bytearray(img_res.content), dtype="uint8")
                image = cv2.imdecode(image, cv2.IMREAD_COLOR)
                if resize:
                    image = cv2.resize(image, dsize=resize)
                cv2.imwrite(img_path, image)
                return img_path
            else:
                raise Exception(f"保存{img_type}图片失败")
        else:
            return img

    @staticmethod
    def clear_white(img):
        """清除图片的空白区域，这里主要清除滑块的空白"""
        img = cv2.imread(img)
        rows, cols, channel = img.shape
        min_x = 255
        min_y = 255
        max_x = 0
        max_y = 0
        for x in range(1, rows):
            for y in range(1, cols):
                t = set(img[x, y])
                if len(t) >= 2:
                    if x <= min_x:
                        min_x = x
                    elif x >= max_x:
                        max_x = x

                    if y <= min_y:
                        min_y = y
                    elif y >= max_y:
                        max_y = y
        img1 = img[min_x:max_x, min_y:max_y]
        return img1

    def template_match(self, tpl, target):
        th, tw = tpl.shape[:2]
        result = cv2.matchTemplate(target, tpl, cv2.TM_CCOEFF_NORMED)
        # 寻找矩阵(一维数组当作向量,用Mat定义) 中最小值和最大值的位置
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
        tl = max_loc
        br = (tl[0] + tw, tl[1] + th)
        # 绘制矩形边框，将匹配区域标注出来
        # target：目标图像
        # tl：矩形定点
        # br：矩形的宽高
        # (0,0,255)：矩形边框颜色
        # 1：矩形边框大小
        cv2.rectangle(target, tl, br, (0, 0, 255), 2)
        cv2.imwrite(self.out, target)
        return tl[0]

    @staticmethod
    def image_edge_detection(img):
        edges = cv2.Canny(img, 100, 200)
        return edges

    def discern(self):
        img1 = self.clear_white(self.gap)
        img1 = cv2.cvtColor(img1, cv2.COLOR_RGB2GRAY)
        slide = self.image_edge_detection(img1)

        back = cv2.imread(self.bg, cv2.COLOR_RGB2GRAY)
        back = self.image_edge_detection(back)

        slide_pic = cv2.cvtColor(slide, cv2.COLOR_GRAY2RGB)
        back_pic = cv2.cvtColor(back, cv2.COLOR_GRAY2RGB)
        x = self.template_match(slide_pic, back_pic)
        # 输出横坐标, 即 滑块在图片上的位置
        return x


def get_track_simple(distance) -> List[int]:
    # 有的检测移动速度的 如果匀速移动会被识别出来，来个简单点的 渐进
    # distance为传入的总距离
    # 移动轨迹
    track: List[int] = []
    # 当前位移
    current = 0
    # 减速阈值
    mid = distance * 4 / 5
    # 计算间隔
    t = 0.2
    # 初速度
    v = 1

    while current < distance:
        if current < mid:
            # 加速度为2
            a = 4
        else:
            # 加速度为-2
            a = -3
        v0 = v
        # 当前速度
        v = v0 + a * t  # type: ignore
        # 移动距离
        move = v0 * t + 1 / 2 * a * t * t
        # 当前位移
        current += move  # type: ignore
        # 加入轨迹
        track.append(round(move))
    return track


def get_tracks(distance: int, level: str = "easy") -> List[int]:
    if level == "easy":
        return get_track_simple(distance)
    else:
        from . import easing

        _, tricks = easing.get_tracks(distance, seconds=2, ease_func="ease_out_expo")
        return tricks
