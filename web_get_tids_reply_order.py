import random
import re
import time
from typing import Iterable, Set, List, Optional

import requests
from lxml import html
import json

TIEBA_BASE_URL = "https://tieba.baidu.com/f"


def build_forum_url(kw: str, pn: int) -> str:
    return TIEBA_BASE_URL + f"?kw={kw}&pn={pn}"


def extract_thread_ids_from_commented_html(page_html: str) -> Set[int]:
    tree = html.fromstring(page_html)

    # 1. Get ALL comments in the document
    comment_nodes = tree.xpath("//comment()")

    all_ids: Set[int] = set()

    for c in comment_nodes:
        comment_text = c.text or ""
        # Quick filter: skip comments that clearly have nothing to do with threads
        if "/p/" not in comment_text:
            continue

        # 2. Wrap in a dummy root so we can parse multiple top-level elements
        fragment_html = "<root>" + comment_text + "</root>"

        try:
            frag = html.fromstring(fragment_html)
        except Exception:
            # sometimes comments might contain broken HTML; just skip those
            continue

        # 3. Now we are inside the *real* HTML that would have been shown
        # Use the same XPath you'd use in the browser:
        #   <a class="j_th_tit " href="/p/1234567890">...</a>
        hrefs = frag.xpath('.//a[contains(@class, "j_th_tit")]/@href')

        # If classes differ, a more generic fallback:
        # hrefs = frag.xpath('.//a[contains(@href, "/p/")]/@href')

        for href in hrefs:
            m = re.search(r"/p/(\d+)", href)
            if m:
                all_ids.add(int(m.group(1)))

    return all_ids


def scrape_tieba_thread_ids(
    web_tieba_cookies: str,
    kw: str,
    start_page: int = 0,
    max_pages: int = 1,
    delay_seconds: float = 20,
    session: Optional[requests.Session] = None,
) -> list[int]:
    if session is None:
        session = requests.Session()

    all_ids: set[int] = set()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36"
        ),
        "Cookie": web_tieba_cookies,
    }

    for page_index in range(start_page, max_pages):
        pn = page_index * 50  # 0, 50, 100, ...
        url = build_forum_url(kw, pn)

        print(f"Fetching page {page_index + 1} (pn={pn}) -> {url}")
        resp = session.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        with open("thread.html", "w") as f:
            f.write(resp.text)
        thread_ids = extract_thread_ids_from_commented_html(resp.text)
        print(f"  Found {len(thread_ids)} thread IDs on this page.")

        for tid in thread_ids:
            all_ids.add(tid)

        if page_index + 1 < max_pages:
            time.sleep(delay_seconds + (delay_seconds / 5) * random.random())

    return list(all_ids)
