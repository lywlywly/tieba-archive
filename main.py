import asyncio
import logging
import time
from typing import Iterable, Optional, Sequence

import aioitertools.itertools as ait
import aiotieba as tb
import yaml
from aiotieba.api._classdef import FragAt, FragEmoji, FragLink, FragText, UserInfo
from aiotieba.api.get_comments._classdef import (
    Comment,
    Comments,
    Contents_c,
    UserInfo_c,
)
from aiotieba.api.get_posts._classdef import (
    Contents_p,
    FragImage_p,
    FragVideo_p,
    FragVoice,
    Post,
    Thread_p,
    UserInfo_p,
    UserInfo_pt,
)
from aiotieba.logging import get_logger
from lxml import etree  # type: ignore
from lxml.builder import E
from sqlmodel import Session, SQLModel, create_engine, exists, select

from lib import to_xml
from tables import (
    CommentTable,
    ForumTable,
    ImageTable,
    PostTable,
    ThreadTable,
    UserProfile,
    UserTable,
)
from utils import (
    CaptureHandler,
    SingleFlightCompletedBuffer,
    download_bytes,
    download_file_async,
    inclusive_takewhile,
    sha256_bytes,
)
from web_get_tids_reply_order import scrape_tieba_thread_ids

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)
    CONCURRENCY_LIMIT = config["concurrency_limit"]
    assert CONCURRENCY_LIMIT and isinstance(CONCURRENCY_LIMIT, int)
    OUTPUT_DIR: str = config["output_dir"]
    assert OUTPUT_DIR and isinstance(OUTPUT_DIR, str)
    BDUSS: str = config["BDUSS"]
    assert BDUSS and isinstance(BDUSS, str)
    forum_names: list[str] = config["forum_names"]
    assert forum_names and isinstance(forum_names, list)
    for _forum_name in forum_names:
        assert isinstance(_forum_name, str)
    pages: int = config["pages"]
    assert pages and isinstance(pages, int)
    sleep: int = config["sleep"]
    assert sleep and isinstance(sleep, int)
    web_tieba_cookies: str = config["web_tieba_cookies"]
    assert web_tieba_cookies and isinstance(web_tieba_cookies, str)

SMALL_AVATAR_URL = "http://tb.himg.baidu.com/sys/portraitn/item/{portrait}"
LARGE_AVATAR_URL = "http://tb.himg.baidu.com/sys/portraith/item/{portrait}"


async def _ensure_avatar_image(
    session: Session,
    portrait: str,
    small_hash: Optional[str] = None,
) -> int:
    if small_hash is None:
        print(f"downloading small avatar for {portrait} to compute small_hash")
        small_bytes = await download_bytes(SMALL_AVATAR_URL.format(portrait=portrait))
        small_hash = sha256_bytes(small_bytes)  # returns str

    stmt = select(ImageTable).where(ImageTable.avatar_small_hash == small_hash)
    imgs = session.exec(stmt).all()
    assert len(imgs) <= 1, imgs

    if imgs:
        im = imgs[0]
        im_id = im.id
        assert im.avatar_large_hash is not None, "avatar_large_hash should always exist"
        print("avatar with same small hash found")
        return im_id  # type: ignore

    print("no avatar with same small hash found, downloading large avatar")
    _, large_hash = await download_file_async(
        LARGE_AVATAR_URL.format(portrait=portrait),
        use_hash_as_filename=True,
        output_dir=OUTPUT_DIR,
    )

    stmt = select(ImageTable).where(ImageTable.avatar_large_hash == large_hash)
    imgs = session.exec(stmt).all()
    assert len(imgs) <= 1, imgs

    if imgs:
        im = imgs[0]
        im_id = im.id
        print("avatar with same large hash found, updating small hash if needed")

        if im.avatar_small_hash != small_hash:
            im.avatar_small_hash = small_hash
            session.add(im)
            session.flush()

        return im_id  # type: ignore

    print("no matching avatar found, creating new ImageTable row")
    new_img = ImageTable(
        tieba_hash=None,
        avatar_small_hash=small_hash,
        avatar_large_hash=large_hash,
    )
    session.add(new_img)
    session.flush()
    print(f"added new avatar image row, id={new_img.id}")
    return new_img.id  # type: ignore


async def upsert_user_profile(
    session: Session,
    user_info: UserInfo | UserInfo_pt | UserInfo_p | UserInfo_c,
    strict_check_portrait: bool = False,
) -> UserProfile:
    assert user_info.portrait, repr(user_info)
    user = session.get(UserTable, user_info.user_id)
    if user is None:
        user = UserTable(
            uid=user_info.user_id,
            portrait=user_info.portrait,
            current_profile_id=None,
        )
        session.add(user)
        session.flush()

    latest_profile: Optional[UserProfile] = None
    if user.current_profile_id is not None:
        latest_profile = session.get(UserProfile, user.current_profile_id)

    same_name = (
        latest_profile is not None
        and latest_profile.username == user_info.user_name
        and latest_profile.nickname == user_info.nick_name
    )

    if same_name and not strict_check_portrait:
        return latest_profile  # type: ignore

    if same_name and strict_check_portrait:
        im = session.get(ImageTable, latest_profile.portrait_id)  # type: ignore
        assert im is not None

        print(f"strict check: downloading small avatar for {user_info.portrait}")
        small_bytes = await download_bytes(
            SMALL_AVATAR_URL.format(portrait=user_info.portrait)
        )
        new_small_hash = sha256_bytes(small_bytes)

        if im.avatar_small_hash and im.avatar_small_hash == new_small_hash:
            print("avatar small hash matches stored value, skipping")
            return latest_profile  # type: ignore

        print("avatar hash differs or missing, need new portrait image")
        im_id = await _ensure_avatar_image(
            session=session,
            portrait=user_info.portrait,
            small_hash=new_small_hash,
        )

    else:
        print("no reusable profile (no latest or name changed)")
        im_id = await _ensure_avatar_image(
            session=session,
            portrait=user_info.portrait,
            small_hash=None,
        )

    new_profile = UserProfile(
        uid=user_info.user_id,
        changed_at=int(time.time()),
        username=user_info.user_name,
        nickname=user_info.nick_name,
        portrait_id=im_id,
    )
    session.add(new_profile)
    session.flush()

    user.current_profile_id = new_profile.id
    session.add(user)
    session.commit()
    session.refresh(new_profile)

    return new_profile


async def safe_get_user_info(
    client: tb.Client, id: int, retry_limit: int = 10, invalid_user_id_limit: int = 3
):
    logger = get_logger()
    invalid_user_id_cnt = 0

    for retry in range(1, retry_limit + 1):
        if retry > 1:
            print(f"get_user_info({id}) failed, retry {retry}/{retry_limit}")

        handler = CaptureHandler()
        logger.addHandler(handler)
        try:
            user_info = await client.get_user_info(id)
        finally:
            logger.removeHandler(handler)

        if user_info.user_id > 0:
            return user_info
        messages = handler.messages
        if f"(300003, '加载数据失败'). args=({id},) kwargs={{}}" in messages:
            invalid_user_id_cnt += 1
            if invalid_user_id_cnt >= invalid_user_id_limit:
                print("deleted user")
                return user_info  # return empty user info

        if retry < retry_limit:
            await asyncio.sleep(3 * retry)

    raise RuntimeError(f"get_user_info({id}) failed after {retry_limit} retries")


singleflight = SingleFlightCompletedBuffer[int, None]()
singleflight_img = SingleFlightCompletedBuffer[str, etree.Element]()


async def safe_get_and_upsert_user_info(
    client: tb.Client,
    session: Session,
    user_id: int,
    user_info: UserInfo | UserInfo_pt | UserInfo_p | UserInfo_c | None = None,
):
    if not user_info:
        user_info = await safe_get_user_info(client, user_id)
    if user_info.user_id == 0:
        return
    await upsert_user_profile(session, user_info)


async def safe_get_and_upsert_user_info_once(
    client: tb.Client,
    session: Session,
    user_id: int,
    user_info: UserInfo | UserInfo_pt | UserInfo_p | UserInfo_c | None = None,
):
    # guard against multiple concurrent function calls being made, resulting in duplicate entries and large avatar download
    # non-overlapping duplicate calls are fine since checking and network request besides large avatar download is cheap
    await singleflight.do(
        user_id,
        lambda: safe_get_and_upsert_user_info(client, session, user_id, user_info),
    )


async def save_post_img( # type: ignore
    client: tb.Client,
    session: Session,
    hash: str,
    origin_src: str,
) -> etree.Element:
    statement = select(ImageTable).where(ImageTable.tieba_hash == hash)
    result = session.exec(statement).all()
    assert len(result) <= 1, result
    if not result:
        await download_file_async(origin_src, output_dir=OUTPUT_DIR)
        new_img = ImageTable(
            tieba_hash=hash, avatar_small_hash=None, avatar_large_hash=None
        )
        session.add(new_img)
        session.flush()
    else:
        print(f"image {hash} found record in database, skipping")
    return etree.Element("img", hash=hash, src=origin_src)


async def save_post_img_once(  # type: ignore
    client: tb.Client,
    session: Session,
    hash: str,
    origin_src: str,
) -> etree.Element:
    return await singleflight_img.do(
        hash,
        lambda: save_post_img(client, session, hash, origin_src),  # type: ignore
    )


async def get_element(session: Session, client: tb.Client, c: object) -> etree.Element:  # type: ignore
    match c:
        case FragText():
            return c.text
        case FragEmoji():
            return etree.Element("emoji", desc=c.desc, id=c.id)
        case FragImage_p():
            return await save_post_img_once(client, session, c.hash, c.origin_src)
        case FragAt():
            if c.user_id > 0:
                await safe_get_and_upsert_user_info_once(client, session, c.user_id)
            elem = etree.Element("at", hash=str(c.user_id))
            elem.text = c.text
            return elem
        case FragLink():
            elem = etree.Element("a", href=str(c.raw_url))
            elem.text = c.title
            return elem
        case FragVideo_p():
            return etree.Element("v")
        case FragVoice():
            return etree.Element("vo")
        case _:
            print(f"unknown fragment: {c}")
            raise Exception("Never")


async def contents_to_etree(  # type: ignore
    session: Session,
    client: tb.Client,
    contents: Contents_p[object] | Contents_c[object],
) -> etree.Element:
    doc = E.root(*[await get_element(session, client, c) for c in contents])
    return doc


async def contents_to_xml(
    session: Session,
    client: tb.Client,
    contents: Contents_p[object] | Contents_c[object],
) -> str:
    return etree.tostring(
        await contents_to_etree(session, client, contents), encoding="unicode"
    )


def query_user(session: Session, uid: int, latest: bool = False):
    statement = (
        (
            select(UserTable).where(UserTable.uid == uid)
            # .order_by(desc(UserTable.id))
            .limit(1)
        )
        if latest
        else (select(UserTable).where(UserTable.uid == uid).limit(1))
    )
    result = session.exec(statement).first()
    return result


async def save_post(session: Session, client: tb.Client, post: Post):
    stmt = select(exists().where(PostTable.pid == post.pid))  # type: ignore
    exists_result = session.exec(stmt).first()
    if exists_result:
        print("post exists skipping...")
        return

    await safe_get_and_upsert_user_info_once(client, session, post.author_id, post.user)

    print(f"saving post {post.pid}")
    p = PostTable(
        pid=post.pid,
        tid=post.tid,
        author_id=post.author_id,
        floor=post.floor,
        time=post.create_time,
        content=await contents_to_xml(session, client, post.contents),  # type: ignore
    )
    session.add(p)
    session.commit()
    print(f"post saved {post.pid}")


async def save_posts(session: Session, client: tb.Client, posts: list[Post]):
    for post in posts:
        await save_post(session, client, post)


async def save_comment(session: Session, client: tb.Client, comment: Comment):
    print(f"saving comment {comment.pid}")
    await safe_get_and_upsert_user_info_once(
        client, session, comment.author_id, comment.user
    )

    if comment.reply_to_id > 0:
        await safe_get_and_upsert_user_info_once(client, session, comment.reply_to_id)

    stmt = select(exists().where(CommentTable.cid == comment.pid))  # type: ignore
    exists_result = session.exec(stmt).first()

    if exists_result:
        print(f"comment {comment.pid} exists skipping...")
        return

    p = CommentTable(
        cid=comment.pid,
        pid=comment.ppid,
        author_id=comment.author_id,
        reply_to=comment.reply_to_id,
        time=comment.create_time,
        content=await contents_to_xml(session, client, comment.contents),  # type: ignore
    )
    session.add(p)
    session.commit()
    print(f"comment saved {comment.pid}")


async def parse_comments(session: Session, client: tb.Client, comments: Comments):
    for comment in comments:
        print(
            f"saving comment id: {comment.pid}, thread id: {comment.tid}, post id: {comment.ppid}"
        )
        await save_comment(session, client, comment)


async def save_thread(
    session: Session, client: tb.Client, thread: Thread_p, first_post: Post
):
    print(f"save thread")
    existing = session.exec(
        select(ThreadTable).where(ThreadTable.tid == thread.tid)
    ).first()

    vote_etree = to_xml(thread.vote_info)

    if existing:
        print(f"Thread {thread.tid} exists; updating XML content.")
        if thread.vote_info.title:
            base_et = etree.XML(existing.content)

            for child in list(base_et):  # type: ignore # make a copy
                if child.tag == "VoteInfo":
                    base_et.remove(child)
            for child in vote_etree:
                base_et.append(child)

            existing.content = etree.tostring(base_et, encoding="unicode")
        existing.view_num = thread.view_num
        existing.reply_num = thread.reply_num - 1
        existing.share_num = thread.share_num
        existing.agree = thread.agree
        existing.disagree = thread.disagree
        existing.updated_time = int(time.time())
        session.add(existing)

    else:
        print(f"Thread {thread.tid} not found; inserting new record.")
        await safe_get_and_upsert_user_info_once(
            client, session, thread.author_id, thread.user
        )
        post_et = await contents_to_etree(session, client, first_post.contents)  # type: ignore
        if thread.vote_info.title:
            for child in vote_etree:
                post_et.append(child)

        exists_result = session.exec(select(exists().where(ForumTable.fid == thread.fid))).first()  # type: ignore

        if not exists_result:
            print(
                f"Forum name: {thread.fname} id: {thread.fid} does not exist, creating"
            )
            new_forum = ForumTable(fid=thread.fid, forum_name=thread.fname)
            session.add(new_forum)

        new_thread = ThreadTable(
            tid=thread.tid,
            title=thread.title,
            time=thread.create_time,
            author_id=thread.author_id,
            updated_time=int(time.time()),
            forum_id=thread.fid,
            content=etree.tostring(post_et, encoding="unicode"),  # type: ignore
            view_num=thread.view_num,
            reply_num=thread.reply_num - 1,
            share_num=thread.share_num,
            agree=thread.agree,
            disagree=thread.disagree,
        )
        session.add(new_thread)

    session.commit()
    print(f"thread saved")


async def save_all_comments(
    session: Session, client: tb.Client, thread_id: int, post_id: int
):
    pn = 1
    while True:
        print(
            f"fetching comments: thread_id: {thread_id}, post_id id: {post_id}, pn:{pn}"
        )
        # FIXME: when failed
        comments = await client.get_comments(thread_id, post_id, pn=pn)
        await parse_comments(session, client, comments)
        if not comments.has_more:
            break
        pn += 1


async def safe_get_posts(
    client: tb.Client,
    thread_id: int,
    pn: int,
    retry_limit: int = 10,
    invalid_thread_id_limit: int = 3,
):
    logger = get_logger()
    invalid_thread_id_cnt = 0

    for retry in range(1, retry_limit + 1):
        if retry > 1:
            print(f"get_posts({thread_id}) failed, retry {retry}/{retry_limit}")

        handler = CaptureHandler()
        logger.addHandler(handler)
        try:
            posts = await client.get_posts(thread_id, pn)
        finally:
            logger.removeHandler(handler)

        if posts.objs:
            return posts
        messages = handler.messages
        if f"(4, '贴子可能已被删除'). args=({thread_id}, {pn}) kwargs={{}}" in messages:
            invalid_thread_id_cnt += 1
            if invalid_thread_id_cnt >= invalid_thread_id_limit:
                print("deleted thread")
                return posts  # return empty user info

        if retry < retry_limit:
            await asyncio.sleep(3 * retry)

    raise RuntimeError(f"get_posts({thread_id}) failed after {retry_limit} retries")


async def gen(client: tb.Client, thread_id: int, thread_info: Thread_p):
    async for pn in ait.count():
        print(f"thread: {thread_id} post pn={pn + 1}")
        posts = await safe_get_posts(client, thread_id, pn=pn + 1)
        if not posts.objs:
            return  # skip when failed to get posts (posts deleted)
        if pn == 0:
            thread_info.__dict__.update(posts.thread.__dict__)
        yield posts


async def get_all_posts(session: Session, client: tb.Client, thread_id: int):
    thread_info: Thread_p = Thread_p()
    posts = [
        post
        async for posts in inclusive_takewhile(
            lambda x: x.has_more,
            gen(client, thread_id, thread_info),
        )
        for post in posts
    ]
    if not posts:
        return  # skip when failed to get posts (posts deleted)
    await save_thread(session, client, thread_info, posts[0])
    await save_posts(session, client, posts[1:])

    post_ids = [post.pid for post in posts if post.reply_num > 0]
    tasks = [
        save_all_comments(session, client, thread_id, post_id) for post_id in post_ids
    ]
    await asyncio.gather(*tasks)


async def fetch_batch(session: Session, client: tb.Client, batch: Sequence[int]):
    tasks = [get_all_posts(session, client, tid) for tid in batch]  # Create tasks
    await asyncio.gather(*tasks)  # Run the batch concurrently
    print("return")


async def run_rolling(
    tids: Iterable[int], session: Session, client: tb.Client, limit: int = 5
):
    it = iter(tids)

    # prime the window
    in_flight: dict[asyncio.Task[None], int] = {}
    for _ in range(limit):
        try:
            tid = next(it)
        except StopIteration:
            break
        t = asyncio.create_task(get_all_posts(session, client, tid))
        in_flight[t] = tid

    # keep refilling as tasks complete
    while in_flight:
        done, _pending = await asyncio.wait(
            in_flight.keys(), return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            tid = in_flight.pop(task)
            try:
                await task  # surface task exception if any
            except Exception as e:
                print(f"task for tid={tid} failed")
                raise e

            # start next task immediately (if any left)
            try:
                next_tid = next(it)
            except StopIteration:
                continue
            new_task = asyncio.create_task(get_all_posts(session, client, next_tid))
            in_flight[new_task] = next_tid


async def main():
    with open("blacklist.txt", "r") as f:
        blacklist = set(int(line.strip()) for line in f.readlines())

    engine = create_engine("sqlite:///tieba.db")
    SQLModel.metadata.create_all(engine)

    while True:
        with Session(engine) as session:
            async with tb.Client(BDUSS) as client:
                for forum_name in forum_names:
                    thread_ids = [
                        x.tid
                        for i in range(1, pages)
                        for x in await client.get_threads(
                            forum_name, sort=tb.ThreadSortType.REPLY, pn=i, rn=20
                        )
                    ]
                    filtered_thread_ids = [t for t in thread_ids if t not in blacklist]
                    await run_rolling(
                        filtered_thread_ids, session, client, limit=CONCURRENCY_LIMIT
                    )
                time.sleep(sleep)


asyncio.run(main()) if __name__ == "__main__" else None
