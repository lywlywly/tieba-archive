import asyncio
import hashlib
import itertools
import logging
import os
import random
import re
import traceback
from collections import OrderedDict
from collections.abc import Callable, Coroutine, Hashable
from typing import Any, AsyncIterable, AsyncIterator, Callable, Generic, TypeVar

import aiofiles
import aiohttp

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")


class SingleFlightCompletedBuffer(Generic[K, T]):
    def __init__(self, max_completed: int = 1000) -> None:
        self._lock = asyncio.Lock()
        self._inflight: dict[K, asyncio.Task[T]] = {}
        self._completed: OrderedDict[K, T] = OrderedDict()
        self._max_completed = max_completed

    async def do(
        self,
        key: K,
        worker: Callable[[], Coroutine[Any, Any, T]],
    ) -> T:
        async with self._lock:
            print(f"size: {len(self._completed)}")
            # fast path: completed buffer
            cached = self._completed.get(key)
            if key in self._completed:
                return self._completed[key]

            # check inflight
            task = self._inflight.get(key)
            if task is None:

                async def runner() -> T:
                    try:
                        return await worker()
                    except Exception:
                        print(f"worker failed for key={key!r}")
                        traceback.print_exc()
                        raise

                task = asyncio.create_task(runner(), name=f"singleflight:{key}")
                self._inflight[key] = task

        try:
            result = await task
        finally:
            async with self._lock:
                if self._inflight.get(key) is task:
                    del self._inflight[key]

        # store into completed buffer (FIFO)
        async with self._lock:
            cached = self._completed.get(key)
            if cached is not None:
                return cached

            self._completed[key] = result

            if len(self._completed) > self._max_completed:
                self._completed.popitem(last=False)

            return result


class CaptureHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord):
        self.messages.append(record.getMessage())


async def async_count(start: int = 0, step: int = 1):
    for i in itertools.count(start, step):
        yield i
        await asyncio.sleep(0)  # Optional: yield to event loop


async def inclusive_takewhile(
    pred: Callable[[T], bool],
    aiter: AsyncIterable[T],
) -> AsyncIterator[T]:
    """
    Like itertools.takewhile, but includes the first element
    that fails the predicate.
    """
    async for item in aiter:
        yield item
        if not pred(item):
            break


def _infer_filename_from_headers(cd_header: str | None) -> str | None:
    """
    Extract filename from a Content-Disposition header if present.
    Handles: attachment; filename="name.ext" and RFC5987 filename*=
    """
    if not cd_header:
        return None

    # filename*=UTF-8''encoded-name.ext
    m = re.search(r"filename\*\s*=\s*[^']*'[^']*'([^;]+)", cd_header, flags=re.I)
    if m:
        from urllib.parse import unquote

        return unquote(m.group(1))

    # filename="name.ext" or filename=name.ext
    m = re.search(r'filename\s*=\s*"([^"]+)"', cd_header, flags=re.I)
    if m:
        return m.group(1)
    m = re.search(r"filename\s*=\s*([^;]+)", cd_header, flags=re.I)
    if m:
        return m.group(1).strip()

    return None


def _dedupe_path(path: str) -> tuple[bool, str]:
    """If path exists, append (1), (2), ... before the extension."""
    base, ext = os.path.splitext(path)
    counter = 0
    candidate = path
    while os.path.exists(candidate):
        counter += 1
        candidate = f"{base} ({counter}){ext}"
    if counter > 0:
        # print(f"duplicate file, new name: {candidate}")
        print(f"duplicate file, omitting: {path}")
        return True, candidate

    return False, candidate


async def download_bytes(
    url: str,
    retries: int = 10,
    backoff_factor: float = 1.0,  # base sleep time
    timeout: float = 30.0,
) -> bytes:
    """
    Download `url` and return bytes. Retries on network errors and 5xx responses.
    """
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)

    for attempt in range(retries + 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
                async with session.get(url) as resp:
                    # Retry only on server errors
                    if resp.status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message="Server error",
                            headers=resp.headers,
                        )
                    resp.raise_for_status()
                    return await resp.read()

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Retry {attempt}/{retries} after error: {e}")
            if attempt == retries:
                raise  # Out of retries → rethrow

            # Exponential backoff with jitter
            sleep_time = backoff_factor * (2**attempt) + random.random() * 0.1
            await asyncio.sleep(sleep_time)

    raise Exception("Never")


async def download_file_async(
    url: str,
    filename: str | None = None,
    use_hash_as_filename: bool = False,
    chunk_size: int = 8192,
    output_dir: str = ".",
    timeout_seconds: int = 10,
    retry: int = 10,
) -> tuple[str, str | None]:
    # TODO: Assumption: no multiple `download_file_async` calls being made on the same url or same hash result when `use_hash_as_filename` is `True`. Otherwise, `os.replace` might rename the incomplete file from another function call to the final one
    """
    Asynchronously download a file from URL and save it locally.

    Args:
        url: The file URL.
        filename: Optional explicit filename. If None, inferred from headers or URL.
        chunk_size: Stream chunk size in bytes.
        output_dir: Directory to save the file (created if missing).
        timeout_seconds: Request timeout.
        retry: Number of retry attempts for network errors.

    Returns:
        Absolute path to the downloaded file.
    """
    os.makedirs(output_dir, exist_ok=True)
    attempt = 0

    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_seconds)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()

                    # Determine final filename
                    if filename is None:
                        cd = resp.headers.get("Content-Disposition")
                        name_from_cd = _infer_filename_from_headers(cd)
                        if name_from_cd:
                            filename = name_from_cd
                        else:
                            filename = (
                                os.path.basename(url.split("?", 1)[0])
                                or "downloaded_file"
                            )

                    final_path = os.path.join(output_dir, filename)
                    is_dupe, _ = _dedupe_path(final_path)
                    if is_dupe and not use_hash_as_filename:
                        return (final_path, None)

                    # Temporary path: <filename>.part
                    temp_path = final_path + ".part"

                    if use_hash_as_filename:
                        h = hashlib.sha256()

                    try:
                        async with aiofiles.open(temp_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                if chunk:
                                    await f.write(chunk)
                                    if use_hash_as_filename:
                                        h.update(chunk)  # type: ignore

                        # Atomic rename on success
                        try:
                            if use_hash_as_filename:
                                _hash = h.hexdigest()  # type: ignore
                                dst = os.path.join(output_dir, _hash + ".jpg")
                                if os.path.exists(dst):
                                    print(f"{dst} exists, replacing")
                                os.replace(temp_path, dst)
                            else:
                                os.replace(temp_path, final_path)
                        except FileNotFoundError:
                            # Most likely another concurrent task already moved temp_path -> final_path.
                            # If final_path exists, we can safely treat this as success.
                            if os.path.exists(final_path):
                                return (os.path.abspath(final_path), None)
                            raise

                    except Exception:
                        # Cleanup partial file if exists
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                        raise

                    if use_hash_as_filename:
                        return (os.path.abspath(final_path), _hash)  # type: ignore
                    return (os.path.abspath(final_path), None)

        except (
            aiohttp.ClientResponseError,
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as e:
            attempt += 1
            if attempt > retry:
                raise RuntimeError(f"Download failed after {retry} retries: {e}") from e
            print(f"Retry {attempt}/{retry} after error: {e}")
            await asyncio.sleep(2 * attempt)


def sha256_bytes(data: bytes) -> str:
    """
    Compute SHA-256 hash of raw bytes.

    Parameters:
        data (bytes): Raw byte content.

    Returns:
        str: Hex digest of the SHA-256 hash.
    """
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()
