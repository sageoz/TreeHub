"""
TreeHub Crawler — llms.txt fetcher with rate limiting and caching.

Usage:
    python scripts/crawler.py --platform supabase --url https://supabase.com/llms.txt
    python scripts/crawler.py --platform supabase --url https://supabase.com/llms.txt --output ./output/
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_CACHE_DIR = Path.home() / ".treehub" / "cache" / "crawl"
DEFAULT_OUTPUT_DIR = Path("indices")

MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0
BACKOFF_FACTOR = 2.0
REQUEST_TIMEOUT = 30.0

USER_AGENT = "TreeHub-Crawler/1.0 (+https://github.com/treehub/indices)"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CrawlResult:
    """Result of a single crawl operation."""

    platform: str
    source_url: str
    content: str
    content_hash: str
    fetched_at: str

    etag: str | None = None
    last_modified: str | None = None

    cached: bool = False
    status_code: int = 200
    was_updated: bool = False


@dataclass
class CrawlerConfig:
    """Configuration for the crawler."""

    cache_dir: Path = field(default_factory=lambda: DEFAULT_CACHE_DIR)
    output_dir: Path = field(default_factory=lambda: DEFAULT_OUTPUT_DIR)

    timeout: float = REQUEST_TIMEOUT
    max_retries: int = MAX_RETRIES
    respect_robots: bool = True
    user_agent: str = USER_AGENT


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------


class LlmsTxtCrawler:
    """Fetches llms.txt from documentation platforms."""

    def __init__(self, config: CrawlerConfig | None = None) -> None:
        self.config = config or CrawlerConfig()
        self.config.cache_dir.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def fetch(self, platform: str, url: str, *, force: bool = False) -> CrawlResult:
        """Fetch llms.txt content."""

        # if self.config.respect_robots and not self._check_robots(url):
        #     raise PermissionError(
        #         f"robots.txt disallows crawling {url}. "
        #         "Set respect_robots=False to override."
        #     )

        content, etag, last_modified, was_updated, status = self._fetch_conditional(
            url, platform
        )

        # ---------------------------------------------------------------
        # NEW: recursive fetch of linked llm documentation files
        # ---------------------------------------------------------------
        self._fetch_recursive(platform, content)

        content_hash = self._hash_content(content)
        now = datetime.now(timezone.utc).isoformat()

        result = CrawlResult(
            platform=platform,
            source_url=url,
            content=content,
            content_hash=f"sha256:{content_hash}",
            fetched_at=now,
            etag=etag,
            last_modified=last_modified,
            cached=not was_updated,
            status_code=status,
            was_updated=was_updated,
        )

        self._save_cache(platform, result)

        if result.was_updated:
            logger.info("Fetched %s (%d bytes) - updated", platform, len(content))
        else:
            logger.info("Using cached %s (%d bytes)", platform, len(content))

        return result

    def has_changed(self, platform: str, previous_hash: str) -> bool | None:
        """Check if cached content hash differs."""
        cached = self._load_cache(platform)

        if cached is None:
            return None

        return cached.content_hash != previous_hash

    # -----------------------------------------------------------------------
    # Recursive link extraction
    # -----------------------------------------------------------------------

    def _extract_llm_links(self, content: str) -> list[str]:
        """Extract .txt documentation links from llms.txt."""

        pattern = r"\((https://[^\)]+\.txt)\)"
        links = re.findall(pattern, content)

        return list(set(links))

    def _fetch_recursive(self, platform: str, base_content: str) -> None:
        """Fetch nested documentation files and save each separately."""

        links = self._extract_llm_links(base_content)

        if not links:
            return

        logger.info("Found %d nested documentation files", len(links))

        out_dir = self.config.output_dir / platform
        out_dir.mkdir(parents=True, exist_ok=True)

        with httpx.Client(timeout=self.config.timeout) as client:

            for link in links:
                try:
                    logger.info("Fetching nested doc: %s", link)

                    resp = client.get(link, headers={"User-Agent": self.config.user_agent})
                    resp.raise_for_status()

                    # filename from URL
                    filename = Path(urlparse(link).path).name
                    file_path = out_dir / filename

                    file_path.write_text(resp.text, encoding="utf-8")

                    logger.info("Saved %s", file_path)

                except Exception as e:
                    logger.warning("Failed to fetch nested doc %s: %s", link, e)

    # -----------------------------------------------------------------------
    # Internal fetch logic
    # -----------------------------------------------------------------------

    def _fetch_conditional(
        self, url: str, platform: str
    ) -> tuple[str, str | None, str | None, bool, int]:

        cached = self._load_cache(platform)

        headers = {"User-Agent": self.config.user_agent}
        use_conditional = False

        if cached:

            if cached.etag:
                headers["If-None-Match"] = cached.etag
                use_conditional = True

            elif cached.last_modified:
                headers["If-Modified-Since"] = cached.last_modified
                use_conditional = True

        backoff = INITIAL_BACKOFF
        last_exception: Exception | None = None

        client = httpx.Client(timeout=self.config.timeout)

        for attempt in range(1, self.config.max_retries + 1):

            try:

                response = client.get(url, headers=headers)

                status = response.status_code
                etag = response.headers.get("ETag")
                last_modified = response.headers.get("Last-Modified")

                if status == 304 and cached:

                    logger.info(
                        "Content not modified (304) for %s - using cache", platform
                    )

                    return (
                        cached.content,
                        cached.etag,
                        cached.last_modified,
                        False,
                        status,
                    )

                response.raise_for_status()

                content = response.text

                return content, etag, last_modified, True, status

            except (httpx.HTTPStatusError, httpx.RequestError) as exc:

                last_exception = exc

                logger.warning(
                    "Attempt %d/%d failed for %s: %s",
                    attempt,
                    self.config.max_retries,
                    url,
                    exc,
                )

                if attempt < self.config.max_retries:

                    time.sleep(backoff)
                    backoff *= BACKOFF_FACTOR

        raise ConnectionError(
            f"Failed to fetch {url} after {self.config.max_retries} retries"
        ) from last_exception

    # -----------------------------------------------------------------------
    # Utilities
    # -----------------------------------------------------------------------

    def _check_robots(self, url: str) -> bool:

        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        try:

            with httpx.Client(timeout=10) as client:

                resp = client.get(robots_url)

                if resp.status_code != 200:
                    return True

                path = parsed.path

                for line in resp.text.splitlines():

                    line = line.strip()

                    if line.lower().startswith("disallow:"):

                        disallowed = line.split(":", 1)[1].strip()

                        if disallowed and path.startswith(disallowed):
                            return False

                return True

        except httpx.RequestError:
            return True

    def _hash_content(self, content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def _cache_path(self, platform: str) -> Path:
        return self.config.cache_dir / f"{platform}.json"

    def _save_cache(self, platform: str, result: CrawlResult) -> None:

        cache_file = self._cache_path(platform)

        data = {
            "platform": result.platform,
            "source_url": result.source_url,
            "content": result.content,
            "content_hash": result.content_hash,
            "fetched_at": result.fetched_at,
            "status_code": result.status_code,
            "etag": result.etag,
            "last_modified": result.last_modified,
        }

        cache_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _load_cache(self, platform: str) -> CrawlResult | None:

        cache_file = self._cache_path(platform)

        if not cache_file.exists():
            return None

        try:

            data = json.loads(cache_file.read_text(encoding="utf-8"))

            return CrawlResult(
                platform=data["platform"],
                source_url=data["source_url"],
                content=data["content"],
                content_hash=data["content_hash"],
                fetched_at=data["fetched_at"],
                status_code=data.get("status_code", 200),
                etag=data.get("etag"),
                last_modified=data.get("last_modified"),
                cached=True,
            )

        except (json.JSONDecodeError, KeyError):

            logger.warning("Corrupt cache for %s, ignoring", platform)

            return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:

    parser = argparse.ArgumentParser(description="TreeHub llms.txt Crawler")

    parser.add_argument("--platform", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--force", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    config = CrawlerConfig(output_dir=Path(args.output))

    crawler = LlmsTxtCrawler(config)

    result = crawler.fetch(args.platform, args.url, force=args.force)

    out_dir = config.output_dir / args.platform
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_file = out_dir / "llms.txt"

    raw_file.write_text(result.content, encoding="utf-8")

    print(f"✅ Crawled {args.platform}")
    print(f"   URL:    {result.source_url}")
    print(f"   Hash:   {result.content_hash}")
    print(f"   Cached: {result.cached}")
    print(f"   Saved:  {raw_file}")


if __name__ == "__main__":
    main()