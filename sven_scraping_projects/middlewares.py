# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from __future__ import annotations

from dataclasses import dataclass

from scrapy import signals
from scrapy.exceptions import IgnoreRequest

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class SvenScrapingProjectsSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class SvenScrapingProjectsDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class HttpStatusLoggingMiddleware:
    """
    Downloader middleware to log non-200 responses and record status counters.
    Does NOT block responses (blocking is handled in the spider middleware so
    retries + other downloader middlewares still run normally).
    """

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        s.stats = crawler.stats
        return s

    def process_response(self, request, response, spider):
        try:
            status = int(getattr(response, "status", 0) or 0)
        except Exception:
            status = 0

        # Always track counts, even if we later skip parsing.
        if status:
            self.stats.inc_value(f"http_status_count/{status}", spider=spider)
            if status == 404:
                self.stats.inc_value("http_status_count/404", spider=spider)

        if status and status != 200:
            # Prefer warning over error: many sites use 301/302; 4xx/5xx are the real problem.
            level = "warning" if status < 500 else "error"
            msg = (
                f"Non-200 response: status={status} url={response.url} "
                f"referer={request.headers.get('Referer', b'').decode(errors='ignore')!r}"
            )
            getattr(spider.logger, level)(msg)
        return response


class Non200ResponseGuardSpiderMiddleware:
    """
    Spider middleware that prevents callbacks from parsing non-200 responses.
    This avoids silently yielding empty/partial items and makes scheduled runs
    deterministic.

    Spiders can override via `allow_parse_httpstatus_list = {codes}`.
    """

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_spider_input(self, response, spider):
        status = getattr(response, "status", 200)
        if status == 200:
            return None

        allow = getattr(spider, "allow_parse_httpstatus_list", set())
        if status in allow:
            return None

        # Skip parsing: treat as a download-level failure.
        raise IgnoreRequest(f"Skipping parse for status={status} url={response.url}")


@dataclass
class _SpiderRunCounters:
    responses: int = 0
    items: int = 0
    non_200: int = 0
    http_404: int = 0
    sitemap_locs_total: int = 0
    sitemap_locs_scheduled: int = 0


class RunValidationExtension:
    """
    Run-level validation + summaries.

    Kept in middlewares.py so Apify Docker builds always ship it alongside
    settings that reference EXTENSIONS (avoids ModuleNotFoundError if extensions.py
    is missing from the image).
    """

    def __init__(self, crawler):
        self.crawler = crawler
        self.stats = crawler.stats
        self.settings = crawler.settings
        self._counters = _SpiderRunCounters()

        self._min_responses = int(self.settings.getint("RUN_VALIDATION_MIN_RESPONSES", 200))
        self._max_404_rate = float(self.settings.getfloat("RUN_VALIDATION_MAX_404_RATE", 0.05))
        self._min_items_per_100 = float(
            self.settings.getfloat("RUN_VALIDATION_MIN_ITEMS_PER_100_RESPONSES", 5)
        )

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        self.stats.set_value("run_validation/enabled", True, spider=spider)

    def response_received(self, response, request, spider):
        self._counters.responses += 1
        status = getattr(response, "status", 200)
        if status != 200:
            self._counters.non_200 += 1
        if status == 404:
            self._counters.http_404 += 1

    def item_scraped(self, item, response, spider):
        self._counters.items += 1

    def _read_sitemap_counters_from_spider(self, spider):
        self._counters.sitemap_locs_total = int(getattr(spider, "sitemap_locs_total", 0) or 0)
        self._counters.sitemap_locs_scheduled = int(
            getattr(spider, "sitemap_locs_scheduled", 0) or 0
        )

    def spider_closed(self, spider, reason):
        self._read_sitemap_counters_from_spider(spider)

        responses = max(self._counters.responses, 0)
        items = max(self._counters.items, 0)
        http_404 = max(self._counters.http_404, 0)

        rate_404 = (http_404 / responses) if responses else 0.0
        items_per_100 = (items / responses * 100.0) if responses else 0.0
        sitemap_cov = None
        if self._counters.sitemap_locs_total:
            sitemap_cov = self._counters.sitemap_locs_scheduled / self._counters.sitemap_locs_total

        self.stats.set_value("run_validation/responses", responses, spider=spider)
        self.stats.set_value("run_validation/items", items, spider=spider)
        self.stats.set_value("run_validation/http_404", http_404, spider=spider)
        self.stats.set_value("run_validation/http_404_rate", rate_404, spider=spider)
        self.stats.set_value("run_validation/items_per_100_responses", items_per_100, spider=spider)
        if sitemap_cov is not None:
            self.stats.set_value("run_validation/sitemap_coverage", sitemap_cov, spider=spider)

        spider.logger.info(
            "Run summary: responses=%d items=%d non_200=%d 404=%d (rate=%.2f%%) items/100=%.2f%s reason=%s",
            responses,
            items,
            self._counters.non_200,
            http_404,
            rate_404 * 100.0,
            items_per_100,
            (
                f" sitemap_coverage={sitemap_cov * 100.0:.2f}%"
                if sitemap_cov is not None
                else ""
            ),
            reason,
        )

        failures = []
        if responses >= self._min_responses:
            if rate_404 > self._max_404_rate:
                failures.append(f"404_rate={rate_404:.3f} > max={self._max_404_rate:.3f}")
            if items_per_100 < self._min_items_per_100:
                failures.append(
                    f"items_per_100_responses={items_per_100:.2f} < min={self._min_items_per_100:.2f}"
                )
        if sitemap_cov is not None and self._counters.sitemap_locs_total >= 100:
            if sitemap_cov < 0.5:
                failures.append(f"sitemap_coverage={sitemap_cov:.3f} < min=0.500")

        if failures:
            msg = "Run validation failed: " + "; ".join(failures)
            self.stats.set_value("validation_failed", True, spider=spider)
            self.stats.set_value("validation_failed_reason", msg, spider=spider)
            spider.logger.error(msg)
