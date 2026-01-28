# Scrapy settings for sven_scraping_projects project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "sven_scraping_projects"

SPIDER_MODULES = ["sven_scraping_projects.spiders"]
NEWSPIDER_MODULE = "sven_scraping_projects.spiders"

ADDONS = {}

# Scrapy 2.14+ defaults to the asyncio-based reactor. This project integrates the
# Apify SDK and runs Scrapy via Twisted, so we force a "classic" Twisted reactor.
#
# IMPORTANT: The reactor must match what Twisted installs on the platform.
# - Linux typically uses EPollReactor
# - macOS/Windows typically use SelectReactor
import sys

if sys.platform.startswith("linux"):
    TWISTED_REACTOR = "twisted.internet.epollreactor.EPollReactor"
else:
    TWISTED_REACTOR = "twisted.internet.selectreactor.SelectReactor"

# Emit progress logs frequently so Apify runs don't look "stuck" during long downloads.
LOGSTATS_INTERVAL = 10
TELNETCONSOLE_ENABLED = False

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'

# Obey robots.txt rules
# ROBOTSTXT_OBEY = False

# # Concurrency and throttling settings
# CONCURRENT_REQUESTS = 16
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# DOWNLOAD_DELAY = 0.5

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "sven_scraping_projects.middlewares.SvenScrapingProjectsSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "sven_scraping_projects.middlewares.SvenScrapingProjectsDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "sven_scraping_projects.pipelines.ApifyPipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"
