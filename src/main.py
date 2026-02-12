import os
import asyncio
import logging
import sys
from apify import Actor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python.failure import Failure


@inlineCallbacks
def run_spiders(spider_names, settings, log_fn=None):
    """Run multiple spiders sequentially. All results are pushed to the same Apify dataset."""
    runner = CrawlerRunner(settings)
    for i, name in enumerate(spider_names):
        if log_fn:
            log_fn(f"Scraper {i + 1}/{len(spider_names)} ready: starting '{name}'")
        yield runner.crawl(name)
        if log_fn:
            log_fn(f"Scraper '{name}' finished (crawl completed, next will start if any)")
    reactor.stop()

def main():

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    log = logging.getLogger(__name__)

    print(f"BOOT: src.main from={__file__}", flush=True)
    print(f"BOOT: SCRAPY_SETTINGS_MODULE={os.environ.get('SCRAPY_SETTINGS_MODULE')}", flush=True)
    print("BOOT: sys.path[0:5]=" + repr(sys.path[0:5]), flush=True)
    try:
        import sven_scraping_projects as ssp  
        print(f"BOOT: sven_scraping_projects from={ssp.__file__}", flush=True)
    except Exception as e:
        print(f"BOOT: could not import sven_scraping_projects: {e!r}", flush=True)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def init_and_get_input():
        await Actor.init()
        return await Actor.get_input() or {}

    input_data = {}
    actor_initialized = False
    try:
        input_data = loop.run_until_complete(init_and_get_input())
        actor_initialized = True
    except Exception as e:
        log.exception("Apify Actor.init() / Actor.get_input() failed; falling back to env. Error: %s", e)

    if actor_initialized:
        Actor.log.info('Starting Scrapy spider on Apify...')
    else:
        log.info('Starting Scrapy spider (Apify SDK not initialized; using fallbacks)...')

    # Default: run UKE, then Apotheker Kammer, then KVHH (last)
    default_spiders = ['uke', 'apothekerkammer-hamburg', 'kvhh']
    spider_names = default_spiders

    try:
        if 'spider_names' in input_data and input_data['spider_names']:
            spider_names = list(input_data['spider_names'])
        elif 'spider_name' in input_data and input_data['spider_name']:
            # Legacy: single spider_name
            spider_names = [input_data['spider_name']]
    except Exception as e:
        log.warning('Could not read spider config from input; using env or default. Error: %s', e)
        env_val = os.environ.get('APIFY_INPUT_SPIDER_NAMES', ','.join(default_spiders))
        spider_names = [n.strip() for n in env_val.split(',') if n.strip()]
    if not spider_names:
        spider_names = default_spiders

    if actor_initialized:
        Actor.log.info(f'Running spiders: {spider_names}')
    else:
        log.info('Running spiders: %s', spider_names)
    
    if actor_initialized:
        Actor.log.info('Loading Scrapy project settings...')
    else:
        log.info('Loading Scrapy project settings...')
    settings = get_project_settings()

    try:
        max_items = input_data.get("max_items")
        if isinstance(max_items, int) and max_items > 0:
            settings.set("CLOSESPIDER_ITEMCOUNT", max_items, priority="cmdline")
            if actor_initialized:
                Actor.log.info(f"Configured CLOSESPIDER_ITEMCOUNT={max_items}")
            else:
                log.info("Configured CLOSESPIDER_ITEMCOUNT=%s", max_items)

        max_pages = input_data.get("max_pages")
        if isinstance(max_pages, int) and max_pages > 0:
            settings.set("CLOSESPIDER_PAGECOUNT", max_pages, priority="cmdline")
            if actor_initialized:
                Actor.log.info(f"Configured CLOSESPIDER_PAGECOUNT={max_pages}")
            else:
                log.info("Configured CLOSESPIDER_PAGECOUNT=%s", max_pages)

        close_spider_timeout_secs = input_data.get("close_spider_timeout_secs")
        if isinstance(close_spider_timeout_secs, (int, float)) and close_spider_timeout_secs > 0:
            settings.set("CLOSESPIDER_TIMEOUT", close_spider_timeout_secs, priority="cmdline")
            if actor_initialized:
                Actor.log.info(f"Configured CLOSESPIDER_TIMEOUT={close_spider_timeout_secs}s")
            else:
                log.info("Configured CLOSESPIDER_TIMEOUT=%ss", close_spider_timeout_secs)
        elif actor_initialized:
            Actor.log.info(
                "No close_spider_timeout_secs set - spider will run until completion. "
                "To scrape all results, increase the timeout in Apify Run options "
                "(default is 300s; increase to 600s+ for large datasets)."
            )
    except Exception as e:
        if actor_initialized:
            Actor.log.warning(f"Failed applying CLOSESPIDER_* settings from input: {e}")
        else:
            log.warning("Failed applying CLOSESPIDER_* settings from input: %s", e)
    

    if actor_initialized:
        Actor.log.info('Starting spider (scheduling crawl)...')
    else:
        log.info('Starting spider (scheduling crawl)...')

    def _log(msg):
        if actor_initialized:
            Actor.log.info(msg)
        else:
            log.info(msg)

    d = run_spiders(spider_names, settings, log_fn=_log)

    def _on_error(f: Failure):
        tb = f.getTraceback()
        if actor_initialized:
            Actor.log.error(f"Spider crashed:\n{tb}")
        else:
            log.error("Spider crashed:\n%s", tb)
        try:
            if reactor.running:
                reactor.stop()
        except Exception:
            pass
        return f

    d.addErrback(_on_error)

    if actor_initialized:
        Actor.log.info('Running Twisted reactor...')
    else:
        log.info('Running Twisted reactor...')
    reactor.run()
    
    if actor_initialized:
        Actor.log.info(f'Spiders {spider_names} completed successfully')
    else:
        log.info('Spiders %s completed successfully', spider_names)
    try:
        if actor_initialized:
            loop.run_until_complete(Actor.exit())
    except Exception as e:
        log.warning('Could not shut down Apify Actor cleanly: %s', e)
    finally:
        try:
            loop.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
