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
def run_spider(spider_name, settings):
    runner = CrawlerRunner(settings)
    yield runner.crawl(spider_name)
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

    spider_name = 'uke' 
    
    try:
        spider_name = input_data.get('spider_name', spider_name)
    except Exception as e:
        log.warning('Could not read "spider_name" from input; using fallback. Error: %s', e)
        spider_name = os.environ.get('APIFY_INPUT_SPIDER_NAME', spider_name)
    
    if actor_initialized:
        Actor.log.info(f'Running spider: {spider_name}')
    else:
        log.info('Running spider: %s', spider_name)
    
    if actor_initialized:
        Actor.log.info('Loading Scrapy project settings...')
    else:
        log.info('Loading Scrapy project settings...')
    settings = get_project_settings()
    

    if actor_initialized:
        Actor.log.info('Starting spider (scheduling crawl)...')
    else:
        log.info('Starting spider (scheduling crawl)...')

    d = run_spider(spider_name, settings)

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
        Actor.log.info(f'Spider {spider_name} completed successfully')
    else:
        log.info('Spider %s completed successfully', spider_name)
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
