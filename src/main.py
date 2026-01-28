import os
import asyncio
import logging
from apify import Actor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks


@inlineCallbacks
def run_spider(spider_name, settings):
    runner = CrawlerRunner(settings)
    yield runner.crawl(spider_name)
    reactor.stop()

def main():

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    log = logging.getLogger(__name__)

    async def init_and_get_input():
        await Actor.init()
        return await Actor.get_input() or {}

    input_data = {}
    actor_initialized = False
    try:
        input_data = asyncio.run(init_and_get_input())
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
    
    settings = get_project_settings()
    

    run_spider(spider_name, settings)
    reactor.run()
    
    if actor_initialized:
        Actor.log.info(f'Spider {spider_name} completed successfully')
    else:
        log.info('Spider %s completed successfully', spider_name)
    try:
        if actor_initialized:
            asyncio.run(Actor.exit())
    except Exception as e:
        log.warning('Could not shut down Apify Actor cleanly: %s', e)


if __name__ == '__main__':
    main()
