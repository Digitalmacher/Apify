import threading
from itemadapter import ItemAdapter
from twisted.internet import reactor

from apify import Actor


class ApifyPipeline:

    def __init__(self):
        self.items = []
        self._apify_available = False
    
    def open_spider(self, spider):
        # Check if Apify Actor is available
        try:
            self._apify_available = True
            Actor.log.info(f'ApifyPipeline: Spider {spider.name} opened')
        except Exception:
            self._apify_available = False
            import logging
            logging.warning('ApifyPipeline: Apify Actor not available, items will be collected but not pushed')
        
        self.items = []
    
    def process_item(self, item, spider):

        if hasattr(item, 'asdict'):
            item_dict = item.asdict()
        elif isinstance(item, dict):
            item_dict = dict(item)
        else:
            adapter = ItemAdapter(item)
            item_dict = dict(adapter)

        # Add source so we can identify which spider produced each record
        item_dict['source'] = spider.name
        self.items.append(item_dict)

        return item
    
    def close_spider(self, spider):
        if not self.items:
            if self._apify_available:
                Actor.log.info(f'ApifyPipeline: No items to push for spider {spider.name}')
            return

        if not self._apify_available:
            import logging
            logging.warning(f'ApifyPipeline: {len(self.items)} items collected but Apify Actor not available')
            return

        # Run async push in a thread and return a Deferred so we don't block the Twisted
        # reactor. Blocking here was preventing the crawl() Deferred from firing and
        # the next spider from starting (dormant state).
        from twisted.internet.defer import Deferred
        import asyncio

        d = Deferred()
        items_to_push = list(self.items)

        def run_push_in_thread():
            err = None
            try:
                async def push_all_items():
                    for item in items_to_push:
                        await Actor.push_data(item)

                asyncio.run(push_all_items())
                if self._apify_available:
                    Actor.log.info(f'ApifyPipeline: Successfully pushed {len(items_to_push)} items to dataset')
            except Exception as e:
                err = e
                if self._apify_available:
                    Actor.log.error(f'ApifyPipeline: Error pushing items to dataset: {e}')
            # Signal completion on the reactor thread so the crawler can finish
            reactor.callFromThread(_done, err)

        def _done(exception):
            if exception is not None:
                from twisted.python.failure import Failure
                d.errback(Failure(exception))
            else:
                d.callback(None)
            if self._apify_available:
                Actor.log.info(f'ApifyPipeline: Spider {spider.name} closed')

        Actor.log.info(f'ApifyPipeline: Pushing {len(items_to_push)} items to Apify dataset...')
        thread = threading.Thread(target=run_push_in_thread, daemon=False)
        thread.start()
        return d
