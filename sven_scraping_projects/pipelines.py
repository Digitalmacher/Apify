import threading
from itemadapter import ItemAdapter
from twisted.internet import reactor

from apify import Actor


def _normalize_for_dataset(obj):
    """
    Make item JSON-schema safe for Apify dataset: no None (use ""),
    only JSON-serializable types. Avoids 'Schema validation failed' when
    different spiders (e.g. KVHH) send null or different shapes.
    """
    if obj is None:
        return ""
    if isinstance(obj, dict):
        return {k: _normalize_for_dataset(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_for_dataset(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)


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
        # Normalize so all spiders pass Apify dataset schema (no None → use "")
        self.items.append(_normalize_for_dataset(item_dict))

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
        from twisted.python.failure import Failure
        import asyncio

        d = Deferred()
        items_to_push = list(self.items)
        done_called = []
        timeout_handle_ref = []

        def _done(exception):
            if done_called:
                return
            done_called.append(True)
            if timeout_handle_ref and timeout_handle_ref[0].active():
                timeout_handle_ref[0].cancel()
            if exception is not None:
                d.errback(Failure(exception))
            else:
                d.callback(None)
            if self._apify_available:
                Actor.log.info(f'ApifyPipeline: Spider {spider.name} closed')

        # Safety: ensure we never hang forever if push stalls (e.g. network)
        PUSH_TIMEOUT_SEC = 7200  # 2 hours
        timeout_handle_ref.append(
            reactor.callLater(
                PUSH_TIMEOUT_SEC,
                lambda: _done(TimeoutError(f'Apify push did not complete within {PUSH_TIMEOUT_SEC}s'))
            )
        )

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
            reactor.callFromThread(_done, err)

        Actor.log.info(f'ApifyPipeline: Pushing {len(items_to_push)} items to Apify dataset...')
        thread = threading.Thread(target=run_push_in_thread, daemon=False)
        thread.start()
        return d
