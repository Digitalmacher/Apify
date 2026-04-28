import os
import asyncio
import logging
import sys
import signal
import threading
import concurrent.futures
import copy
from apify import Actor, Configuration
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python.failure import Failure
from sven_scraping_projects.apify_runtime import set_actor_loop


@inlineCallbacks
def run_spiders(spider_names, settings, log_fn=None):
    """Run multiple spiders sequentially. All results are pushed to the same Apify dataset."""
    for i, name in enumerate(spider_names):
        # Persist scheduler/dupefilter state per spider so Apify migrations (SIGTERM)
        # can restart the container and continue the crawl instead of losing progress.
        #
        # NOTE: Each spider gets its own JOBDIR to avoid cross-spider state collisions.
        jobdir_root = os.environ.get("SCRAPY_JOBDIR_ROOT") or os.path.join("storage", "scrapy_jobdir")
        jobdir = os.path.join(jobdir_root, name)
        # Scrapy's Settings.copy() performs a deepcopy. We intentionally store a live asyncio
        # event loop in settings (APIFY_ACTOR_LOOP) so pipelines can run Actor SDK coroutines
        # on a shared background loop. Event loops (and related asyncio internals) are not
        # deepcopy/pickle-safe, so we must temporarily strip them before copying.
        actor_loop = None
        try:
            actor_loop = settings.get("APIFY_ACTOR_LOOP")
        except Exception:
            actor_loop = None

        try:
            if actor_loop is not None:
                settings.set("APIFY_ACTOR_LOOP", None, priority="cmdline")
            spider_settings = settings.copy()
        except Exception:
            # Best-effort diagnostics: pinpoint which value breaks deepcopy to avoid future regressions.
            suspects = []
            try:
                for k in list(settings.keys()):
                    try:
                        copy.deepcopy(settings.get(k))
                    except Exception:
                        suspects.append(k)
            except Exception:
                pass
            raise RuntimeError(
                "Failed to copy Scrapy settings (deepcopy). "
                + (f"Non-deepcopyable keys: {suspects!r}" if suspects else "")
            )
        finally:
            if actor_loop is not None:
                try:
                    settings.set("APIFY_ACTOR_LOOP", actor_loop, priority="cmdline")
                except Exception:
                    pass

        if actor_loop is not None:
            spider_settings.set("APIFY_ACTOR_LOOP", actor_loop, priority="cmdline")
        spider_settings.set("JOBDIR", jobdir, priority="cmdline")
        spider_settings.set("SCHEDULER_PERSIST", True, priority="cmdline")

        runner = CrawlerRunner(spider_settings)
        if log_fn:
            log_fn(f"Scraper {i + 1}/{len(spider_names)} ready: starting '{name}'")
        yield runner.crawl(name)
        if log_fn:
            log_fn(f"Scraper '{name}' finished (crawl completed, next will start if any)")
    reactor.stop()

def main():

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    log = logging.getLogger(__name__)
    received_sigterm = {"value": False}

    print(f"BOOT: src.main from={__file__}", flush=True)
    print(f"BOOT: SCRAPY_SETTINGS_MODULE={os.environ.get('SCRAPY_SETTINGS_MODULE')}", flush=True)
    print("BOOT: sys.path[0:5]=" + repr(sys.path[0:5]), flush=True)
    try:
        import sven_scraping_projects as ssp  
        print(f"BOOT: sven_scraping_projects from={ssp.__file__}", flush=True)
    except Exception as e:
        print(f"BOOT: could not import sven_scraping_projects: {e!r}", flush=True)

    # Run all Apify SDK async calls on a single dedicated asyncio loop.
    # Scrapy/Twisted reactor runs in the main thread; we keep the asyncio loop alive in a background thread.
    actor_loop = asyncio.new_event_loop()
    actor_loop_thread = threading.Thread(target=actor_loop.run_forever, name="apify-actor-loop", daemon=True)
    actor_loop_thread.start()
    set_actor_loop(actor_loop)

    def _run_on_actor_loop(coro, *, timeout: float | None = None):
        fut: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(coro, actor_loop)
        return fut.result(timeout=timeout)

    # We do not rely on platform events (MIGRATING/PERSIST_STATE listeners).
    # Explicitly disable the events websocket to avoid noisy shutdown errors.
    actor = Actor(configuration=Configuration(actor_events_ws_url=None))

    async def init_and_get_input():
        await actor.init()
        return await actor.get_input() or {}

    input_data = {}
    actor_initialized = False
    try:
        input_data = _run_on_actor_loop(init_and_get_input(), timeout=120)
        actor_initialized = True
    except Exception as e:
        log.exception("Apify Actor.init() / Actor.get_input() failed; falling back to env. Error: %s", e)

    if actor_initialized:
        try:
            import apify as _apify_pkg
            actor.log.info("BOOT: apify_version=%s", getattr(_apify_pkg, "__version__", "unknown"))
        except Exception:
            actor.log.info("BOOT: apify_version=unknown (import failed)")
        try:
            import websockets as _websockets_pkg
            actor.log.info("BOOT: websockets_version=%s", getattr(_websockets_pkg, "__version__", "unknown"))
        except Exception:
            actor.log.info("BOOT: websockets_version=unknown (import failed)")
        try:
            actor.log.info("BOOT: actor_events_ws_url=%r", actor.configuration.actor_events_ws_url)
        except Exception:
            actor.log.info("BOOT: actor_events_ws_url=<unavailable>")
        actor.log.info('Starting Scrapy spider on Apify...')
    else:
        log.info('Starting Scrapy spider (Apify SDK not initialized; using fallbacks)...')

    # Default: run UKE, Apotheker Kammer, Asklepios, KVHH, then Zahnärzte (KZV Hamburg)
    default_spiders = ['uke', 'apothekerkammer-hamburg', 'asklepios', 'kvhh', 'zahnaerzte_hh']
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
        actor.log.info(f'Running spiders: {spider_names}')
    else:
        log.info('Running spiders: %s', spider_names)
    
    if actor_initialized:
        actor.log.info('Loading Scrapy project settings...')
    else:
        log.info('Loading Scrapy project settings...')
    settings = get_project_settings()
    # Make the Apify loop available to Scrapy components (e.g. pipeline) without putting it into
    # Scrapy settings (Scrapy deep-copies settings in multiple places and asyncio loops are not deepcopy-safe).

    try:
        max_items = input_data.get("max_items")
        if isinstance(max_items, int) and max_items > 0:
            settings.set("CLOSESPIDER_ITEMCOUNT", max_items, priority="cmdline")
            if actor_initialized:
                actor.log.info(f"Configured CLOSESPIDER_ITEMCOUNT={max_items}")
            else:
                log.info("Configured CLOSESPIDER_ITEMCOUNT=%s", max_items)

        max_pages = input_data.get("max_pages")
        if isinstance(max_pages, int) and max_pages > 0:
            settings.set("CLOSESPIDER_PAGECOUNT", max_pages, priority="cmdline")
            if actor_initialized:
                actor.log.info(f"Configured CLOSESPIDER_PAGECOUNT={max_pages}")
            else:
                log.info("Configured CLOSESPIDER_PAGECOUNT=%s", max_pages)

        close_spider_timeout_secs = input_data.get("close_spider_timeout_secs")
        if isinstance(close_spider_timeout_secs, (int, float)) and close_spider_timeout_secs > 0:
            settings.set("CLOSESPIDER_TIMEOUT", close_spider_timeout_secs, priority="cmdline")
            if actor_initialized:
                actor.log.info(f"Configured CLOSESPIDER_TIMEOUT={close_spider_timeout_secs}s")
            else:
                log.info("Configured CLOSESPIDER_TIMEOUT=%ss", close_spider_timeout_secs)
        elif actor_initialized:
            actor.log.info(
                "No close_spider_timeout_secs set - spider will run until completion. "
                "To scrape all results, increase the timeout in Apify Run options "
                "(default is 300s; increase to 600s+ for large datasets)."
            )
    except Exception as e:
        if actor_initialized:
            actor.log.warning(f"Failed applying CLOSESPIDER_* settings from input: {e}")
        else:
            log.warning("Failed applying CLOSESPIDER_* settings from input: %s", e)
    
    # Best-effort graceful shutdown on SIGTERM (Apify migrations / preemption).
    # Scrapy + Twisted will still be interrupted, but this increases the chance
    # pipelines flush and stats are emitted before the container is stopped.
    def _handle_sigterm(signum, frame):
        received_sigterm["value"] = True
        try:
            if actor_initialized:
                Actor.log.warning("Received SIGTERM (likely migration). Attempting graceful shutdown...")
            else:
                log.warning("Received SIGTERM. Attempting graceful shutdown...")
        except Exception:
            pass
        try:
            if reactor.running:
                reactor.callFromThread(reactor.stop)
        except Exception:
            pass

    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
    except Exception:
        # Some restricted environments may not allow installing signal handlers.
        pass


    if actor_initialized:
        actor.log.info('Starting spider (scheduling crawl)...')
    else:
        log.info('Starting spider (scheduling crawl)...')

    def _log(msg):
        if actor_initialized:
            actor.log.info(msg)
        else:
            log.info(msg)

    d = run_spiders(spider_names, settings, log_fn=_log)

    had_error = {"value": False}

    def _on_error(f: Failure):
        had_error["value"] = True
        tb = f.getTraceback()
        if actor_initialized:
            actor.log.error(f"Spider crashed:\n{tb}")
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
        actor.log.info('Running Twisted reactor...')
    else:
        log.info('Running Twisted reactor...')
    reactor.run()
    
    exit_code = 0
    # IMPORTANT: Apify migrations deliver SIGTERM. We persist crawl state via JOBDIR and
    # SCHEDULER_PERSIST, so the correct behavior is to exit promptly WITHOUT calling
    # Actor.exit() (which would mark the run as successfully finished and prevent resume).
    if received_sigterm["value"]:
        exit_code = 143  # conventional exit code for SIGTERM
        if actor_initialized:
            actor.log.warning("Run interrupted by SIGTERM (migration/preemption). Exiting to allow restart/resume.")
        else:
            log.warning("Run interrupted by SIGTERM (migration/preemption). Exiting to allow restart/resume.")
    elif had_error["value"]:
        exit_code = 1
        if actor_initialized:
            actor.log.error("Run failed (one or more spiders crashed).")
        else:
            log.error("Run failed (one or more spiders crashed).")
    else:
        if actor_initialized:
            actor.log.info(f"Spiders {spider_names} completed successfully")
        else:
            log.info("Spiders %s completed successfully", spider_names)
    try:
        if actor_initialized and not received_sigterm["value"]:
            try:
                _run_on_actor_loop(actor.exit(), timeout=180)
            except Exception as e:
                # Do not fail the run just because internal platform event websocket cleanup failed.
                log.warning('Could not shut down Apify Actor cleanly (ignored): %s', e, exc_info=True)
    except Exception as e:
        log.warning('Could not shut down Apify Actor cleanly: %s', e)
    finally:
        try:
            try:
                set_actor_loop(None)
            except Exception:
                pass
            try:
                if actor_loop.is_running():
                    actor_loop.call_soon_threadsafe(actor_loop.stop)
            except Exception:
                try:
                    actor_loop.call_soon_threadsafe(actor_loop.stop)
                except Exception:
                    pass

            try:
                actor_loop_thread.join(timeout=15)
            except Exception:
                pass
        finally:
            try:
                if not actor_loop.is_closed():
                    actor_loop.close()
            except Exception:
                pass

    raise SystemExit(exit_code)


if __name__ == '__main__':
    main()
