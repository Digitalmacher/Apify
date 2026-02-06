# Plan: Running UKE + Apotheker Kammer Spiders on Apify

## Research Summary

### Apify-Specific Findings

1. **Single Actor, Multiple Spiders**
   - Apify's Scrapy integration runs one Actor per execution. To run multiple spiders, we run them **sequentially** within the same Actor using Scrapy's `CrawlerRunner`.
   - The Apify docs show single-spider examples; multi-spider is achieved by chaining `runner.crawl()` calls.

2. **Data Storage**
   - `Actor.push_data(item)` appends each item to the Actor's **default dataset**.
   - All spiders can push to the same dataset; items accumulate automatically.
   - No merge step needed—each `push_data` call adds one record to the run's output dataset.

3. **Pipeline Behavior**
   - Each spider gets its own pipeline instance. On `close_spider`, the pipeline pushes its collected items via `Actor.push_data()`.
   - Running spiders sequentially ensures clean per-spider push cycles.

4. **Scrapy Multi-Spider Pattern**
   - Use `@defer.inlineCallbacks` and `yield runner.crawl(spider_name)` in a loop.
   - Call `reactor.stop()` only after **all** spiders complete.

### Current Codebase

- **Spiders**: `uke` (UKE.de doctor profiles), `apothekerkammer-hamburg` (Apotheker Kammer Hamburg pharmacy finder)
- **Entry point**: `src/main.py` — reads `spider_name` from input, runs one spider
- **Pipeline**: `ApifyPipeline` — collects items per spider, pushes to Apify dataset on `close_spider`

---

## Implementation Plan

### 1. Input Schema

Support both single-spider and multi-spider modes:

| Input Field | Type | Default | Description |
|-------------|------|---------|-------------|
| `spider_names` | array of strings | `["uke", "apothekerkammer-hamburg"]` | List of spiders to run (in order) |
| `spider_name` | string | (legacy) | If provided and `spider_names` absent, run only this spider |

**Backward compatibility**: If `spider_name` is set and `spider_names` is not, use `[spider_name]`.

### 2. Main Runner Changes (`src/main.py`)

- Replace single `run_spider(spider_name)` with `run_spiders(spider_names)`.
- Loop over `spider_names` and `yield runner.crawl(name)` for each.
- Stop reactor only after all crawls complete.
- Log start/end for each spider.

### 3. Pipeline Enhancement (`sven_scraping_projects/pipelines.py`)

- Add `source` (or `spider`) to each item before pushing, so the dataset clearly identifies which spider produced each record.
- Example: `{"source": "uke", "name": "...", ...}` and `{"source": "apothekerkammer-hamburg", "name": "...", ...}`.

### 4. Apify Actor Definition (Optional)

- If using `apify.json` or `.actor/actor.json`, add input schema for `spider_names` with enum `["uke", "apothekerkammer-hamburg"]` and default `["uke", "apothekerkammer-hamburg"]`.

---

## Execution Flow

```
Actor starts
  → Actor.init(), Actor.get_input()
  → Parse spider_names (default: ["uke", "apothekerkammer-hamburg"])
  → For each spider_name in spider_names:
       → runner.crawl(spider_name)
       → Spider runs, ApifyPipeline collects items
       → close_spider → push all items to dataset (with source field)
  → reactor.stop()
  → Actor.exit()
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/main.py` | Multi-spider loop, input parsing for `spider_names` |
| `sven_scraping_projects/pipelines.py` | Add `source` field to each item |

---

## Testing

- **Local**: `python -m src` with env `APIFY_INPUT_KEY` or input JSON containing `spider_names`.
- **Apify**: Run Actor with default input (both spiders) or override `spider_names` to run only one.
