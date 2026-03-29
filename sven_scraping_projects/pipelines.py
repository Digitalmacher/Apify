import json
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


def _split_csvish(value):
    """
    Convert comma-separated (or already-list) values into a clean list of strings.
    Keeps stable output shape for downstream consumers.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if not isinstance(value, str):
        value = str(value)
    s = value.strip()
    if not s:
        return []
    # Many spiders join with ", " already. Avoid splitting URLs by only splitting on commas.
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _first_non_empty(*values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, str):
            if v.strip():
                return v.strip()
            continue
        return v
    return ""


def _canonicalize_item(item_dict):
    """
    Map spider-specific keys into a canonical schema.

    - Keeps provenance (`source`, `source_url`)
    - Merges synonymous fields (phone/telephone, website/internet, specialty variants, etc.)
    - Preserves original payload in `raw_source_fields` for auditability
    """
    source = (item_dict.get("source") or "").strip()

    # Basic identity / provenance
    source_url = _first_non_empty(item_dict.get("url"), item_dict.get("source_url"))
    display_name = _first_non_empty(item_dict.get("display_name"), item_dict.get("name"))

    name_title = _first_non_empty(item_dict.get("name_title"), item_dict.get("title"))
    first_name = _first_non_empty(item_dict.get("first_name"))
    last_name = _first_non_empty(item_dict.get("last_name"))

    # Contact
    phone = _first_non_empty(item_dict.get("phone"), item_dict.get("telephone"))
    fax = _first_non_empty(item_dict.get("fax"))
    email = _first_non_empty(item_dict.get("email"))
    website_url = _first_non_empty(item_dict.get("website"), item_dict.get("internet"), item_dict.get("website_url"))

    # Location
    address_freeform = _first_non_empty(item_dict.get("address"), item_dict.get("address_freeform"))
    street = _first_non_empty(item_dict.get("street"))
    postal_code = _first_non_empty(item_dict.get("postal_code"), item_dict.get("zip"))
    city = _first_non_empty(item_dict.get("city"))
    location_freeform = _first_non_empty(item_dict.get("location"), item_dict.get("location_freeform"))

    # Role / org
    job_title = _first_non_empty(item_dict.get("job_title"), item_dict.get("position"))
    department_or_unit = _first_non_empty(
        item_dict.get("department_or_unit"),
        item_dict.get("department"),
        item_dict.get("einrichtung"),
    )
    practice_name = _first_non_empty(item_dict.get("practice_name"))
    practice_relation = _first_non_empty(item_dict.get("practice_relation"))

    # Domain fields (specialty / expertise / services)
    primary_specialty = _first_non_empty(item_dict.get("primary_specialty"), item_dict.get("specialty"))
    responsibility_area = _first_non_empty(
        item_dict.get("responsibility_area"),
        item_dict.get("area_of_responsibility"),
    )

    # Collect specialties-like signals into a list (deduped, stable order)
    specialties_list = []
    for v in (
        primary_specialty,
        item_dict.get("specialization"),
        item_dict.get("specialties"),
        item_dict.get("areas_of_expertise"),
    ):
        specialties_list.extend(_split_csvish(v))
    # Some spiders misuse position/area_of_work as specialty labels; normalize per source below.

    services_or_focus = []
    for v in (
        item_dict.get("services_or_focus_areas"),
        item_dict.get("main_areas_of_activity"),
        item_dict.get("areas_of_activity"),
    ):
        services_or_focus.extend(_split_csvish(v))

    languages = _split_csvish(item_dict.get("languages"))

    # Extras
    image_url = _first_non_empty(item_dict.get("image_url"), item_dict.get("img_url"))
    career_highlights = _first_non_empty(item_dict.get("career_highlights"))
    llm_content = _first_non_empty(item_dict.get("llm_content"))

    memberships = []
    memberships.extend(_split_csvish(item_dict.get("memberships")))
    memberships.extend(_split_csvish(item_dict.get("field_membership")))

    affiliated_facilities = []
    if item_dict.get("affiliated_facilities"):
        affiliated_facilities.extend(_split_csvish(item_dict.get("affiliated_facilities")))
    else:
        # Asklepios legacy: clinic_1/clinic_2 are multi-line strings; keep as-is entries.
        for k in ("clinic_1", "clinic_2"):
            v = item_dict.get(k)
            if isinstance(v, str) and v.strip():
                affiliated_facilities.append(v.strip())

    # Source-specific fixes to prevent semantic mixing
    if source == "kvhh":
        # kvhh sets multiple synonymous keys to the same Fachgebiet.
        kvhh_specialty = _first_non_empty(
            item_dict.get("specialization"),
            item_dict.get("position"),
            item_dict.get("area_of_work"),
            item_dict.get("department"),
        )
        if kvhh_specialty:
            primary_specialty = kvhh_specialty
            specialties_list = _split_csvish(kvhh_specialty)
        # kvhh "Leistungen" are services/focus areas
        services_or_focus = _split_csvish(item_dict.get("main_areas_of_activity"))

    if source == "zahnaerzte_hh":
        # specialization is effectively their main specialty list.
        if not primary_specialty:
            primary_specialty = _first_non_empty(item_dict.get("specialization"))
        if item_dict.get("specialization"):
            specialties_list = _split_csvish(item_dict.get("specialization"))
        # They also set position/area_of_work/main_areas_of_activity to same value; treat as specialty list already.
        if not services_or_focus:
            services_or_focus = _split_csvish(item_dict.get("main_areas_of_activity"))

    if source == "uke":
        # UKE work_area is usually contact/location-like; do NOT treat as specialty.
        # Keep it as department/unit if department missing, else preserve in raw.
        if not department_or_unit:
            department_or_unit = _first_non_empty(item_dict.get("work_area"))

    if source == "asklepios":
        # Asklepios specialty is the medical specialty; position is job title.
        if not primary_specialty:
            primary_specialty = _first_non_empty(item_dict.get("specialty"))

    # Deduplicate list fields while preserving order
    def _dedupe(seq):
        seen = set()
        out = []
        for x in seq:
            x = (x or "").strip() if isinstance(x, str) else str(x).strip()
            if not x or x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out

    specialties = _dedupe(specialties_list)
    services_or_focus_areas = _dedupe(services_or_focus)
    languages = _dedupe(languages)
    memberships = _dedupe(memberships)
    affiliated_facilities = _dedupe(affiliated_facilities)

    # Entity type inference (minimal, consistent with current sources)
    entity_type = _first_non_empty(item_dict.get("entity_type"))
    if not entity_type:
        if source in {"apothekerkammer-hamburg"}:
            entity_type = "organization"
        else:
            entity_type = "person"

    canonical = {
        # Provenance
        "source": source,
        "source_url": source_url,

        # Entity
        "entity_type": entity_type,
        "display_name": display_name,
        "name_title": name_title,
        "first_name": first_name,
        "last_name": last_name,

        # Contact
        "phone": phone,
        "fax": fax,
        "email": email,
        "website_url": website_url,

        # Location
        "address_freeform": address_freeform,
        "street": street,
        "postal_code": postal_code,
        "city": city,
        "location_freeform": location_freeform,

        # Org/role
        "job_title": job_title,
        "department_or_unit": department_or_unit,
        "practice_name": practice_name,
        "practice_relation": practice_relation,

        # Domain
        "primary_specialty": primary_specialty,
        "specialties": specialties,
        "services_or_focus_areas": services_or_focus_areas,
        "responsibility_area": responsibility_area,
        "languages": languages,
        "memberships": memberships,

        # Extras
        "image_url": image_url,
        "career_highlights": career_highlights,
        "affiliated_facilities": affiliated_facilities,
        "llm_content": llm_content,

        # Audit / debugging escape hatch
        "raw_source_fields": dict(item_dict),
    }

    # Drop empty-string-only keys? Keep as-is: dataset consumers may prefer stable keys.
    return canonical


def _flatten_for_apify_dataset_schema(item):
    """
    Apify dataset schema in .actor/actor.json types several keys as `string`
    (e.g. specialties, languages). Arrays and nested objects fail validation.

    - list -> comma-separated string
    - dict (e.g. raw_source_fields) -> JSON string
    """
    out = {}
    for k, v in item.items():
        if k == "raw_source_fields" and isinstance(v, dict):
            out[k] = json.dumps(_normalize_for_dataset(v), ensure_ascii=False)
        elif isinstance(v, list):
            parts = []
            for x in v:
                if x is None:
                    continue
                s = str(x).strip()
                if s:
                    parts.append(s)
            out[k] = ", ".join(parts)
        elif isinstance(v, dict):
            out[k] = json.dumps(_normalize_for_dataset(v), ensure_ascii=False)
        else:
            out[k] = v
    return out


def _add_legacy_dataset_aliases(rec):
    """
    Original actor schema + UI views expect name, url, telephone alongside
    canonical keys. Duplicate here so validation and table views stay populated.
    """
    if not isinstance(rec, dict):
        return rec
    out = dict(rec)
    # Legacy / overview view fields (same semantics as canonical)
    if "name" not in out or out.get("name") == "":
        out["name"] = out.get("display_name") or ""
    if "url" not in out or out.get("url") == "":
        out["url"] = out.get("source_url") or ""
    if "telephone" not in out or out.get("telephone") == "":
        out["telephone"] = out.get("phone") or ""
    if "title" not in out or out.get("title") == "":
        out["title"] = out.get("name_title") or ""
    if "department" not in out or out.get("department") == "":
        out["department"] = out.get("department_or_unit") or ""
    if "work_area" not in out or out.get("work_area") == "":
        out["work_area"] = out.get("location_freeform") or ""
    if "location" not in out or out.get("location") == "":
        out["location"] = out.get("location_freeform") or ""
    if "areas_of_expertise" not in out or out.get("areas_of_expertise") == "":
        out["areas_of_expertise"] = out.get("specialties") or ""
    if "areas_of_activity" not in out or out.get("areas_of_activity") == "":
        out["areas_of_activity"] = out.get("services_or_focus_areas") or ""
    if "website" not in out or out.get("website") == "":
        out["website"] = out.get("website_url") or ""
    if "address" not in out or out.get("address") == "":
        out["address"] = out.get("address_freeform") or ""
    return out


def _stringify_apify_dataset_record(rec):
    """
    Apify dataset validation expects string-typed fields (see .actor/actor.json).
    Spiders sometimes yield ints (e.g. PLZ as number from JSON); _normalize_for_dataset
    preserves int/float/bool, which still fails schema validation.
    """
    if not isinstance(rec, dict):
        return rec
    out = {}
    for k, v in rec.items():
        if v is None:
            out[k] = ""
        elif isinstance(v, str):
            out[k] = v
        elif isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        else:
            out[k] = str(v)
    return out


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
        # Canonicalize + normalize so all spiders share one schema
        canonical = _canonicalize_item(item_dict)
        flattened = _flatten_for_apify_dataset_schema(canonical)
        normalized = _normalize_for_dataset(flattened)
        with_aliases = _add_legacy_dataset_aliases(normalized)
        self.items.append(_stringify_apify_dataset_record(with_aliases))

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
