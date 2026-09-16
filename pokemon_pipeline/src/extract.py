"""
API extraction layer for the Pokémon ETL pipeline.

Responsibilities:
- Create reusable HTTP session
- Handle API requests
- Retry temporary failures
- Discover Pokémon resources
- Extract Pokémon details
- Extract species details
- Track failed requests
"""

import time
import requests
from exceptions import ExtractionError
from config import (
    BASE_URL,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRYABLE_STATUS_CODES
)


# -------------------------------------------------------------------
# HTTP session
# -------------------------------------------------------------------

def create_session():
    """
    Create a reusable HTTP session.

    Sessions allow connection reuse and provide a central place
    to configure common request headers.
    """

    session = requests.Session()

    session.headers.update({
        "Accept": "application/json"
    })

    return session


# -------------------------------------------------------------------
# Generic API request
# -------------------------------------------------------------------

def get_json(session, url, logger):
    """
    Request JSON data from an API endpoint.

    Temporary failures such as timeouts, connection errors,
    rate limiting, and server errors are retried with
    exponential backoff.

    Returns:
        dict/list: Parsed JSON response on success.
        None: If the request ultimately fails.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            response.raise_for_status()

            return response.json()

        except requests.exceptions.Timeout:

            logger.warning(
                "Request timeout | attempt=%s/%s | url=%s",
                attempt,
                MAX_RETRIES,
                url
            )

        except requests.exceptions.ConnectionError:

            logger.warning(
                "Connection error | attempt=%s/%s | url=%s",
                attempt,
                MAX_RETRIES,
                url
            )

        except requests.exceptions.HTTPError:

            status_code = response.status_code

            if status_code not in RETRYABLE_STATUS_CODES:

                logger.error(
                    "Permanent HTTP error | status=%s | url=%s",
                    status_code,
                    url
                )

                return None

            logger.warning(
                "Retryable HTTP error | status=%s | attempt=%s/%s | url=%s",
                status_code,
                attempt,
                MAX_RETRIES,
                url
            )

        except requests.exceptions.RequestException as error:

            logger.error(
                "Request failed | error=%s | url=%s",
                error,
                url
            )

            return None

        # Don't wait after the final attempt.
        if attempt < MAX_RETRIES:

            wait_time = 2 ** (attempt - 1)

            logger.info(
                "Retrying request | wait=%ss",
                wait_time
            )

            time.sleep(wait_time)

    logger.error(
        "Request failed after all retries | url=%s",
        url
    )

    return None

def get_pokemon_urls(session, logger):
    """
    Retrieve all Pokémon resource URLs using API pagination.

    The extraction is considered successful only when pagination
    reaches the end normally.

    Raises:
        ExtractionError: If any pagination request fails.
    """

    url = f"{BASE_URL}/pokemon/?limit=100&offset=0"

    pokemon_urls = []

    while url:

        data = get_json(
            session,
            url,
            logger
        )

        if data is None:

            logger.error(
                "Pokemon pagination failed | url=%s",
                url
            )

            raise ExtractionError(
                f"Unable to retrieve Pokemon page: {url}"
            )

        results = data.get("results")

        if results is None:

            logger.error(
                "Invalid pagination response | missing='results' | url=%s",
                url
            )

            raise ExtractionError(
                "Pokemon API response does not contain 'results'"
            )

        pokemon_urls.extend(
            item["url"]
            for item in results
            if item.get("url")
        )

        url = data.get("next")

    if not pokemon_urls:

        raise ExtractionError(
            "Pokemon API returned no resources"
        )

    logger.info(
        "Pokemon URLs discovered | count=%s",
        len(pokemon_urls)
    )

    return pokemon_urls

def get_move_urls(pokemon_data):
    """
    Extract unique move API URLs referenced by Pokémon records.
    """

    move_urls = set()

    for pokemon in pokemon_data:
        for item in pokemon.get("moves") or []:
            move_info = item.get("move") or {}
            move_url = move_info.get("url")

            if move_url:
                move_urls.add(move_url)

    return sorted(move_urls)

def extract_pokemon(session, urls, logger):
    """
    Extract individual Pokémon records.

    A failed Pokémon does not stop the entire pipeline.
    The URL is recorded so the failure can be reviewed later.
    """

    pokemon_data = []
    failed_urls = []

    total = len(urls)

    for index, url in enumerate(urls, start=1):

        data = get_json(
            session,
            url,
            logger
        )

        if data is None:

            failed_urls.append(url)

            logger.error(
                "Pokemon extraction failed | progress=%s/%s | url=%s",
                index,
                total,
                url
            )

            continue

        pokemon_data.append(data)

        # Progress logging every 100 records keeps the log useful
        # without generating thousands of log lines.
        if index % 100 == 0 or index == total:

            logger.info(
                "Pokemon extraction progress | %s/%s",
                index,
                total
            )

    logger.info(
        "Pokemon extraction completed | success=%s failed=%s",
        len(pokemon_data),
        len(failed_urls)
    )

    return pokemon_data, failed_urls

def extract_species(session, species_urls, logger):
    """
    Extract species records from unique species URLs.

    Species URLs are expected to be deduplicated before reaching
    this function to avoid unnecessary API requests.
    """

    species_data = []
    failed_urls = []

    total = len(species_urls)

    for index, url in enumerate(species_urls, start=1):

        data = get_json(
            session,
            url,
            logger
        )

        if data is None:

            failed_urls.append(url)

            logger.error(
                "Species extraction failed | progress=%s/%s | url=%s",
                index,
                total,
                url
            )

            continue

        species_data.append(data)

        if index % 100 == 0 or index == total:

            logger.info(
                "Species extraction progress | %s/%s",
                index,
                total
            )

    logger.info(
        "Species extraction completed | success=%s failed=%s",
        len(species_data),
        len(failed_urls)
    )

    return species_data, failed_urls

def extract_moves(session, move_urls, logger):
    """
    Extract move resources from PokéAPI.
    """

    move_data = []
    failed_urls = []

    total = len(move_urls)

    for index, url in enumerate(move_urls, start=1):

        try:
            move = get_json(session, url, logger)

            if move:
                move_data.append(move)

        except Exception as error:
            failed_urls.append(url)

            logger.error(
                "Move extraction failed | url=%s | error=%s",
                url,
                error
            )

        if index % 100 == 0 or index == total:
            logger.info(
                "Move extraction progress | %s/%s",
                index,
                total
            )

    logger.info(
        "Move extraction completed | success=%s | failed=%s",
        len(move_data),
        len(failed_urls)
    )

    return move_data, failed_urls

# -------------------------------------------------------------------
# Incremental extraction helpers
# -------------------------------------------------------------------

def get_resource_id(url):
    """
    Extract the resource ID from a PokéAPI URL.

    Example:
        https://pokeapi.co/api/v2/pokemon/25/

    Returns:
        25
    """

    return int(url.rstrip("/").split("/")[-1])


def filter_new_urls(urls, existing_records):
    """
    Return only URLs whose resource IDs are not already
    present in the existing raw dataset.
    """

    existing_ids = {
        record.get("id")
        for record in existing_records
        if record.get("id") is not None
    }

    new_urls = [
        url
        for url in urls
        if get_resource_id(url) not in existing_ids
    ]

    return new_urls


def merge_records(existing_records, new_records):
    """
    Merge old and newly extracted records using the resource ID
    as the unique key.

    If the same ID appears in both datasets, the new record
    replaces the old record.
    """

    records = {
        record["id"]: record
        for record in existing_records
        if record.get("id") is not None
    }

    for record in new_records:

        if record.get("id") is not None:
            records[record["id"]] = record

    return [
        records[key]
        for key in sorted(records)
    ]