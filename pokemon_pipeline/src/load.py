"""
Data loading layer for the Pokémon ETL pipeline.

Responsibilities:
- Save raw API responses as JSON.
- Load previously extracted raw data.
- Save processed datasets as CSV.
- Create required directories automatically.
"""

import json
from pathlib import Path


# -------------------------------------------------------------------
# JSON loading
# -------------------------------------------------------------------

def load_json(file_path, logger):
    """
    Load previously stored JSON data.

    Returns:
        list/dict: Loaded JSON data.

    Raises:
        OSError: If the file cannot be read.
        JSONDecodeError: If the file contains invalid JSON.
    """

    file_path = Path(file_path)

    if not file_path.exists():

        logger.info(
            "Raw file not found | file=%s",
            file_path
        )

        return None

    try:

        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        logger.info(
            "Raw data loaded | file=%s | records=%s",
            file_path,
            len(data)
        )

        return data

    except (OSError, json.JSONDecodeError) as error:

        logger.error(
            "Failed to load raw data | file=%s | error=%s",
            file_path,
            error
        )

        raise


# -------------------------------------------------------------------
# JSON saving
# -------------------------------------------------------------------

def save_json(data, file_path, logger):
    """
    Save raw API data as a JSON snapshot.

    The file is overwritten intentionally.
    """

    file_path = Path(file_path)

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            indent=4,
            ensure_ascii=False
        )

    logger.info(
        "JSON snapshot saved | file=%s | records=%s",
        file_path,
        len(data)
    )


# -------------------------------------------------------------------
# CSV saving
# -------------------------------------------------------------------

def save_csv(df, file_path, logger):
    """
    Save a processed DataFrame as a complete CSV snapshot.

    Existing data is replaced instead of appended.
    """

    file_path = Path(file_path)

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        file_path,
        index=False
    )

    logger.info(
        "CSV dataset saved | file=%s | records=%s | columns=%s",
        file_path,
        len(df),
        len(df.columns)
    )