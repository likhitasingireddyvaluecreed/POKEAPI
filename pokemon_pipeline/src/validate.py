"""
Validation layer for the Pokémon ETL pipeline.

Validation ensures that data is structurally and logically valid
before it becomes part of the processed layer.
"""

import pandas as pd
from exceptions import ValidationError

# -------------------------------------------------------------------
# Generic validation
# -------------------------------------------------------------------

def validate_dataframe(
    df,
    required_columns,
    dataset_name,
    logger
):
    """
    Validate basic structural requirements.

    Raises:
        ValidationError: If validation fails.
    """

    if df.empty:

        logger.error(
            "Validation failed | dataset=%s | reason=empty dataset",
            dataset_name
        )

        raise ValidationError(
            f"{dataset_name} dataset is empty"
        )

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:

        logger.error(
            "Validation failed | dataset=%s | missing_columns=%s",
            dataset_name,
            missing_columns
        )

        raise ValidationError(
            f"{dataset_name} is missing columns: {missing_columns}"
        )

    logger.info(
        "Validation passed | dataset=%s | records=%s",
        dataset_name,
        len(df)
    )

    return True
def validate_unique_key(
    df,
    key_column,
    dataset_name,
    logger
):
    """
    Validate that a business key is unique.

    Raises:
        ValidationError: If duplicate keys are found.
    """

    if key_column not in df.columns:

        raise ValidationError(
            f"{dataset_name} does not contain key: {key_column}"
        )

    duplicate_count = (
        df[key_column]
        .duplicated()
        .sum()
    )

    if duplicate_count > 0:

        logger.error(
            "Validation failed | dataset=%s | duplicate_%s=%s",
            dataset_name,
            key_column,
            duplicate_count
        )

        raise ValidationError(
            f"{dataset_name} contains "
            f"{duplicate_count} duplicate {key_column} values"
        )

    logger.info(
        "Unique key validation passed | dataset=%s | key=%s",
        dataset_name,
        key_column
    )

    return True

def validate_non_negative(
    df,
    columns,
    dataset_name,
    logger
):
    """
    Ensure numeric columns do not contain negative values.
    """

    for column in columns:

        if column not in df.columns:
            continue

        invalid_count = (
            df[column]
            .dropna()
            .lt(0)
            .sum()
        )

        if invalid_count > 0:

            logger.error(
                "Validation failed | dataset=%s | "
                "column=%s | negative_values=%s",
                dataset_name,
                column,
                invalid_count
            )

            raise ValidationError(
                f"{dataset_name}.{column} "
                f"contains negative values"
            )

    logger.info(
        "Non-negative validation passed | dataset=%s",
        dataset_name
    )

    return True

REQUIRED_COLUMNS = [
    "pokemon_id",
    "pokemon_name",
    "height_m",
    "weight_kg",
    "base_experience",
    "hp",
    "attack",
    "defense",
    "special_attack",
    "special_defense",
    "speed",
    "total_base_stats",
    "offensive_power",
    "defensive_power",
    "speed_percentile",
    "battle_style",
    "stat_specialization",
    "size_class",
    "special_status"
]


def validate_required_columns(df):

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            f"Missing required columns: "
            f"{missing_columns}"
        )


def validate_pokemon_ids(df):

    if df["pokemon_id"].isna().any():

        raise ValueError(
            "Missing Pokémon IDs"
        )

    if (
        df["pokemon_id"] <= 0
    ).any():

        raise ValueError(
            "Invalid Pokémon ID found"
        )

    if (
        df["pokemon_id"]
        .duplicated()
        .any()
    ):

        raise ValueError(
            "Duplicate Pokémon IDs found"
        )


def validate_pokemon_names(df):

    if df["pokemon_name"].isna().any():

        raise ValueError(
            "Missing Pokémon names"
        )

    if (
        df["pokemon_name"]
        .str.strip()
        .eq("")
        .any()
    ):

        raise ValueError(
            "Empty Pokémon names found"
        )


def validate_numeric_values(df):

    columns = [
        "height_m",
        "weight_kg",
        "base_experience",
        "hp",
        "attack",
        "defense",
        "special_attack",
        "special_defense",
        "speed"
    ]

    for column in columns:

        values = df[column].dropna()

        if (values < 0).any():

            raise ValueError(
                f"Negative value found "
                f"in {column}"
            )


def validate_total_stats(df):

    expected = (
        df["hp"] +
        df["attack"] +
        df["defense"] +
        df["special_attack"] +
        df["special_defense"] +
        df["speed"]
    )

    if not (
        df["total_base_stats"]
        .equals(expected)
    ):

        raise ValueError(
            "Incorrect total_base_stats values"
        )


def validate_speed_percentile(df):

    if (
        (df["speed_percentile"] < 0) |
        (df["speed_percentile"] > 100)
    ).any():

        raise ValueError(
            "Invalid speed percentile"
        )


def validate_battle_style(df):

    allowed = {
        "Offensive",
        "Defensive",
        "Balanced"
    }

    invalid = (
        set(df["battle_style"]) -
        allowed
    )

    if invalid:

        raise ValueError(
            f"Invalid battle styles: "
            f"{invalid}"
        )


def validate_special_status(df):

    allowed = {
        "Normal",
        "Baby",
        "Legendary",
        "Mythical"
    }

    invalid = (
        set(df["special_status"]) -
        allowed
    )

    if invalid:

        raise ValueError(
            f"Invalid special statuses: "
            f"{invalid}"
        )


def validate_foreign_keys(
    pokemon_df,
    types_df,
    abilities_df,
    species_df
):

    pokemon_ids = set(
        pokemon_df["pokemon_id"]
    )

    type_ids = set(
        types_df["pokemon_id"]
    )

    ability_ids = set(
        abilities_df["pokemon_id"]
    )

    species_ids = set(
        species_df["pokemon_id"]
    )

    if not type_ids.issubset(
        pokemon_ids
    ):

        raise ValueError(
            "Type table contains "
            "unknown Pokémon IDs"
        )

    if not ability_ids.issubset(
        pokemon_ids
    ):

        raise ValueError(
            "Ability table contains "
            "unknown Pokémon IDs"
        )

    if not species_ids.issubset(
        pokemon_ids
    ):

        raise ValueError(
            "Species table contains "
            "unknown Pokémon IDs"
        )


def validate_data(df):

    print("Running data validation...")

    validate_required_columns(df)

    validate_pokemon_ids(df)

    validate_pokemon_names(df)

    validate_numeric_values(df)

    validate_total_stats(df)

    validate_speed_percentile(df)

    validate_battle_style(df)

    validate_special_status(df)

    print("Main Pokémon validation passed.")


def validate_all(
    pokemon_df,
    types_df,
    abilities_df,
    species_df
):

    validate_data(pokemon_df)

    validate_foreign_keys(
        pokemon_df,
        types_df,
        abilities_df,
        species_df
    )

    print(
        "All dataset relationships "
        "validated successfully."
    )