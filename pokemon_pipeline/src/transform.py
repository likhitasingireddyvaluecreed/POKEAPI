import pandas as pd


STAT_COLUMNS = [
    "hp",
    "attack",
    "defense",
    "special_attack",
    "special_defense",
    "speed"
]


# ==========================================
# POKEMON TRANSFORMATION
# ==========================================

def transform_pokemon(pokemon):

    record = {
        "pokemon_id": pokemon.get("id"),
        "pokemon_name": pokemon.get("name"),
        "height_m": (
            pokemon.get("height", 0) / 10
        ),
        "weight_kg": (
            pokemon.get("weight", 0) / 10
        ),
        "base_experience":
            pokemon.get("base_experience")
    }

    for stat in STAT_COLUMNS:
        record[stat] = None

    for item in pokemon.get("stats", []):

        stat_name = (
            item["stat"]["name"]
            .replace("-", "_")
        )

        if stat_name in STAT_COLUMNS:

            record[stat_name] = (
                item["base_stat"]
            )

    return record


def transform_data(pokemon_data):

    records = []

    for pokemon in pokemon_data:

        record = transform_pokemon(
            pokemon
        )

        records.append(record)

    return pd.DataFrame(records)


# ==========================================
# TYPES
# ==========================================
def transform_types(pokemon_data):
    """
    Create the Pokémon-to-type relationship dataset.
    """

    records = []

    for pokemon in pokemon_data:
        pokemon_id = pokemon.get("id")

        for item in pokemon.get("types") or []:
            type_info = item.get("type") or {}

            records.append({
                "pokemon_id": pokemon_id,
                "type_name": type_info.get("name"),
                "slot": item.get("slot")
            })

    return pd.DataFrame(records)


# ==========================================
# ABILITIES
# ==========================================

def transform_abilities(pokemon_data):
    """
    Create the Pokémon-to-ability relationship dataset.
    """

    records = []

    for pokemon in pokemon_data:
        pokemon_id = pokemon.get("id")

        for item in pokemon.get("abilities") or []:
            ability_info = item.get("ability") or {}

            records.append({
                "pokemon_id": pokemon_id,
                "ability_name": ability_info.get("name"),
                "slot": item.get("slot"),
                "is_hidden": item.get("is_hidden")
            })

    return pd.DataFrame(records)

# ==========================================
# SPECIES
# ==========================================

def get_english_genus(species):

    for item in species.get(
        "genera",
        []
    ):

        if item["language"]["name"] == "en":

            return item["genus"]

    return None
def get_nested_name(data, key):
    value = data.get(key)

    if isinstance(value, dict):
        return value.get("name")

    return None
def get_egg_group_names(species):

    egg_groups = species.get("egg_groups") or []

    egg_group_1 = None
    egg_group_2 = None

    if len(egg_groups) > 0:
        egg_group_1 = egg_groups[0].get("name")

    if len(egg_groups) > 1:
        egg_group_2 = egg_groups[1].get("name")

    return egg_group_1, egg_group_2

def transform_species(species_data):

    records = []

    for species in species_data:

        egg_group_1, egg_group_2 = get_egg_group_names(species)

        record = {
            "pokemon_id": species.get("id"),

            # Pokémon category/genus
            "pokemon_category": get_english_genus(species),

            # Classification
            "generation": get_nested_name(
                species,
                "generation"
            ),

            "color": get_nested_name(
                species,
                "color"
            ),

            "shape": get_nested_name(
                species,
                "shape"
            ),

            "habitat": get_nested_name(
                species,
                "habitat"
            ),

            # Gameplay/species information
            "capture_rate": species.get("capture_rate"),

            "base_happiness": species.get(
                "base_happiness"
            ),

            "growth_rate": get_nested_name(
                species,
                "growth_rate"
            ),

            "gender_rate": species.get(
                "gender_rate"
            ),

            "hatch_counter": species.get(
                "hatch_counter"
            ),

            # Egg groups
            "egg_group_1": egg_group_1,
            "egg_group_2": egg_group_2,

            # Special status
            "is_baby": species.get(
                "is_baby"
            ),

            "is_legendary": species.get(
                "is_legendary"
            ),

            "is_mythical": species.get(
                "is_mythical"
            )
        }

        records.append(record)

    return pd.DataFrame(records)


# ==========================================
# CLEANING
# ==========================================

def remove_duplicates(df):

    before = len(df)

    df = df.drop_duplicates(
        subset=["pokemon_id"]
    )

    after = len(df)

    print(
        "Duplicates removed:",
        before - after
    )

    return df


def convert_data_types(df):

    numeric_columns = [
        "pokemon_id",
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

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    return df


def handle_missing_values(df):

    categorical_columns = [
        "pokemon_name"
    ]

    df[categorical_columns] = (
        df[categorical_columns]
        .fillna("Unknown")
    )

    return df


def apply_business_rules(df):

    df = df[
        (df["pokemon_id"] > 0) &
        (df["height_m"] >= 0) &
        (df["weight_kg"] >= 0) &
        (
            df["base_experience"].isna() |
            (df["base_experience"] >= 0)
        )
    ]

    return df


def validate_stat_values(df):

    for column in STAT_COLUMNS:

        df = df[
            df[column].isna() |
            (df[column] >= 0)
        ]

    return df


def clean_data(df):

    df = remove_duplicates(df)

    df = convert_data_types(df)

    df = handle_missing_values(df)

    df = apply_business_rules(df)

    df = validate_stat_values(df)

    return df


# ==========================================
# DERIVED ATTRIBUTES
# ==========================================

def add_total_base_stats(df):

    df["total_base_stats"] = (
        df[STAT_COLUMNS].sum(axis=1)
    )

    return df


def add_offensive_power(df):

    df["offensive_power"] = (
        df["attack"] +
        df["special_attack"]
    )

    return df


def add_defensive_power(df):

    df["defensive_power"] = (
        df["defense"] +
        df["special_defense"]
    )

    return df


def add_speed_percentile(df):

    df["speed_percentile"] = (
        df["speed"]
        .rank(pct=True)
        .mul(100)
        .round(2)
    )

    return df


def add_battle_style(df):

    difference = (
        df["offensive_power"] -
        df["defensive_power"]
    )

    df["battle_style"] = "Balanced"

    df.loc[
        difference > 20,
        "battle_style"
    ] = "Offensive"

    df.loc[
        difference < -20,
        "battle_style"
    ] = "Defensive"

    return df


def add_stat_specialization(df):

    df["stat_specialization"] = (
        df[STAT_COLUMNS]
        .idxmax(axis=1)
    )

    return df


def add_size_class(df):

    height_33 = (
        df["height_m"]
        .quantile(0.33)
    )

    height_66 = (
        df["height_m"]
        .quantile(0.66)
    )

    weight_33 = (
        df["weight_kg"]
        .quantile(0.33)
    )

    weight_66 = (
        df["weight_kg"]
        .quantile(0.66)
    )

    df["size_class"] = "Medium"

    small = (
        (df["height_m"] <= height_33) &
        (df["weight_kg"] <= weight_33)
    )

    large = (
        (df["height_m"] >= height_66) &
        (df["weight_kg"] >= weight_66)
    )

    df.loc[
        small,
        "size_class"
    ] = "Small"

    df.loc[
        large,
        "size_class"
    ] = "Large"

    return df


def add_special_status(df):

    df["special_status"] = "Normal"

    df.loc[
        df["is_baby"] == True,
        "special_status"
    ] = "Baby"

    df.loc[
        df["is_legendary"] == True,
        "special_status"
    ] = "Legendary"

    df.loc[
        df["is_mythical"] == True,
        "special_status"
    ] = "Mythical"

    return df

def merge_species_attributes(
    pokemon_df,
    species_df
):

    species_columns = [
        "pokemon_id",
        "is_baby",
        "is_legendary",
        "is_mythical"
    ]

    species_status = species_df[
        species_columns
    ]

    pokemon_df = pokemon_df.merge(
        species_status,
        on="pokemon_id",
        how="left"
    )

    return pokemon_df

def add_derived_attributes(df):

    df = add_total_base_stats(df)

    df = add_offensive_power(df)

    df = add_defensive_power(df)

    df = add_speed_percentile(df)

    df = add_battle_style(df)

    df = add_stat_specialization(df)

    df = add_size_class(df)

    df = add_special_status(df)

    return df


def transform_varieties(species_data):
    records = []

    for species in species_data:
        species_id = species.get("id")

        for variety in species.get("varieties") or []:
            pokemon = variety.get("pokemon") or {}
            pokemon_url = pokemon.get("url")

            pokemon_id = None
            if pokemon_url:
                pokemon_id = int(
                    pokemon_url.rstrip("/").split("/")[-1]
                )

            records.append({
                "species_id": species_id,
                "pokemon_id": pokemon_id,
                "pokemon_name": pokemon.get("name"),
                "is_default": variety.get("is_default")
            })

    return pd.DataFrame(records)

def transform_types_dimension(pokemon_data):
    """
    Create a unique type dimension dataset.
    """

    types = set()

    for pokemon in pokemon_data:
        for item in pokemon.get("types") or []:
            type_info = item.get("type") or {}
            type_name = type_info.get("name")

            if type_name:
                types.add(type_name)

    records = [
        {
            "type_name": type_name
        }
        for type_name in sorted(types)
    ]

    return pd.DataFrame(records)


def transform_abilities_dimension(pokemon_data):
    """
    Create a unique ability dimension dataset.
    """

    abilities = set()

    for pokemon in pokemon_data:
        for item in pokemon.get("abilities") or []:
            ability_info = item.get("ability") or {}
            ability_name = ability_info.get("name")

            if ability_name:
                abilities.add(ability_name)

    records = [
        {
            "ability_name": ability_name
        }
        for ability_name in sorted(abilities)
    ]

    return pd.DataFrame(records)

############ moves ##################################

def transform_moves(move_data):
    """
    Transform raw move API responses into the move dimension.
    """

    records = []

    for move in move_data:

        type_info = move.get("type") or {}
        damage_class_info = move.get("damage_class") or {}
        generation_info = move.get("generation") or {}

        records.append({
            "move_id": move.get("id"),
            "move_name": move.get("name"),
            "move_type": type_info.get("name"),
            "power": move.get("power"),
            "accuracy": move.get("accuracy"),
            "pp": move.get("pp"),
            "priority": move.get("priority"),
            "damage_class": damage_class_info.get("name"),
            "generation": generation_info.get("name")
        })

    return pd.DataFrame(records)

def transform_pokemon_moves(pokemon_data):
    """
    Create the Pokémon-to-move relationship dataset.

    A Pokémon can learn many moves, and a move can be learned
    by many Pokémon.
    """

    records = []

    for pokemon in pokemon_data:

        pokemon_id = pokemon.get("id")

        for move_item in pokemon.get("moves") or []:

            move_info = move_item.get("move") or {}

            move_url = move_info.get("url")

            move_id = None

            if move_url:
                move_id = int(
                    move_url.rstrip("/").split("/")[-1]
                )

            for version_detail in (
                move_item.get("version_group_details") or []
            ):

                version_group = (
                    version_detail.get("version_group") or {}
                )

                move_learn_method = (
                    version_detail.get("move_learn_method") or {}
                )

                records.append({
                    "pokemon_id": pokemon_id,
                    "move_id": move_id,
                    "move_name": move_info.get("name"),
                    "learn_method": move_learn_method.get("name"),
                    "level_learned_at": version_detail.get(
                        "level_learned_at"
                    ),
                    "version_group": version_group.get("name")
                })

    return pd.DataFrame(records)