"""
Main orchestration module for the Pokémon ETL pipeline.

Pipeline flow:

API
 ↓
Extraction
 ↓
Raw Data
 ↓
Transformation
 ↓
Validation
 ↓
Processed Data
"""

import time

from config import (
    INCREMENTAL_MODE,
    PROCESSED_MOVES_FILE,
    RAW_POKEMON_FILE,
    RAW_SPECIES_FILE,
    PROCESSED_POKEMON_FILE,
    PROCESSED_TYPES_FILE,
    PROCESSED_ABILITIES_FILE,
    PROCESSED_SPECIES_FILE,
    PROCESSED_VARIETIES_FILE,
    TYPES_FILE,
    ABILITIES_FILE,
    MOVES_FILE,
)

from logger import setup_logger

from exceptions import (
    ExtractionError,
    ValidationError,
    PipelineError
)

from extract import (
    create_session,
    get_pokemon_urls,
    extract_pokemon,
    extract_species,
    get_move_urls,
    extract_moves,
    filter_new_urls,
    merge_records
)

from load import (
    load_json,
    save_json,
    save_csv
)

from transform import (
    transform_data,
    transform_types,
    transform_abilities,
    transform_species,
    transform_varieties,
    transform_types_dimension,
    transform_abilities_dimension,
    transform_moves,
    transform_pokemon_moves
)

from validate import (
    validate_dataframe,
    validate_non_negative,
    validate_unique_key
)


def main():
    """
    Orchestrate the complete Pokémon ETL pipeline.

    The main function coordinates extraction, incremental
    processing, transformation, validation, and loading.
    """

    start_time = time.perf_counter()

    logger = setup_logger()

    try:

        # ============================================================
        # PIPELINE START
        # ============================================================

        logger.info("=" * 70)
        logger.info("Pokémon ETL pipeline started")
        logger.info(
            "Pipeline mode | incremental=%s",
            INCREMENTAL_MODE
        )
        logger.info("=" * 70)

        # Create one reusable HTTP session for the pipeline.
        session = create_session()

        # ============================================================
        # 1. DISCOVER POKÉMON
        # ============================================================

        logger.info("Discovering Pokémon resources")

        pokemon_urls = get_pokemon_urls(
            session,
            logger
        )

        if not pokemon_urls:

            raise ExtractionError(
                "No Pokémon resources were discovered"
            )

        # ============================================================
        # 2. LOAD EXISTING RAW POKÉMON
        # ============================================================

        if INCREMENTAL_MODE:

            existing_pokemon = load_json(
                RAW_POKEMON_FILE,
                logger
            )

            if existing_pokemon is None:
                existing_pokemon = []

        else:

            existing_pokemon = []

        # ============================================================
        # 3. IDENTIFY NEW POKÉMON
        # ============================================================

        if INCREMENTAL_MODE:

            new_pokemon_urls = filter_new_urls(
                pokemon_urls,
                existing_pokemon
            )

        else:

            new_pokemon_urls = pokemon_urls

        logger.info(
            "Pokemon incremental check | existing=%s | new=%s",
            len(existing_pokemon),
            len(new_pokemon_urls)
        )

        # ============================================================
        # 4. EXTRACT NEW POKÉMON
        # ============================================================

        new_pokemon, failed_pokemon = extract_pokemon(
            session,
            new_pokemon_urls,
            logger
        )

        if failed_pokemon:

            logger.warning(
                "Pokemon extraction completed with failures | failed=%s",
                len(failed_pokemon)
            )

        # ============================================================
        # 5. MERGE POKÉMON DATA
        # ============================================================

        pokemon_data = merge_records(
            existing_pokemon,
            new_pokemon
        )

        # DISCOVER MOVES
        logger.info("Discovering move resources")

        move_urls = get_move_urls(pokemon_data)

        logger.info(
            "Unique move resources discovered | count=%s",
            len(move_urls)
        )
        # EXTRACT MOVES
        move_data, failed_moves = extract_moves(
            session,
            move_urls,
            logger
        )

        if failed_moves:
            logger.warning(
                "Move extraction completed with failures | failed=%s",
                len(failed_moves)
            )

        logger.info(
            "Pokemon dataset ready | total=%s",
            len(pokemon_data)
        )

        # Save only when new data was extracted or
        # when running in full-refresh mode.
        if new_pokemon or not INCREMENTAL_MODE:

            save_json(
                pokemon_data,
                RAW_POKEMON_FILE,
                logger
            )

        # ============================================================
        # 6. DISCOVER SPECIES URLs
        # ============================================================

        species_urls = list({
            pokemon["species"]["url"]
            for pokemon in pokemon_data
            if pokemon.get("species")
            and pokemon["species"].get("url")
        })

        logger.info(
            "Unique species resources discovered | count=%s",
            len(species_urls)
        )

        # ============================================================
        # 7. LOAD EXISTING SPECIES
        # ============================================================

        if INCREMENTAL_MODE:

            existing_species = load_json(
                RAW_SPECIES_FILE,
                logger
            )

            if existing_species is None:
                existing_species = []

        else:

            existing_species = []

        # ============================================================
        # 8. IDENTIFY NEW SPECIES
        # ============================================================

        if INCREMENTAL_MODE:

            new_species_urls = filter_new_urls(
                species_urls,
                existing_species
            )

        else:

            new_species_urls = species_urls

        logger.info(
            "Species incremental check | existing=%s | new=%s",
            len(existing_species),
            len(new_species_urls)
        )

        # ============================================================
        # 9. EXTRACT NEW SPECIES
        # ============================================================

        new_species, failed_species = extract_species(
            session,
            new_species_urls,
            logger
        )

        if failed_species:

            logger.warning(
                "Species extraction completed with failures | failed=%s",
                len(failed_species)
            )

        # ============================================================
        # 10. MERGE SPECIES DATA
        # ============================================================

        species_data = merge_records(
            existing_species,
            new_species
        )

        logger.info(
            "Species dataset ready | total=%s",
            len(species_data)
        )

        if new_species or not INCREMENTAL_MODE:

            save_json(
                species_data,
                RAW_SPECIES_FILE,
                logger
            )

        # ============================================================
        # 11. TRANSFORMATION
        # ============================================================

        logger.info("Starting transformation layer")

        pokemon_df = transform_data(
            pokemon_data
        )

        types_df = transform_types(
            pokemon_data
        )

        types_dimension_df = transform_types_dimension(pokemon_data)


        abilities_df = transform_abilities(
            pokemon_data
        )

        abilities_dimension_df = transform_abilities_dimension(pokemon_data)

        species_df = transform_species(
            species_data
        )
        varieties_df = transform_varieties(
            species_data
        )

        moves_df = transform_moves(move_data)
        pokemon_moves_df = transform_pokemon_moves(pokemon_data)


        logger.info(
            "Transformation completed"
        )

        # ============================================================
        # 12. VALIDATION
        # ============================================================

        logger.info("Starting validation layer")

        # ------------------------------------------------------------
        # Pokémon dataset validation
        # ------------------------------------------------------------

        validate_dataframe(
            pokemon_df,
            [
                "pokemon_id",
                "pokemon_name",
                "height_m",
                "weight_kg",
                "base_experience"
            ],
            "pokemon",
            logger
        )

        validate_unique_key(
            pokemon_df,
            "pokemon_id",
            "pokemon",
            logger
        )

        validate_non_negative(
            pokemon_df,
            [
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
            ],
            "pokemon",
            logger
        )

        # ------------------------------------------------------------
        # Species dataset validation
        # ------------------------------------------------------------

        validate_dataframe(
            species_df,
            [
                "pokemon_id",
                "pokemon_category",
                "generation"
            ],
            "pokemon_species",
            logger
        )

        validate_unique_key(
            species_df,
            "pokemon_id",
            "pokemon_species",
            logger
        )

        logger.info(
            "All validations passed"
        )

        # ============================================================
        # 13. SAVE PROCESSED DATA
        # ============================================================

        logger.info("Saving processed datasets")

        save_csv(
            pokemon_df,
            PROCESSED_POKEMON_FILE,
            logger
        )

        save_csv(
            types_df,
            PROCESSED_TYPES_FILE,
            logger
        )

        save_csv(
            abilities_df,
            PROCESSED_ABILITIES_FILE,
            logger
        )

        save_csv(
            species_df,
            PROCESSED_SPECIES_FILE,
            logger
        )

        save_csv(
        varieties_df,
        PROCESSED_VARIETIES_FILE,
        logger
        )

        save_csv(
        types_dimension_df,
        TYPES_FILE,
        logger
        )

        save_csv(
            abilities_dimension_df,
            ABILITIES_FILE,
            logger
        )

        save_csv(
            moves_df,
            MOVES_FILE,
            logger
        )

        save_csv(
            pokemon_moves_df,
            PROCESSED_MOVES_FILE,
            logger
        )
            # ============================================================
        # 14. PIPELINE SUMMARY
        # ============================================================

        duration = (
            time.perf_counter() - start_time
        )

        logger.info("=" * 70)

        logger.info(
            "Pipeline completed successfully | duration=%.2fs",
            duration
        )

        logger.info(
            "Pipeline summary | "
            "pokemon=%s | "
            "species=%s | "
            "types=%s | "
            "abilities=%s | "
            "varieties=%s | "
            "pokemon_failures=%s | "
            "species_failures=%s",
            len(pokemon_df),
            len(species_df),
            len(types_df),
            len(abilities_df),
            len(varieties_df),
            len(failed_pokemon),
            len(failed_species)
        )

        logger.info("=" * 70)

    # ================================================================
    # EXCEPTION HANDLING
    # ================================================================

    except ExtractionError as error:

        logger.error(
            "Pipeline failed during extraction | error=%s",
            error
        )

    except ValidationError as error:

        logger.error(
            "Pipeline failed during validation | error=%s",
            error
        )

    except PipelineError as error:

        logger.error(
            "Pipeline failed | error=%s",
            error
        )

    except Exception:

        # Unexpected programming/system error.
        # logger.exception() records the full traceback.
        logger.exception(
            "Unexpected pipeline failure"
        )

    finally:

        duration = (
            time.perf_counter() - start_time
        )

        logger.info(
            "Pipeline execution finished | duration=%.2fs",
            duration
        )


if __name__ == "__main__":
    main()