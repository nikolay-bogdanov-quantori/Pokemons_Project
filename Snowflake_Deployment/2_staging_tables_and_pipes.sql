USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.STAGING;

-- generations
create or replace table NIKOLAY_BOGDANOV.STAGING.STG_GENERATIONS
(
    json_data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create stream stg_generations_stream on table STG_GENERATIONS;

create pipe stg_generations_pipe auto_ingest=true as
copy into STG_GENERATIONS(json_data, filename, amnd_user)
from (
    select $1, metadata$filename, CURRENT_USER()
    from @SF_stage/snowpipe/Bogdanov/Generations/
)
pattern='processed_generations.*.json'
file_format = (type = 'JSON', strip_outer_array = true)
;

-- moves
create or replace table NIKOLAY_BOGDANOV.STAGING.STG_MOVES
(
    json_data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create stream stg_moves_stream on table STG_MOVES;

create pipe stg_moves_pipe auto_ingest=true as
copy into STG_MOVES(json_data, filename, amnd_user)
from (
    select $1, metadata$filename, CURRENT_USER()
    from @SF_stage/snowpipe/Bogdanov/Moves/
)
pattern='processed_moves.*.json'
file_format = (type = 'JSON', strip_outer_array = true)
;

-- stats
create or replace table NIKOLAY_BOGDANOV.STAGING.STG_STATS
(
    json_data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create stream stg_stats_stream on table STG_STATS;


create pipe stg_stats_pipe auto_ingest=true as
copy into NIKOLAY_BOGDANOV.STAGING.STG_STATS(json_data, filename, amnd_user)
from (
    select $1, metadata$filename, CURRENT_USER()
    from @SF_stage/snowpipe/Bogdanov/Stats/
)
pattern='processed_stats.*.json'
file_format = (type = 'JSON', strip_outer_array = true)
;


-- types
create or replace table NIKOLAY_BOGDANOV.STAGING.STG_TYPES
(
    json_data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create stream stg_types_stream on table STG_TYPES;

create pipe stg_types_pipe auto_ingest=true as
copy into NIKOLAY_BOGDANOV.STAGING.STG_TYPES(json_data, filename, amnd_user)
from (
    select $1, metadata$filename, CURRENT_USER()
    from @SF_stage/snowpipe/Bogdanov/Types/
)
pattern='processed_types.*.json'
file_format = (type = 'JSON', strip_outer_array = true)
;

--pokemons
create or replace table NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS
(
    json_data variant,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create stream stg_pokemons_stream on table STG_POKEMONS;
create stream stg_pokemons_parsed_types_stream on table STG_POKEMONS;
create stream stg_pokemons_parsed_past_types_stream on table STG_POKEMONS;
create stream stg_pokemons_parsed_moves_stream on table STG_POKEMONS;
create stream stg_pokemons_parsed_stats_stream on table STG_POKEMONS;

create pipe stg_pokemons_pipe auto_ingest=true as
copy into STG_POKEMONS(json_data, filename, amnd_user)
from (
    select $1, metadata$filename, CURRENT_USER()
    from @SF_stage/snowpipe/Bogdanov/Pokemons/
)
pattern='processed_pokemons.*.json'
file_format = (type = 'JSON', strip_outer_array = true)
;