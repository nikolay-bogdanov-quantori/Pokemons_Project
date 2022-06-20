USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.STORAGE;

--generations
create or replace table NIKOLAY_BOGDANOV.STORAGE.GENERATIONS
(
    generation_id int PRIMARY KEY,
    name varchar UNIQUE,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_GENERATIONS
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_generations_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.GENERATIONS(generation_id, name, filename,amnd_user,amnd_date)
select
    parse_json($1):id as generation_id,
    parse_json($1):name as name,
    filename,
    amnd_user,
    amnd_date
from stg_generations_stream
where metadata$action = 'INSERT'
;

select * from STORAGE.GENERATIONS;

--moves
create or replace table NIKOLAY_BOGDANOV.STORAGE.MOVES
(
    move_id int PRIMARY KEY,
    name varchar UNIQUE,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_MOVES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_moves_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.MOVES(move_id, name, filename, amnd_user,amnd_date)
select
    parse_json($1):id as move_id,
    parse_json($1):name as name,
    filename,
    amnd_user,
    amnd_date
from stg_moves_stream
where metadata$action = 'INSERT'
;

select * from STORAGE.MOVES;

--stats
create or replace table NIKOLAY_BOGDANOV.STORAGE.STATS
(
    stat_id int PRIMARY KEY,
    name varchar UNIQUE,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_STATS
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_stats_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.STATS(stat_id, name, filename, amnd_user,amnd_date)
select
    parse_json($1):id as stat_id,
    parse_json($1):name as name,
    filename,
    amnd_user,
    amnd_date
from stg_stats_stream
where metadata$action = 'INSERT'
;

select * from STORAGE.STATS;

--types
create or replace table NIKOLAY_BOGDANOV.STORAGE.TYPES
(
    type_id int PRIMARY KEY,
    name varchar UNIQUE,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_TYPES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_types_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.TYPES(type_id, name, filename, amnd_user,amnd_date)
select
    parse_json($1):id as type_id,
    parse_json($1):name as name,
    filename,
    amnd_user,
    amnd_date
from stg_types_stream
where metadata$action = 'INSERT'
;

select * from STORAGE.TYPES;


-- pokemons
create or replace table NIKOLAY_BOGDANOV.STORAGE.POKEMONS
(
    pokemon_id int PRIMARY KEY,
    name varchar UNIQUE,
    generation_id int,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime default current_timestamp
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_POKEMONS
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.POKEMONS(pokemon_id, name, generation_id, filename, amnd_user, amnd_date)
select
    parse_json($1):id as pokemon_id,
    parse_json($1):name as name,
    parse_json($1):generation as generation_id,
    filename,
    amnd_user,
    amnd_date
from stg_pokemons_stream
where metadata$action = 'INSERT'
;

select * from STORAGE.POKEMONS;