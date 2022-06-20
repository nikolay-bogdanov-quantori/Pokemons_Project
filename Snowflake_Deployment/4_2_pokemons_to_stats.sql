USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.STORAGE;

create or replace table NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PARSED_STATS
(
    pokemon_id int,
    stat_name varchar,
    stat_value int
);

create task NIKOLAY_BOGDANOV.STAGING.PARSE_STG_POKEMONS_STATS
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_parsed_stats_stream')
as
insert into NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PARSED_STATS(pokemon_id, stat_name, stat_value)
select
    parse_json(p.$1):id as pokemon_id,
    parse_json(s.value):stat_name as stat_name,
    parse_json(s.value):stat_value as stat_value
from stg_pokemons_parsed_stats_stream p,
lateral flatten( input => $1:stats) s
where metadata$action = 'INSERT'
;

--storage
create or replace view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_STATS as
select
    ps.pokemon_id,
    s.stat_id,
    ps.stat_value
from STG_POKEMONS_PARSED_STATS ps
join STORAGE.STATS s
on
    ps.stat_name = s.name
;

create stream NIKOLAY_BOGDANOV.STAGING.stg_pokemons_to_stats_stream on view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_STATS;
select * from NIKOLAY_BOGDANOV.STAGING.stg_pokemons_to_stats_stream;

create or replace table NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_STATS
(
    pokemon_id int not null,
    stat_id int not null,
    stat_value int not null,
    constraint uniq_pairs unique(pokemon_id, stat_id),
    constraint FK_POKEMONS foreign key (pokemon_id) references STORAGE.POKEMONS (pokemon_id),
    constraint FK_STATS foreign key (stat_id) references STORAGE.STATS (stat_id)
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_POKEMONS_TO_STATS
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_to_stats_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_STATS(pokemon_id, stat_id, stat_value)
select
    pokemon_id,
    stat_id,
    stat_value
from stg_pokemons_to_stats_stream
where metadata$action = 'INSERT'
;


select count(*) from STORAGE.POKEMONS_TO_STATS; -- 6756
