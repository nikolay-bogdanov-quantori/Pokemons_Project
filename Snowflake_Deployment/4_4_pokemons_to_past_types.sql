USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.STORAGE;

create or replace table NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PAST_TYPENAMES
(
    pokemon_id int,
    generation_name varchar,
    past_type_name varchar
);


create task NIKOLAY_BOGDANOV.STAGING.PARSE_STG_POKEMONS_PAST_TYPES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_parsed_past_types_stream')
as
insert into NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PAST_TYPENAMES(pokemon_id, generation_name, past_type_name)
select
    parse_json(p.$1):id as pokemon_id,
    parse_json(past_types_generations.value):generation_name as generation_name,
    past_types.value as past_type_name
from stg_pokemons_parsed_past_types_stream p,
lateral flatten( input => $1:past_types) past_types_generations,
lateral flatten(parse_json(past_types_generations.value):types) past_types
where metadata$action = 'INSERT'
;

create or replace view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_PAST_TYPES as
select
    ppt.pokemon_id,
    g.generation_id,
    t.type_id
from NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PAST_TYPENAMES ppt
join STORAGE.TYPES t
join STORAGE.GENERATIONS g
on
    ppt.past_type_name = t.name
    and ppt.generation_name = g.name
;

create stream NIKOLAY_BOGDANOV.STAGING.stg_pokemons_to_past_types_stream on view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_PAST_TYPES;
select * from NIKOLAY_BOGDANOV.STAGING.stg_pokemons_to_past_types_stream;

create or replace table NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_PAST_TYPES
(
    pokemon_id int not null,
    generation_id int not null,
    type_id int not null,
    constraint uniq_links unique(pokemon_id, generation_id, type_id),
    constraint FK_POKEMONS foreign key (pokemon_id) references POKEMONS (pokemon_id),
    constraint FK_GENERATIONS foreign key (generation_id) references GENERATIONS (generation_id),
    constraint FK_TYPES foreign key (type_id) references TYPES (type_id)
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_POKEMONS_TO_PAST_TYPES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_to_past_types_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_PAST_TYPES(pokemon_id, generation_id, type_id)
select
    pokemon_id,
    generation_id,
    type_id
from stg_pokemons_to_past_types_stream
where metadata$action = 'INSERT'
;

select count(*) from STORAGE.POKEMONS_TO_PAST_TYPES; -- 36