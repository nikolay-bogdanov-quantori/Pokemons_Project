USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.STORAGE;

create or replace table NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PARSED_TYPES
(
    pokemon_id int,
    type_name varchar
);


create task NIKOLAY_BOGDANOV.STAGING.PARSE_STG_POKEMONS_TYPES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_parsed_types_stream')
as
insert into NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PARSED_TYPES(pokemon_id, type_name)
select
    parse_json(p.$1):id as pokemon_id,
    types.value as type_name
from stg_pokemons_parsed_types_stream p,
lateral flatten( input => $1:types) types
where metadata$action = 'INSERT'
;

select * from STAGING.STG_POKEMONS_PARSED_TYPES;

create or replace view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_TYPES as
select
    pt.pokemon_id,
    t.type_id
from STAGING.STG_POKEMONS_PARSED_TYPES pt
join STORAGE.TYPES t
on pt.type_name = t.name
;

create or replace stream NIKOLAY_BOGDANOV.STAGING.stg_pokemons_to_types_stream on view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_TYPES;
select * from STAGING.stg_pokemons_to_types_stream;

create or replace table NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_TYPES
(
    pokemon_id int not null,
    type_id int not null,
    constraint uniq_pairs unique(pokemon_id, type_id),
    constraint FK_POKEMONS foreign key (pokemon_id) references STORAGE.POKEMONS (pokemon_id),
    constraint FK_TYPES foreign key (type_id) references STORAGE.TYPES (type_id)
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_POKEMONS_TO_TYPES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_to_types_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_TYPES(pokemon_id, type_id)
select
    pokemon_id,
    type_id
from stg_pokemons_to_types_stream
where metadata$action = 'INSERT'
;

select count(*) from STORAGE.POKEMONS_TO_TYPES; --1728