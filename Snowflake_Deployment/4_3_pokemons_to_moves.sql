USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.STORAGE;

create or replace table NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PARSED_MOVES
(
    pokemon_id int,
    move_name varchar
);


create task NIKOLAY_BOGDANOV.STAGING.PARSE_STG_POKEMONS_MOVES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_parsed_moves_stream')
as
insert into NIKOLAY_BOGDANOV.STAGING.STG_POKEMONS_PARSED_MOVES(pokemon_id, move_name)
select
    parse_json(p.$1):id as pokemon_id,
    m.value as move_name
from stg_pokemons_parsed_moves_stream p,
lateral flatten( input => $1:moves) m
where metadata$action = 'INSERT'
;

--storage
create or replace view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_MOVES as
select
    pm.pokemon_id,
    m.move_id
from STG_POKEMONS_PARSED_MOVES pm
join STORAGE.MOVES m
on
    pm.move_name = m.name
;

create stream NIKOLAY_BOGDANOV.STAGING.stg_pokemons_to_moves_stream on view NIKOLAY_BOGDANOV.STAGING.V_STG_POKEMONS_TO_MOVES;
select * from NIKOLAY_BOGDANOV.STAGING.stg_pokemons_to_moves_stream;

create table NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_MOVES
(
    pokemon_id int not null,
    move_id int not null,
    constraint uniq_pairs unique(pokemon_id, move_id),
    constraint FK_POKEMONS foreign key (pokemon_id) references STORAGE.POKEMONS (pokemon_id),
    constraint FK_MOVES foreign key (move_id) references STORAGE.MOVES (move_id)
);

create task NIKOLAY_BOGDANOV.STAGING.MOVING_STG_POKEMONS_TO_MOVES
WAREHOUSE = TASKS_WH
SCHEDULE = '2 minute'
when
  system$stream_has_data('stg_pokemons_to_moves_stream')
as
insert into NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_MOVES(pokemon_id, move_id)
select
    pokemon_id,
    move_id
from stg_pokemons_to_moves_stream
where metadata$action = 'INSERT'
;

select count(*) from STORAGE.POKEMONS_TO_MOVES -- 78139