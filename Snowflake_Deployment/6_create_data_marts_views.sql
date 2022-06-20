USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.DATA_MARTS;


-- a) Сколько покемонов в каждом типе (type в терминах API), насколько это меньше чем у следующего по рангу типа? А насколько больше, чем у предыдущего?
create or replace view NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_A as
select
    t.name as type_name,
    count(pt.pokemon_id) as pokemons_count,
    lead(pokemons_count) over(order by pokemons_count) - pokemons_count as diff_to_next,
    lag(pokemons_count) over(order by pokemons_count) - pokemons_count  as diff_to_prev
from NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_TYPES pt
join NIKOLAY_BOGDANOV.STORAGE.TYPES t
on
    pt.type_id = t.type_id
group by t.name
;

select * from NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_A;

-- b) Сколько у разных атак (moves в терминах API) использующих их покемонов? + показать дельту от следующей и предыдущей по популярности атаки.
create or replace view NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_B as
select
    m.name as move_name,
    count(pm.pokemon_id) as pokemons_count,
    pokemons_count - lead(pokemons_count) over(order by pokemons_count desc) as diff_to_next,
    pokemons_count - lag(pokemons_count) over(order by pokemons_count desc) as diff_to_prev
from NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_MOVES pm
join NIKOLAY_BOGDANOV.STORAGE.MOVES m
on
    pm.move_id = m.move_id
group by m.name
;

select * from NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_B;

-- c) Составить рейтинг покемонов по сумме их характеристик (stats в терминах API). Например, если у покемона есть только 2 статы: HP 20 & attack 25, то в финальный рейтинг идёт сумма характеристик: 20 + 25 = 45.
create or replace view NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_C as
select
    p.name as pokemon_name,
    sum(ps.stat_value) as stat_rating
from NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_STATS ps
join NIKOLAY_BOGDANOV.STORAGE.POKEMONS p
on
    p.pokemon_id = ps.pokemon_id
group by p.name
order by stat_rating desc, p.name
;

select * from NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_C;

-- отображает, как изменялся тип покемона по поколениям, логику работы постарался изложить в readme к проекту
create or replace view NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TYPES_PROGRESSION as
select
    p.pokemon_id,
    g.generation_id,
    ifnull(last_value(ppt.type_id) ignore nulls over(partition by p.pokemon_id order by g.generation_id desc rows between unbounded preceding and current row), pt.type_id) as type_id_for_generation
from NIKOLAY_BOGDANOV.STORAGE.POKEMONS p
join NIKOLAY_BOGDANOV.STORAGE.GENERATIONS g
on
    p.generation_id <= g.generation_id -- джойним со всеми более поздними (относительно поколения  появления покемона) поколениями,
left join NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_PAST_TYPES ppt
on
    p.pokemon_id = ppt.pokemon_id
    and ppt.generation_id = g.generation_id
left join NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TO_TYPES pt
on
    p.pokemon_id = pt.pokemon_id
    and ppt.pokemon_id is null -- неочевидное решение, но позволяет избежать появления дубликатов, когда у покемона изначально был один тип, а в одном из следующих поколений стало несколько (случай покемонов 10008, 10012)
;

--та как столбец POKEMONS_TYPES_PROGRESSION.type_id_for_generation вычисляется оконной функцией, а по нему нужно заджойнить имена, нам нужна еще одна вьюшка
create or replace view NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TYPES_PROGRESSION_WITH_TYPENAMES as
select
    ptp.pokemon_id,
    ptp.generation_id,
    t.name as type_name
from NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TYPES_PROGRESSION ptp
join STORAGE.TYPES t
on
    ptp.type_id_for_generation = t.type_id
order by ptp.pokemon_id, ptp.generation_id desc, t.name
;

-- select * -- запрос нескольких характерных покемонов, может использоваться для теста вьюшки
-- from DATA_MARTS.POKEMONS_TYPES_PROGRESSION_WITH_TYPENAMES ptp
-- where
--     pokemon_id = 1 -- bulbasaur
--     or pokemon_id = 35 -- clefairy
--     or pokemon_id = 10008
--     or pokemon_id = 10012
-- ;

-- Показать количество покемонов по типам (строки таблицы, type в терминах API) и поколениям (колонки таблицы, generations в терминах API).
create or replace view NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_D as
select *
from NIKOLAY_BOGDANOV.STORAGE.POKEMONS_TYPES_PROGRESSION_WITH_TYPENAMES
pivot(count(pokemon_id) for generation_id in (1, 2, 3, 4, 5, 6, 7, 8))
as p (type_name, generation_i, generation_ii, generation_iii, generation_iv, generation_v, generation_vi, generation_vii, generation_viii)
order by type_name
;

select * from NIKOLAY_BOGDANOV.DATA_MARTS.ANSWER_D;