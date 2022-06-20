USE ROLE SYSADMIN;
USE DATABASE NIKOLAY_BOGDANOV;
USE WAREHOUSE DEPLOYMENT_WH;
USE NIKOLAY_BOGDANOV.STAGING;

GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin; -- не нашел другого способа запускать таки, созданные под SYSADMIN, не уверен, что он безопасен

show tasks;
--
alter task MOVING_STG_GENERATIONS resume;
alter task MOVING_STG_MOVES resume;
alter task MOVING_STG_STATS resume;
alter task MOVING_STG_TYPES resume;
alter task MOVING_STG_POKEMONS resume;
--
alter task PARSE_STG_POKEMONS_TYPES resume;
alter task MOVING_STG_POKEMONS_TO_TYPES resume;
--
alter task PARSE_STG_POKEMONS_STATS resume;
alter task MOVING_STG_POKEMONS_TO_STATS resume;
--
alter task PARSE_STG_POKEMONS_MOVES resume;
alter task MOVING_STG_POKEMONS_TO_MOVES resume;
--
alter task PARSE_STG_POKEMONS_PAST_TYPES resume;
alter task MOVING_STG_POKEMONS_TO_PAST_TYPES resume;
