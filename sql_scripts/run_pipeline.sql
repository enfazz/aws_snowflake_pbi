-- Run it commnad to run snowflake pipeline
call run_pipeline();



-- Some procedure to run individual parts of pipeline
-- Run a loading from bronze layer to silver

call bronze_to_silver_loading             ('bronze_layer', 'silver_layer');

-- All procedure to run a each of gold table loading from silver layer

call SILVER_TO_GOLD_LAYER_CUSTOMERS_TABLE   ('silver_layer', 'gold_layer');
call SILVER_TO_GOLD_LAYER_DATE_TABLE        ('silver_layer', 'gold_layer');
call SILVER_TO_GOLD_LAYER_GEOLOCATION_TABLE ('silver_layer', 'gold_layer');
call SILVER_TO_GOLD_LAYER_PROJECT_TABLE     ('silver_layer', 'gold_layer');
call SILVER_TO_GOLD_LAYER_SALES_TABLE       ('silver_layer', 'gold_layer');
call SILVER_TO_GOLD_LAYER_SELLERS_TABLE     ('silver_layer', 'gold_layer');