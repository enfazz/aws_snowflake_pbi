create schema project.silver_layer;

CREATE STORAGE INTEGRATION aws_s3_storage
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::435355431943:role/snowflake-s3-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-bucket324324');


describe integration aws_s3_storage;

create or replace file format my_csv_format
type = csv
field_delimiter = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
skip_header = 1
null_if = ('NULL', 'null')
empty_field_as_null = true;


CREATE OR REPLACE STAGE my_s3_stage
STORAGE_INTEGRATION = aws_s3_storage
URL = 's3://snowflake-bucket324324/olist/'
FILE_FORMAT = my_csv_format;


list @my_s3_stage;

-- select $1, $2, $3, $4, $5 from @my_s3_stage/product_category_name_translation/product_category_name_translation_202307114.csv;

create or replace pipe BRONZE_LAYER.olist_sellers_dataset_pipe
AUTO_INGEST=TRUE
as
copy into project.bronze_layer.olist_sellers_dataset
from @my_s3_stage
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

show pipes;

-- select SYSTEM$PIPE_STATUS( 'olist_customers_dataset_pipe' );