CREATE OR REPLACE PROCEDURE bronze_to_silver_loading(bronze_schema_name string, silver_schema_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, regexp_replace, lower
import datetime as dt


def fill_null_in_columns(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Fill null's value in string, date, timestamp columns
    """
    list_of_columns = [(f.name, f.datatype.__str__().split('Type')[0].lower()) for f in df.schema.fields]
    list_of_string_columns = [i[0] for i in list_of_columns if i[1] == 'string']
    list_of_datetime_columns = [i[0] for i in list_of_columns if i[1] in ('datetime', 'timestamp')]

    for i in list_of_datetime_columns:
        df = df.withColumn(i, when(col(i).isNull(), dt.date(1900, 1, 1)).otherwise(col(i)))

    return df.na.fill(value="<Underfind>", subset=list_of_string_columns)


def delete_unnecessary_symbols(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Delete unnecessary symbols in sting columns
    """
    list_of_columns = [(f.name, f.datatype.__str__().split('Type')[0].lower()) for f in df.schema.fields]
    list_of_string_columns = [i[0] for i in list_of_columns if i[1] == 'string']
    
    for column in list_of_string_columns:
        for symbol in ["\!", "\#", "\^", "\$", "\@", "\&", "\*", "\.", "\,", "\?" "\"", "\'", "/", "|"]:
            df = df.withColumn(column, lower(regexp_replace(column, symbol, "")))

    return df


def upload_table(session, table_name, schema_name):
    """
    table_name (string): name of table
    schema_name (string): name of schema

    Upload table from snowflake for table name and schema name
    """
    return session.table(schema_name + '.' + table_name)


def save_table(df, table_name, schema_name):
    """
    df (DataFrame): input dataframe that need to save
    table_name (string): name of table
    schema_name (string): name of schema

    Save table into snowflake for table name and schema name
    """
    df.write.mode("overwrite").saveAsTable(schema_name + '.' + table_name)


def main(session: snowpark.Session, bronze_schema_name, silver_schema_name):     
    list_of_tables = [
        "olist_customers_dataset",
        "olist_geolocation_dataset",
        "olist_orders_dataset",
        "olist_order_items_dataset",
        "olist_order_payments_dataset",
        "olist_order_reviews_dataset",
        "olist_products_dataset",
        "olist_sellers_dataset",
        "product_category_name_translation"
    ]
    list_of_datasets = {name_of_table: upload_table(session, name_of_table, bronze_schema_name) for name_of_table in list_of_tables}

    for table_name in list_of_tables:
        # Fill null value in string, date, timestamp columns with value <Underfind>, 1900-01-01, 1900-01-01
        list_of_datasets[table_name] = fill_null_in_columns(list_of_datasets[table_name])
        # Delete unnecessary symbols in string columns like: . , / ? ! @ # and e.t.
        list_of_datasets[table_name] = delete_unnecessary_symbols(list_of_datasets[table_name])
        
        # Delete olist from begining and dataset from ending in table name
        list_of_datasets[table_name.replace("olist_", "").replace("_dataset", "")] = list_of_datasets[table_name]
        del list_of_datasets[table_name]

    for name_of_table in list_of_datasets.keys():
        save_table(list_of_datasets[name_of_table], name_of_table, silver_schema_name)

    return 'Loading from bronze to silver is successful'
$$;

call bronze_to_silver_loading('bronze_layer', 'silver_layer');




CREATE OR REPLACE PROCEDURE silver_to_gold_layer_customers_table(silver_schema_name string, golden_schema_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, regexp_replace, lower
import datetime as dt


def replace_to_friendly_columns_name(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Replace all columns in the dataset for pleasant to the eyes in Power BI 
    """
    list_of_columns = [f.name for f in df.schema.fields]
    list_of_new_columns = [" ".join([i.lower().capitalize() for i in f.name.split("_")]) for f in df.schema.fields]
    for column_id in range(len(list_of_columns)):
        df = df.withColumnRenamed(list_of_columns[column_id], list_of_new_columns[column_id])

    return df


def upload_table(session, table_name, schema_name):
    """
    table_name (string): name of table
    schema_name (string): name of schema

    Upload table from snowflake for table name and schema name
    """
    return session.table(schema_name + '.' + table_name)


def save_table(df, table_name, schema_name):
    """
    df (DataFrame): input dataframe that need to save
    table_name (string): name of table
    schema_name (string): name of schema

    Save table into snowflake for table name and schema name
    """
    df.write.mode("overwrite").saveAsTable(schema_name + '.' + table_name)


def main(session: snowpark.Session, silver_schema_name, golden_schema_name): 
    table_name="customers"
    dim_table_name = "dim_customer"
    
    df_customers = upload_table(session, table_name, silver_schema_name)
    
    df_source = (
        df_customers
        .select(
             col("CUSTOMER_ID")
            ,col("CUSTOMER_UNIQUE_ID")
            ,col("CUSTOMER_ZIP_CODE_PREFIX")
            ,col("CUSTOMER_CITY")
            ,col("CUSTOMER_STATE")
        )
    )

    df_source = replace_to_friendly_columns_name(df_source)

    save_table(df_source, dim_table_name, golden_schema_name)

    return 'Loading from silver to gold of ' + dim_table_name + ' is successful'
$$;

call silver_to_gold_layer_customers_table('customers', 'dim_customer', 'silver_layer', 'gold_layer');



CREATE OR REPLACE PROCEDURE silver_to_gold_layer_geolocation_table(silver_schema_name string, golden_schema_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, regexp_replace, lower
import datetime as dt


def replace_to_friendly_columns_name(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Replace all columns in the dataset for pleasant to the eyes in Power BI 
    """
    list_of_columns = [f.name for f in df.schema.fields]
    list_of_new_columns = [" ".join([i.lower().capitalize() for i in f.name.split("_")]) for f in df.schema.fields]
    for column_id in range(len(list_of_columns)):
        df = df.withColumnRenamed(list_of_columns[column_id], list_of_new_columns[column_id])

    return df


def upload_table(session, table_name, schema_name):
    """
    table_name (string): name of table
    schema_name (string): name of schema

    Upload table from snowflake for table name and schema name
    """
    return session.table(schema_name + '.' + table_name)


def save_table(df, table_name, schema_name):
    """
    df (DataFrame): input dataframe that need to save
    table_name (string): name of table
    schema_name (string): name of schema

    Save table into snowflake for table name and schema name
    """
    df.write.mode("overwrite").saveAsTable(schema_name + '.' + table_name)


def main(session: snowpark.Session, silver_schema_name, golden_schema_name): 
    table_name="geolocation"
    dim_table_name = "dim_geolocation"
    
    df_geolocation = upload_table(session, table_name, silver_schema_name)
    
    df_source = (
        df_geolocation
        .select(
             col("GEOLOCATION_ZIP_CODE_PREFIX")
            ,col("GEOLOCATION_LAT")
            ,col("GEOLOCATION_LNG")
            ,col("GEOLOCATION_CITY")
            ,col("GEOLOCATION_STATE")
        )
    )

    df_source = replace_to_friendly_columns_name(df_source)

    save_table(df_source, dim_table_name, golden_schema_name)

    return 'Loading from silver to gold of ' + dim_table_name + ' is successful'
$$;





CREATE OR REPLACE PROCEDURE silver_to_gold_layer_project_table(silver_schema_name string, golden_schema_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, regexp_replace, lower
import datetime as dt


def replace_to_friendly_columns_name(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Replace all columns in the dataset for pleasant to the eyes in Power BI 
    """
    list_of_columns = [f.name for f in df.schema.fields]
    list_of_new_columns = [" ".join([i.lower().capitalize() for i in f.name.split("_")]) for f in df.schema.fields]
    for column_id in range(len(list_of_columns)):
        df = df.withColumnRenamed(list_of_columns[column_id], list_of_new_columns[column_id])

    return df


def upload_table(session, table_name, schema_name):
    """
    table_name (string): name of table
    schema_name (string): name of schema

    Upload table from snowflake for table name and schema name
    """
    return session.table(schema_name + '.' + table_name)


def save_table(df, table_name, schema_name):
    """
    df (DataFrame): input dataframe that need to save
    table_name (string): name of table
    schema_name (string): name of schema

    Save table into snowflake for table name and schema name
    """
    df.write.mode("overwrite").saveAsTable(schema_name + '.' + table_name)


def main(session: snowpark.Session, silver_schema_name, golden_schema_name): 
    table_product_name="products"
    table_product_translation_name = "PRODUCT_CATEGORY_NAME_TRANSLATION"
    dim_table_name = "dim_product"
    
    df_project = upload_table(session, table_product_name, silver_schema_name)
    df_product_translation_name = upload_table(session, table_product_translation_name, silver_schema_name)
    
    df_product_translation_name_new = df_product_translation_name\
                                      .groupBy("PRODUCT_CATEGORY_NAME", "PRODUCT_CATEGORY_NAME_ENGLISH")\
                                      .count().select(
                                          "PRODUCT_CATEGORY_NAME", "PRODUCT_CATEGORY_NAME_ENGLISH"
                                          )
    
    df_source = (
        df_project
        .withColumn('PRODUCT_CATEGORY_NAME_TEMP', col('PRODUCT_CATEGORY_NAME'))
        .join(df_product_translation_name_new, 
              df_project.PRODUCT_CATEGORY_NAME == df_product_translation_name_new.PRODUCT_CATEGORY_NAME, 
              "left")
        .select(
             col("PRODUCT_ID")
            ,col("PRODUCT_CATEGORY_NAME_TEMP")
            ,col("PRODUCT_CATEGORY_NAME_ENGLISH")
            ,col("PRODUCT_PHOTOS_QTY")
            ,col("PRODUCT_WEIGHT_G")
            ,col("PRODUCT_LENGTH_CM")
            ,col("PRODUCT_HEIGHT_CM")
            ,col("PRODUCT_WIDTH_CM")
        )
    )

    df_source = replace_to_friendly_columns_name(df_source)

    save_table(df_source, dim_table_name, golden_schema_name)

    return 'Loading from silver to gold of ' + dim_table_name + ' is successful'
$$;



CREATE OR REPLACE PROCEDURE silver_to_gold_layer_sellers_table(silver_schema_name string, golden_schema_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, regexp_replace, lower
import datetime as dt


def replace_to_friendly_columns_name(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Replace all columns in the dataset for pleasant to the eyes in Power BI 
    """
    list_of_columns = [f.name for f in df.schema.fields]
    list_of_new_columns = [" ".join([i.lower().capitalize() for i in f.name.split("_")]) for f in df.schema.fields]
    for column_id in range(len(list_of_columns)):
        df = df.withColumnRenamed(list_of_columns[column_id], list_of_new_columns[column_id])

    return df


def upload_table(session, table_name, schema_name):
    """
    table_name (string): name of table
    schema_name (string): name of schema

    Upload table from snowflake for table name and schema name
    """
    return session.table(schema_name + '.' + table_name)


def save_table(df, table_name, schema_name):
    """
    df (DataFrame): input dataframe that need to save
    table_name (string): name of table
    schema_name (string): name of schema

    Save table into snowflake for table name and schema name
    """
    df.write.mode("overwrite").saveAsTable(schema_name + '.' + table_name)


def main(session: snowpark.Session, silver_schema_name, golden_schema_name): 
    table_name="sellers"
    dim_table_name = "dim_sellers"
    
    df_sellers = upload_table(session, table_name, silver_schema_name)
    
    df_source = (
        df_sellers
        .select(
             col("SELLER_ID")
            ,col("SELLER_ZIP_CODE_PREFIX")
            ,col("SELLER_CITY")
            ,col("SELLER_STATE")
        )
    )

    df_source = replace_to_friendly_columns_name(df_source)

    save_table(df_source, dim_table_name, golden_schema_name)

    return 'Loading from silver to gold of ' + table_name + ' is successful'
$$;



CREATE OR REPLACE PROCEDURE silver_to_gold_layer_sales_table(silver_schema_name string, golden_schema_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, regexp_replace, lower
import datetime as dt


def replace_to_friendly_columns_name(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Replace all columns in the dataset for pleasant to the eyes in Power BI 
    """
    list_of_columns = [f.name for f in df.schema.fields]
    list_of_new_columns = [" ".join([i.lower().capitalize() for i in f.name.split("_")]) for f in df.schema.fields]
    for column_id in range(len(list_of_columns)):
        df = df.withColumnRenamed(list_of_columns[column_id], list_of_new_columns[column_id])

    return df


def upload_table(session, table_name, schema_name):
    """
    table_name (string): name of table
    schema_name (string): name of schema

    Upload table from snowflake for table name and schema name
    """
    return session.table(schema_name + '.' + table_name)


def save_table(df, table_name, schema_name):
    """
    df (DataFrame): input dataframe that need to save
    table_name (string): name of table
    schema_name (string): name of schema

    Save table into snowflake for table name and schema name
    """
    df.write.mode("overwrite").saveAsTable(schema_name + '.' + table_name)


def main(session: snowpark.Session, silver_schema_name, golden_schema_name): 
    table_order="ORDERS"
    table_order_items="ORDER_ITEMS"
    table_order_payments="ORDER_PAYMENTS"
    table_order_reviews="ORDER_REVIEWS"
    fact_table_name = "fact_sales"
    
    df_order = upload_table(session, table_order, silver_schema_name)
    df_order_items = upload_table(session, table_order_items, silver_schema_name)
    df_order_payments = upload_table(session, table_order_payments, silver_schema_name)
    df_order_reviews = upload_table(session, table_order_reviews, silver_schema_name)
    
    df_source = (
        df_order
        .withColumn('ORDER_ID_TEMP', col('ORDER_ID'))
        .join(df_order_items, df_order.ORDER_ID == df_order_items.ORDER_ID, 'left')
        .join(df_order_payments, df_order.ORDER_ID == df_order_payments.ORDER_ID, 'left')
        .join(df_order_reviews, df_order.ORDER_ID == df_order_reviews.ORDER_ID, 'left')
        .select(
             col('ORDER_ID_TEMP')
            ,col("CUSTOMER_ID")
            ,col("ORDER_STATUS")
            ,col("ORDER_PURCHASE_TIMESTAMP")
            ,col("ORDER_APPROVED_AT")
            ,col("ORDER_DELIVERED_CARRIER_DATE")
            ,col("ORDER_DELIVERED_CUSTOMER_DATE")
            ,col("ORDER_ESTIMATED_DELIVERY_DATE")
            ,col("ORDER_ITEM_ID")
            ,col("SELLER_ID")
            ,col("SHIPPING_LIMIT_DATE")
            ,col("PRICE")
            ,col("FREIGHT_VALUE")
            ,col("PAYMENT_SEQUENTIAL")
            ,col("PRODUCT_ID")
            ,col("PAYMENT_TYPE")
            ,col("PAYMENT_INSTALLMENTS")
            ,col("PAYMENT_VALUE")
            ,col("REVIEW_ID")
            ,col("REVIEW_SCORE")
            ,col("REVIEW_COMMENT_TITLE")
            ,col("REVIEW_COMMENT_MESSAGE")
            ,col("REVIEW_CREATION_DATE")
            ,col("REVIEW_ANSWER_TIMESTAMP")
        )
    )

    df_source = replace_to_friendly_columns_name(df_source)

    save_table(df_source, fact_table_name, golden_schema_name)

    return 'Loading from silver to gold of ' + fact_table_name + ' is successful'
$$;


CREATE OR REPLACE PROCEDURE silver_to_gold_layer_date_table(silver_schema_name string, golden_schema_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, regexp_replace, lower
import datetime as dt


def replace_to_friendly_columns_name(df):
    """
    df (DataFrame): input dataframe with the data that needs to be changed

    Replace all columns in the dataset for pleasant to the eyes in Power BI 
    """
    list_of_columns = [f.name for f in df.schema.fields]
    list_of_new_columns = [" ".join([i.lower().capitalize() for i in f.name.split("_")]) for f in df.schema.fields]
    for column_id in range(len(list_of_columns)):
        df = df.withColumnRenamed(list_of_columns[column_id], list_of_new_columns[column_id])

    return df


def upload_table(session, table_name, schema_name):
    """
    table_name (string): name of table
    schema_name (string): name of schema

    Upload table from snowflake for table name and schema name
    """
    return session.table(schema_name + '.' + table_name)


def save_table(df, table_name, schema_name):
    """
    df (DataFrame): input dataframe that need to save
    table_name (string): name of table
    schema_name (string): name of schema

    Save table into snowflake for table name and schema name
    """
    df.write.mode("overwrite").saveAsTable(schema_name + '.' + table_name)


def main(session: snowpark.Session, silver_schema_name, golden_schema_name): 
    beginDate = '2000-01-01'
    endDate = '2050-12-31'
    silver_schema_name = 'silver_layer'
    dim_table_name = 'dim_date'
    
    query = f"""
        with a as (
                select -1 + row_number() over(order by 0) as i, start_date + i as calendarDate 
                from (select '{beginDate}'::date start_date, '{endDate}'::date end_date)
                join table(generator(rowcount => 10000 )) x
                qualify i < 1 + end_date - start_date
            ),
            b as (
                select
                year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
                CalendarDate,
                year(calendarDate) AS CalendarYear,
                month(calendarDate) as CalendarMonth,
                month(calendarDate) as MonthOfYear,
                day(calendarDate) as CalendarDay,
                dayofweek(calendarDate) AS DayOfWeek,
                week(calendarDate) + 1 as DayOfWeekStartMonday,
                case
                    when week(calendarDate) < 5 then 'Y'
                    else 'N'
                end as IsWeekDay,
                dayofmonth(calendarDate) as DayOfMonth,
                case
                    when calendarDate = last_day(calendarDate) then 'Y'
                    else 'N'
                end as IsLastDayOfMonth,
                dayofyear(calendarDate) as DayOfYear,
                weekofyear(calendarDate) as WeekOfYearIso,
                quarter(calendarDate) as QuarterOfYear,
                /* Use fiscal periods needed by organization fiscal calendar */
                case
                    when month(calendarDate) >= 10 then year(calendarDate) + 1
                    else year(calendarDate)
                end as FiscalYearOctToSep,
                (month(calendarDate) + 2) % 12 + 1 AS FiscalMonthOctToSep,
                case
                    when month(calendarDate) >= 7 then year(calendarDate) + 1
                    else year(calendarDate)
                end as FiscalYearJulToJun,
                (month(calendarDate) + 5) % 12 + 1 AS FiscalMonthJulToJun
                from
                a
                order by
                calendarDate
        )
        select * from b
    """
    
    date_df = session.sql(query)
    date_df = replace_to_friendly_columns_name(date_df)
    save_table(date_df, dim_table_name, 'gold_layer')

    return 'Loading from silver to gold of ' + dim_table_name + ' is successful'
$$;



CREATE or replace TASK snowflake_pipeline
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  SCHEDULE = 'USING CRON  0 * * * * America/Los_Angeles'
  AS
  EXECUTE IMMEDIATE
  $$
  BEGIN
    call bronze_to_silver_loading('bronze_layer', 'silver_layer');
    
    call silver_to_gold_layer_date_table('silver_layer', 'gold_layer');
    call silver_to_gold_layer_sales_table('silver_layer', 'gold_layer');
    call silver_to_gold_layer_sellers_table('silver_layer', 'gold_layer');
    call silver_to_gold_layer_project_table('silver_layer', 'gold_layer');
    call silver_to_gold_layer_geolocation_table('silver_layer', 'gold_layer');
    call silver_to_gold_layer_customers_table('silver_layer', 'gold_layer');
  END;
  $$;





call bronze_to_silver_loading('bronze_layer', 'silver_layer');


table_name = 'customers'
dim_table_name = "dim_customer"
silver_schema_name = 'silver_layer'
golden_schema_name = 'gold_layer'