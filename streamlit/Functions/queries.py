import snowflake.connector
import pandas as pd
import os
# Set up the connection parameters
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user= os.getenv("user"),
        password=os.getenv("password"),
        account=os.getenv("account"),
        database=os.getenv("database"),
        schema=os.getenv("schema")
    )

    # Create a cursor object to execute SQL queries
    cur = conn.cursor()

    return conn, cur

def query_22(DMS):
    """
    Execute a SQL query to calculate the average quantity on hand for each product name, brand, class, and category,
    and rollup data by product name, brand, class, and category.

    Parameters:
    - DMS (int): A substitution parameter used in the SQL query. The query will return data for 12 months starting
                 from the month specified by this parameter.

    Returns:
    - res_df (pandas dataframe): The result of the SQL query in a pandas dataframe.
    """

    

    # write a query
    query = """
    -- start query 22 in stream 0 using template query22.tpl and seed QUALIFICATION
    select  
    i_product_name,
    i_brand,
    i_class,
    i_category,
    avg(inv_quantity_on_hand) qoh
    from inventory
        ,date_dim
        ,item
    where inv_date_sk=d_date_sk
            and inv_item_sk=i_item_sk
            and d_month_seq between {0} and ({0} + 11)
    group by rollup(i_product_name
                    ,i_brand
                        ,i_class
                        ,i_category)
    order by qoh, i_product_name, i_brand, i_class, i_category
    limit 100;
    -- end query 22 in stream 0 using template query22.tpl
        """.format(DMS)

    conn, cur = connect_to_snowflake()

    # Execute a SQL query
    cur.execute(query)
     # Fetch all rows of the query result
    rows = cur.fetchall()
    # Get the column names from the description attribute of the cursor object
    col_names = [desc[0] for desc in cur.description]
    # Create pandas dataframe
    res_df = pd.DataFrame(rows, columns=col_names)
    # Close the cursor and connection
    cur.close()
    conn.close()
    return res_df

def query_19(month, year, manager_id):
    """
    Select the top revenue generating products bought by out of zip code customers for a given year, month and
    manager. Qualification Substitution Parameters
    • MANAGER.01 = 8 = [1,100]
    • MONTH.01 = 11 = [1900,2100]
    • YEAR.01 = 1998 = [1,12]
    
    
    """
    # write a query
    query = """
 -- start query 19 in stream 0 using template query19.tpl and seed QUALIFICATION
  select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id= {0}
   and d_moy= {1}
   and d_year= {2}
   and ss_customer_sk = c_customer_sk 
   and c_current_addr_sk = ca_address_sk
   and substr(ca_zip,1,5) <> substr(s_zip,1,5) 
   and ss_store_sk = s_store_sk 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
 limit 100 ;
-- end query 19 in stream 0 using template query19.tpl
        """.format(manager_id,month,year)

    conn, cur = connect_to_snowflake()

    # Execute a SQL query
    cur.execute(query)
     # Fetch all rows of the query result
    rows = cur.fetchall()
    # Get the column names from the description attribute of the cursor object
    col_names = [desc[0] for desc in cur.description]
    # Create pandas dataframe
    res_df = pd.DataFrame(rows, columns=col_names)
    # Close the cursor and connection
    cur.close()
    conn.close()
    return res_df

def query_17(year):
    """
    query17.tpl
    Analyze, for each state, all items that were sold in stores in a particular quarter and returned in the next three
    quarters and then re-purchased by the customer through the catalog channel in the three following quarters.
    Qualification Substitution Parameters:
    
    YEAR.01 = 2001

    """
    # write a query
    query = """
 -- start query 17 in stream 0 using template query17.tpl and seed QUALIFICATION
  select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '{0}Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('{0}Q1','{0}Q2','{0}Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('{0}Q1','{0}Q2','{0}Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
 limit 100;
-- end query 17 in stream 0 using template query17.tpl
        """.format(year)

    conn, cur = connect_to_snowflake()

    # Execute a SQL query
    cur.execute(query)
     # Fetch all rows of the query result
    rows = cur.fetchall()
    # Get the column names from the description attribute of the cursor object
    col_names = [desc[0] for desc in cur.description]
    # Create pandas dataframe
    res_df = pd.DataFrame(rows, columns=col_names)
    # Close the cursor and connection
    cur.close()
    conn.close()
    return res_df

def query_15(d_qoy,d_year):
    """
   Report the total catalog sales for customers in selected geographical regions or who made large purchases for a
    given year and quarter.
    Qualification Substitution Parameters:
    • QOY.01 = 2 =[1,4]
    • YEAR.01 = 2001

    """
    # write a query
    query = """
    -- start query 15 in stream 0 using template query15.tpl and seed QUALIFICATION
    select  ca_zip
        ,sum(cs_sales_price)
    from catalog_sales
        ,customer
        ,customer_address
        ,date_dim
    where cs_bill_customer_sk = c_customer_sk
        and c_current_addr_sk = ca_address_sk 
        and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                    '85392', '85460', '80348', '81792')
            or ca_state in ('CA','WA','GA')
            or cs_sales_price > 500)
        and cs_sold_date_sk = d_date_sk
        and d_qoy = {0} and d_year = {1}
    group by ca_zip
    order by ca_zip
    limit 100;
    -- end query 15 in stream 0 using template query15.tpl
        """.format(d_qoy,d_year)
    

    conn, cur = connect_to_snowflake()

    # Execute a SQL query
    cur.execute(query)
     # Fetch all rows of the query result
    rows = cur.fetchall()
    # Get the column names from the description attribute of the cursor object
    col_names = [desc[0] for desc in cur.description]
    # Create pandas dataframe
    res_df = pd.DataFrame(rows, columns=col_names)
    # Close the cursor and connection
    cur.close()
    conn.close()
    return res_df

def query_26(cd_gender,cd_marital_status,cd_education_status,d_year):
    """
    Computes the average quantity, list price, discount, sales price for promotional items sold through the catalog
    channel where the promotion was not offered by mail or in an event for given gender, marital status and
    educational status.
    Qualification Substitution Parameters:
    • YEAR.01 = 2000
    • ES.01 = College = { Secondary, Advanced Degree, 2 yr Degree, 4 yr Degree, Unknown, Primary, College}
    • MS.01 = S = { S, D, W, U, M }
    • GEN.01 = M = {M, F}

    """
    # write a query
    query = """
    -- start query 26 in stream 0 using template query26.tpl and seed QUALIFICATION
    select  i_item_id, 
            avg(cs_quantity) agg1,
            avg(cs_list_price) agg2,
            avg(cs_coupon_amt) agg3,
            avg(cs_sales_price) agg4 
    from catalog_sales, customer_demographics, date_dim, item, promotion
    where cs_sold_date_sk = d_date_sk and
        cs_item_sk = i_item_sk and
        cs_bill_cdemo_sk = cd_demo_sk and
        cs_promo_sk = p_promo_sk and
        cd_gender = '{0}' and 
        cd_marital_status = '{1}' and
        cd_education_status = '{2}' and
        (p_channel_email = 'N' or p_channel_event = 'N') and
        d_year = {3} 
    group by i_item_id
    order by i_item_id
    limit 100;
    -- end query 26 in stream 0 using template query26.tpl
        """.format(cd_gender,cd_marital_status,cd_education_status,d_year)

    conn, cur = connect_to_snowflake()

    # Execute a SQL query
    cur.execute(query)
     # Fetch all rows of the query result
    rows = cur.fetchall()
    # Get the column names from the description attribute of the cursor object
    col_names = [desc[0] for desc in cur.description]
    # Create pandas dataframe
    res_df = pd.DataFrame(rows, columns=col_names)
    # Close the cursor and connection
    cur.close()
    conn.close()
    return res_df

