#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 11 22:59:23 2020

@author: parulgaba
"""

'''
# Run these only once
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext

from pyspark.sql.functions import desc

sc = SparkContext(appName="PythonStreamingQueueStream") 
sqlContext = SQLContext(sc)
'''
"""
import csv
import os
import pandas as pd
from os.path import basename
import pymysql
import glob
from datetime import datetime


connection_details = {
    'name': 'local',
    'conn': '127.0.0.1',
    'user': 'user',
    'password': 'welcome',
    'sqlDb': 'ethos-sales'
}

try:
    myDb = pymysql.connect(host=connection_details['conn'], user=connection_details['user'], password=connection_details['password'])
except Exception as e:
    raise e
    

cur = myDb.cursor()

dirpath = '/Users/parulgaba/Desktop/Capstone-Ethos/ConfidentialData/csvdata/'

data_path = '/Users/parulgaba/Desktop/Capstone-Ethos/ethos-retail-model/data/'

path = dirpath + '*.csv'

'''
for f in os.listdir(dirpath):
    os.rename(os.path.join(dirpath, f), os.path.join(dirpath, f).replace(' ', '_'))
    
transfer_data_cols = "`,`".join([str(i) for i in transfer_data.columns])


for i,row in transfer_data.iterrows():
    sql = "INSERT INTO `transfer` (`" +transfer_data_cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cur.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    myDb.commit()
    
    That means we first do the closing stock join with purchase and transfer

That will be our available table - Then we join sales data on it.
Condition being item_no, location_code, [sales_date in closing_week or ______CONDITION WHICH MATCHES OTHER ITEMS___]
'''    

# Load annual data
sales3 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.19 to 31.03.20.csv")
sales2 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.17 to 31.03.18.csv")
sales1 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.18 to 31.03.19.csv")
sales_data = sales1.unionAll(sales2)


purchase1 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Purchase_Data_01.04.17_to_31.03.18.csv")
purchase2 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Purchase_Data_01.04.18_to_31.03.19.csv")
purchase3 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Purchase_Data_01.04.19_to_06.02.2020.csv")
purchase_data = purchase1.unionAll(purchase2).unionAll(purchase3)

transfer1 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Transfer_Data_01.04.17_to_31.03.18.csv")
transfer2 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Transfer_Data_01.04.18_to_31.03.19.csv")
transfer3 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Transfer_Data_01.04.19_to_06.02.2020.csv")
transfer_data = transfer1.unionAll(transfer2).unionAll(transfer3)


sales_data.createOrReplaceTempView("sales_raw")
purchase_data.createOrReplaceTempView("purchase_raw")
transfer_data.createOrReplaceTempView("transfer_raw")
   
# Load master data
item_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Attributes_Encoded_final.csv")
item_master = item_master.drop('s_no')
item_master_old = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Master.csv")
item_master = item_master.unionAll(item_master_old)
item_master = item_master.dropDuplicates(subset = ['item_no'])

item_master.createOrReplaceTempView("item_master") 
# load weekly closing data
weekly_closing_dir_path = '/Users/parulgaba/Desktop/Capstone-Ethos/Encoded/weekly-closing-stock/'
weekly_closing_file_path = weekly_closing_dir_path + '*.csv'

weekly_closing_output_path = data_path + 'closing_stock_data.csv'

ethos_transaction_summary = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path + 'ethos_transaction_summary_v2.csv')

# Before reading closing stock, add closing date column to the files using file name.
# Ideally date must be put along with other cloumns while generatin closing stock data
for f in glob.glob(weekly_closing_file_path):
    print(f)
    temp_date = os.path.splitext(os.path.basename(f))[0].split('_')[1]
    
    for dateformat in ('%d.%m.%y', '%d.%m.%Y'):
        try:
            closing_date = datetime.datetime.strptime(temp_date, dateformat).strftime('%Y/%m/%d')
            print(closing_date)
        except:
            print('passing for ' + temp_date)
            pass
    
    csv_input = pd.read_csv(f)
    csv_input.head()
    csv_input['closing_date'] = closing_date
    csv_input.head()
    csv_input.to_csv(f, index=False, encoding='utf-8')

extension = 'csv'
all_filenames = [i for i in glob.glob(weekly_closing_dir_path + '/*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
combined_csv.dropna(subset=['Item No_', 'Location Code']).to_csv(weekly_closing_output_path, index=False, encoding='utf-8')

closing_stock_data = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(weekly_closing_output_path)

# closing_stock_data = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path + 'closing_stock_data.csv')
closing_stock_data.createOrReplaceTempView("closing_stock_raw")


'''         
for f in glob.glob(path):
    print (f)
'''
    
## Aggregate duplicate closing tock file by item_no,location_code,closing_date 

###CASE WHEN to_date(closing_date, 'yyyy/MM/dd') = to_date('2019/03/31', 'yyyy/MM/dd')
## THEN CONCAT('W01', '-FY', (year(date_sub(closing_date, 90))) % 100 + 1, year(date_sub(closing_date, 90)) % 100 + 2)
## ELSE sdssdf END

filepath = '/Users/parulgaba/Desktop/Capstone-Ethos/ethos-retail-model/data-engineering/ethos-load-to-pysql.py'
"""

dirpath = '/Users/parulgaba/Desktop/Capstone-Ethos/ConfidentialData/csvdata/'

data_path = '/Users/parulgaba/Desktop/Capstone-Ethos/ethos-retail-model/data/'

sales3 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.19 to 31.03.20.csv")
sales2 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.17 to 31.03.18.csv")
sales1 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.18 to 31.03.19.csv")
sales_data = sales1.unionAll(sales2)


purchase1 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Purchase_Data_01.04.17_to_31.03.18.csv")
purchase2 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Purchase_Data_01.04.18_to_31.03.19.csv")
purchase3 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Purchase_Data_01.04.19_to_06.02.2020.csv")
purchase_data = purchase1.unionAll(purchase2).unionAll(purchase3)

transfer1 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Transfer_Data_01.04.17_to_31.03.18.csv")
transfer2 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Transfer_Data_01.04.18_to_31.03.19.csv")
transfer3 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Transfer_Data_01.04.19_to_06.02.2020.csv")
transfer_data = transfer1.unionAll(transfer2).unionAll(transfer3)


sales_data.createOrReplaceTempView("sales_raw")
purchase_data.createOrReplaceTempView("purchase_raw")
transfer_data.createOrReplaceTempView("transfer_raw")
   
# Load master data
item_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Attributes_Encoded_final.csv")
item_master = item_master.drop('s_no')
item_master_old = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Master.csv")
item_master = item_master.unionAll(item_master_old)
item_master = item_master.dropDuplicates(subset = ['item_no'])

item_master.createOrReplaceTempView("item_master") 

weekly_closing_output_path = data_path + 'closing_stock_data.csv'

closing_stock_data = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(weekly_closing_output_path)
closing_stock_data.createOrReplaceTempView("closing_stock_raw")

closing_sql = """select
location_code,
item_no,
to_date(closing_date, 'yyyy/MM/dd') closing_date,
CASE WHEN closing_date = to_date('2019/03/31', 'yyyy/MM/dd')
 THEN 'W01-FY1920'
 WHEN closing_date = to_date('2020/03/29', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
ELSE CONCAT('W', lpad(weekofyear(date_sub(closing_date, 90)),2,0), '-FY', year(date_sub(closing_date, 90)) % 100, year(date_sub(closing_date, 90)) % 100 + 1) END week,
first(brand) brand,
first(department) department,
sum(quantity) quantity,
sum(cost_amount) cost_amount,
sum(purchase_mrp) purchase_mrp,
sum(stock_prevailing_mrp) stock_prevailing_mrp,
first(to_date(purchase_date, 'yyyy/MM/dd')) purchase_date,
first(state) state,
first(region) region
from closing_stock_raw 
where closing_date is not null
group by item_no,location_code,closing_date"""

closing_stock_table = sqlContext.sql(closing_sql)
closing_stock_table.createOrReplaceTempView("closing_stock")

# 'yyyy/MM/dd'

sales_sql = '''select
     `Store No_` location_code,
     to_date(`Date`, 'yyyy/MM/dd') sales_date,
     cast(`Item No_` as int) item_no,
     first(`Brand`) brand,
     first(`Department`) sales_department,
     count(`Customer No_`) customer_no,
     sum(cast(Quantity as int)) quantity,
     sum(float(`Price`)) price,
     sum(float(`Total Price`)) total_price,
     sum(float(`Line Discount Amount`)) line_discount,
     sum(float(`CRM Line Disc_ Amount`)) crm_line_discount,
     sum(float(`Discount Amount`)) discount,
     sum(float(`Tax Amount`)) tax,
     sum(float(`Cost Amount`)) cost,
     sum(float(`Billing`)) billing,
     sum(float(`Contribution`)) contribution,
     first(to_date(`Receipt Date`, 'yyyy/MM/dd')) receipt_date,
     sum(float(`Trade Incentive %`)) trade_incentive,
     sum(float(`Trade Incentives Value`)) trade_incentive_value,
     sum(float(`Total Contribution`)) total_contribution,
     first(`State`) state,
     first(`Region`) region
from sales_raw
where `Date` is not null
group by 1,2,3
'''

sales3.createOrReplaceTempView("sales3")

sales3_sql = """
select
     `Store No_` location_code,
     to_date(`Date`, 'dd/MM/yyyy') sales_date,
     cast(`Item No_` as int) item_no,
     first(`Brand`) brand,
     first(`Department`) sales_department,
     count(`Customer No_`) customer_no,
     sum(cast(Quantity as int)) quantity,
     sum(float(`Price`)) price,
     sum(float(`Total Price`)) total_price,
     sum(float(`Line Discount Amount`)) line_discount,
     sum(float(`CRM Line Disc_ Amount`)) crm_line_discount,
     sum(float(`Discount Amount`)) discount,
     sum(float(`Tax Amount`)) tax,
     sum(float(`Cost Amount`)) cost,
     sum(float(`Billing`)) billing,
     sum(float(`Contribution`)) contribution,
     first(to_date(`Receipt Date`, 'dd/MM/yy')) receipt_date,
     sum(float(`Trade Incentive %`)) trade_incentive,
     sum(float(`Trade Incentives Value`)) trade_incentive_value,
     sum(float(`Total Contribution`)) total_contribution,
     first(`State`) state,
     first(`Region`) region
from sales3
where `Date` is not null
group by 1,2,3
"""

# CONCAT('W', weekofyear(to_date(`Date`, 'yyyy/MM/dd')), '-FY', year(to_date(`Date`, 'yyyy/MM/dd')) % 100, year(to_date(`Date`, 'yyyy/MM/dd')) % 100 + 1) week

sales_table = sqlContext.sql(sales_sql)
sales_table.createOrReplaceTempView("sales")

sales_3_data = sqlContext.sql(sales3_sql)

sales_3_data.createOrReplaceTempView("sales_3")

sales_table = sales_table.unionAll(sales_3_data)

sales_table.createOrReplaceTempView("sales")


purchase_sql = """select
 `Location Code` location_code,
 to_date(`Posting Date`, 'yyyy/MM/dd')  posting_date,
 `Item No_` item_no,
 `Brand` brand,
 `Department` department,
 `Quantity` quantity,
 `Purchase MRP` purchase_mrp,
 `Cost Amount` cost_amount,
 `State Code` state_code,
 `Region` region
from purchase_raw
where `Posting Date` is not null
"""

purchase_table = sqlContext.sql(purchase_sql)
purchase_table.createOrReplaceTempView("purchase")

transfer_sql = """select `Store Out` store_out, to_date(`Store Out Date`, 'yyyy/MM/dd') store_out_date, `Store In` store_in, to_date(`Store In Date`, 'yyyy/MM/dd') store_in_date, `Item No_` item_no, `Brand` brand, `Product Group Code` product_group_code, `Quantity` quantity, `Cost Amount` cost_amount, float(MRP) mrp, to_date(`Purchase Date`, 'yyyy/MM/dd') purchase_date, `State` state, `Region` region from transfer_raw where `Store In Date` is not null"""

transfer_table = sqlContext.sql(transfer_sql)
transfer_table.createOrReplaceTempView("transfer")

# WEEKLY SUMMARY
# lpad(weekofyear(date_sub(store_in_date, 91)), 2, 0)
# WHEN posting_date = to_date('2019/03/31', 'yyyy/MM/dd')
# THEN CONCAT('W01', '-FY1920')
purchase_weekly_query = """SELECT
 location_code,
 item_no,
  CASE WHEN posting_date = to_date('2019/03/31', 'yyyy/MM/dd')
 THEN 'W52-FY1819'
    WHEN posting_date = to_date('2020/03/29', 'yyyy/MM/dd')
 THEN 'W52-FY1920'
   WHEN posting_date = to_date('2020/03/30', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
    WHEN posting_date = to_date('2020/03/31', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
    WHEN posting_date = to_date('2017/04/02', 'yyyy/MM/dd')
 THEN 'W52-FY1617'
  WHEN posting_date = to_date('2019/04/01', 'yyyy/MM/dd')
 THEN 'W01-FY1920'
 ELSE CONCAT('W', lpad(weekofyear(date_sub(posting_date, 91)),2,0), '-FY', year(date_sub(posting_date, 91)) % 100, year(date_sub(posting_date, 91)) % 100 + 1) END week,
 first(brand) brand,
 first(department) department,
 sum(quantity) quantity,
 sum(purchase_mrp) purchase_mrp,
 sum(cost_amount) cost_amount,
 first(state_code) state,
 first(region) region
from purchase
GROUP BY 1,2,3
"""

purchase_weekly = sqlContext.sql(purchase_weekly_query)
purchase_weekly.createOrReplaceTempView("purchase_weekly")

purchase_join_query = """
SELECT
     CASE WHEN a.location_code is not null THEN a.location_code ELSE b.location_code END location_code,
     CASE WHEN a.item_no is not null THEN a.item_no ELSE b.item_no END item_no,
     CASE WHEN a.week is not null THEN a.week ELSE b.week END week,
     a.closing_date closing_date,
     CASE WHEN a.brand is not null THEN a.brand ELSE b.brand END brand,
     CASE WHEN a.department is not null THEN a.brand ELSE b.department END department,
     CASE WHEN a.state is not null THEN a.state ELSE b.state END state,
     CASE WHEN a.region is not null THEN a.region ELSE b.region END region,
     a.quantity quantity,
     b.quantity purchase_quantity,
     a.cost_amount purchase_cost_amount,
     a.purchase_mrp purchase_mrp,
     a.purchase_date purchase_date,
     CASE when a.stock_prevailing_mrp is not null THEN a.stock_prevailing_mrp ELSE b.purchase_mrp END stock_prevailing_mrp
    from closing_stock a FULL OUTER JOIN purchase_weekly b
ON a.location_code = b.location_code
AND a.item_no = b.item_no
AND b.week = a.week
"""
purchase_join = sqlContext.sql(purchase_join_query)
purchase_join.createOrReplaceTempView("purchase_join")

transfer_weekly_query = """SELECT
 item_no,
 store_in,
  CASE WHEN store_in_date = to_date('2019/03/31', 'yyyy/MM/dd')
 THEN 'W52-FY1819'
   WHEN store_in_date = to_date('2020/03/29', 'yyyy/MM/dd')
 THEN 'W52-FY1920'
   WHEN store_in_date = to_date('2020/03/30', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
    WHEN store_in_date = to_date('2020/03/31', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
      WHEN store_in_date = to_date('2017/04/02', 'yyyy/MM/dd')
 THEN 'W52-FY1617'
   WHEN store_in_date = to_date('2019/04/01', 'yyyy/MM/dd')
 THEN 'W01-FY1920'
 ELSE CONCAT('W', lpad(weekofyear(date_sub(store_in_date, 91)),2,0), '-FY', year(date_sub(store_in_date, 91)) % 100, year(date_sub(store_in_date, 91)) % 100 + 1) END week,
first(brand) brand,
first(product_group_code) product_group_code,
sum(quantity) quantity,
sum(cost_amount) cost_amount,
sum(mrp) mrp,
first(purchase_date) purchase_date,
 first(state) state,
 first(region) region
from transfer
GROUP BY 1,2,3"""

transfer_weekly = sqlContext.sql(transfer_weekly_query)
transfer_weekly.createOrReplaceTempView("transfer_weekly")

transfer_in_join_query = """select
    CASE WHEN a.location_code is not null THEN a.location_code ELSE b.store_in END location_code,
    CASE WHEN a.item_no is not null THEN a.item_no ELSE b.item_no END item_no,
    CASE WHEN a.week is not null THEN a.week ELSE b.week END week,
    a.closing_date closing_date,
    CASE WHEN a.brand is not null THEN a.brand ELSE b.brand END brand,
    CASE WHEN a.state is not null THEN a.state ELSE b.state END state,
    CASE WHEN a.region is not null THEN a.region ELSE b.region END region,
    a.department,
    a.quantity quantity,
    a.purchase_quantity purchase_quantity,
    b.quantity transfer_quantity,
    a.purchase_cost_amount purchase_cost_amount,
    a.purchase_mrp purchase_mrp,
    a.purchase_date purchase_date,
    CASE when a.stock_prevailing_mrp is not null THEN a.stock_prevailing_mrp ELSE b.mrp END stock_prevailing_mrp,
    b.store_in store_in,
    b.product_group_code,
    b.cost_amount transfer_cost_amount
    from purchase_join a FULL OUTER JOIN transfer_weekly b
    ON a.location_code = b.store_in
    AND a.item_no = b.item_no
   AND b.week = a.week
   """
   
transfer_in_join = sqlContext.sql(transfer_in_join_query)
transfer_in_join.createOrReplaceTempView("transfer_in_join") 

## Calculate avg_days_to_sell = (week(closing_stock.purchase_date)-week(sales_date)) * 7
sales_weekly_query = """select
 b.item_no item_no,
 b.location_code location_code,
 CASE WHEN sales_date = to_date('2019/03/31', 'yyyy/MM/dd')
 THEN 'W52-FY1819'
  WHEN sales_date = to_date('2020/03/29', 'yyyy/MM/dd')
 THEN 'W52-FY1920'
   WHEN sales_date = to_date('2020/03/30', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
    WHEN sales_date = to_date('2020/03/31', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
     WHEN sales_date = to_date('2017/04/02', 'yyyy/MM/dd')
 THEN 'W52-FY1617'
    WHEN sales_date = to_date('2019/04/01', 'yyyy/MM/dd')
 THEN 'W01-FY1920'
 ELSE CONCAT('W', lpad(weekofyear(date_sub(sales_date, 91)),2,0), '-FY', year(date_sub(sales_date, 91)) % 100, year(date_sub(sales_date, 91)) % 100 + 1) END week,
 first(b.sales_department) sales_department,
 count(b.customer_no) num_of_customers,
 sum(b.quantity) quantity,
 sum(b.price) price,
 sum(b.total_price) total_price,
 sum(b.line_discount) line_discount,
 sum(b.crm_line_discount) crm_line_discount,
 sum(b.discount) discount,
 sum(b.tax) tax,
 sum(b.cost) cost,
 sum(b.billing) billing,
 sum(b.contribution) contribution,
 sum(b.trade_incentive) trade_incentive,
 sum(b.trade_incentive_value) trade_incentive_value,
 sum(b.total_contribution) total_contribution,
  first(state) state,
 first(region) region
from sales b
group by 1,2,3
"""
sales_weekly = sqlContext.sql(sales_weekly_query)
sales_weekly.createOrReplaceTempView("sales_weekly")

sales_join_data_query = """SELECT
    CASE WHEN a.location_code is not null THEN a.location_code ELSE b.location_code END location_code,
    CASE WHEN a.item_no is not null THEN a.item_no ELSE b.item_no END item_no,
    CASE WHEN a.week is not null THEN a.week ELSE b.week END week,
    a.closing_date closing_date,
    CASE WHEN a.state is not null THEN a.state ELSE b.state END state,
    CASE WHEN a.region is not null THEN a.region ELSE b.region END region,
    a.brand brand,
    a.department,
    a.quantity quantity,
    a.purchase_quantity purchase_quantity,
    a.transfer_quantity transfer_quantity,
    b.quantity sales_quantity,
    a.purchase_cost_amount purchase_cost_amount,
    a.purchase_mrp purchase_mrp,
    a.purchase_date purchase_date,
    CASE when a.stock_prevailing_mrp is not null THEN a.stock_prevailing_mrp ELSE b.price END stock_prevailing_mrp,
    a.store_in store_in,
    a.product_group_code,
    a.transfer_cost_amount transfer_cost_amount,
    b.sales_department sales_department,
    DATEDIFF(a.closing_date, a.purchase_date) days_to_sell,
    b.num_of_customers,
    b.total_price total_price,
    b.line_discount line_discount,
    b.crm_line_discount crm_line_discount,
    b.discount discount,
    b.tax tax,
    b.cost cost,
    b.billing billing,
    b.contribution contribution,
    b.trade_incentive trade_incentive,
    b.trade_incentive_value trade_incentive_value,
    b.total_contribution total_contribution
from transfer_in_join a FULL OUTER JOIN sales_weekly b
ON a.location_code = b.location_code
AND a.item_no = b.item_no
AND b.week = a.week
"""
sales_join = sqlContext.sql(sales_join_data_query)
sales_join.createOrReplaceTempView("sales_join")


## Join item master and store masterstore_join = sales_join.join(store_master, store_master.store_code == sales_join.location_code, how='left').drop('store_code')

store_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "store_master.csv")
store_master = store_master.drop('state', 'region')
# store_regions = sales_join.filter("region is not null and state is not null").select('location_code', 'region', 'state').distinct()
# store_master = store_master.join(store_regions, store_master.store_code == store_regions.location_code, how = 'left').drop('location_code')

store_join = sales_join.join(store_master, store_master.store_code == sales_join.location_code, how='left').drop('store_code')
store_join = store_join.na.fill(0)

store_join.createOrReplaceTempView("store_join")

item_join_query = """select a.*,
    (a.quantity + a.purchase_quantity + a.transfer_quantity) available_quantity,
    substr(a.week, 0, 3) week_no,
    substr(a.week, 4) year,
    float(split(b.case_size, ' ')[0]) case_size,
    b.case_size_range,
    b.gender,
    b.movement,
    b.material,
    b.dial_color,
    b.strap_type,
    b.strap_color,
    b.precious_stone,
    b.glass,
    b.case_shape,
    b.watch_type
from store_join a LEFT JOIN item_master b
ON a.item_no = b.item_no
"""

ethos_transaction_summary = sqlContext.sql(item_join_query)

# transfer_in_join.select('week', 'closing_date').distinct().show(100)
#ethos_transaction_summary.select('week', 'closing_date').distinct().show(100)

print('Done\n\n Done.')

ethos_transaction_summary.groupBy().sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()

ethos_transaction_summary.filter("week like 'W52-%'").groupBy('week').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()
ethos_transaction_summary.filter("week like 'W01-%'").groupBy('week').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()

#store_regions = ethos_transaction_summary.filter("region is null or state is null").select('location_code', 'region', 'state').distinct()
#store_regions.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'stores_without_region.csv',header = 'true')

#ethos_weekly = ethos_transaction_summary.groupBy('week').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity')
#ethos_weekly.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'test_weekly.csv',header = 'true')


ethos_transaction_summary.groupBy('region').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()

ethos_transaction_summary.select([count(when(col(c).isNull(), c)).alias(c) for c in ethos_transaction_summary.columns]).show()
# ethos_transaction_summary.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'ethos_transaction_summary.csv',header = 'true')
### -------- JOIN LOGIC ENDS HERE --- ONLY TESTING and TALLYING BELOW ------- ####
 
'''
ethos_transaction_summary.filter("closing_date = date'2020-02-02'").sum('sales_quantity', 'price', 'billing','transfer_quantity', 'purchase_quantity')

#check corporate orders
ethos_transaction_summary.filter("sales_quantity > 2").select('item_no', 'location_code', 'sales_quantity', 'closing_date', 'days_to_sell')


unique_items_in_item_master = item_master.select('item_no').distinct() 

unique_items_in_transactional_data = purchase_store_master.select('item_no').distinct() 


unique_items_in_transactional_data.subtract(unique_items_in_transactional_data.intersect(unique_items_in_item_master)).count()


>>> sales_join.groupBy().sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity').show()
+-------------+----------------------+----------------------+-------------------+
|sum(quantity)|sum(purchase_quantity)|sum(transfer_quantity)|sum(sales_quantity)|
+-------------+----------------------+----------------------+-------------------+
|      6802945|                200215|                185772|             196936|
+-------------+----------------------+----------------------+-------------------+

>>> closing_stock_table.groupBy().sum('quantity').show()
+-------------+
|sum(quantity)|
+-------------+
|      6802945|
+-------------+

>>> transfer_table.groupBy().sum('quantity').show()
+--------------------+-------------+215
|            sum(mrp)|sum(quantity)|
+--------------------+-------------+
|2.548899039574707E10|       185772|
+--------------------+-------------+

>>> purchase_table.groupBy().sum('quantity').show()
+-------------+
|sum(quantity)|
+-------------+
|       200215|
+-------------+

>>> sales_table.groupBy().sum('quantity').show()
+-------------+
|sum(quantity)|
+-------------+
|       196936|
+-------------+


MISC TALLY

sales_table.filter("sales_date <= date'2020-02-08' and sales_date > date'2019-07-28'").groupBy().sum('quantity', 'total_price').show()


## Sales quantity that wasn't mapped - Left anti join filters on what's not in the left table that's there in right table
sales_table.filter("sales_date > date'2019-09-08' and sales_date < date'2019-09-23'").join(unique_items, on=['item_no', 'location_code'], how = 'inner').groupBy().sum('quantity', 'total_price', 'billing').show()
sales_table.filter("sales_date <= date'2020-02-08' and sales_date > date'2019-07-28'").join(unique_items, on=['item_no', 'location_code'], how = 'inner').groupBy().sum('quantity', 'total_price', 'billing').show()


ethos_transaction_summary.select('gender').distinct().show()
+------+
|gender|
+------+
|  null|
|   Men|
| Women|
|Unisex|
+------+

Pull missing items in sales

sales_table.filter("sales_date > date'2017-04-02'and sales_date <= date'2017-04-09'").join(ethos_transaction_summary.filter("closing_date == date'2017-04-02'").select('item_no', 'location_code', 'closing_date'), on=['item_no', 'location_code'], how = 'left_anti').select(sales_table["*"])



# Get unique items in summary to avoid tallying with sales/transfer/purchase data not mapped with closing stock weekly data
unique_items = ethos_transaction_summary.select('item_no', 'location_code').distinct()
unique_items_week = ethos_transaction_summary.filter("closing_date == date'2019-09-08'").select('item_no', 'location_code').distinct()


import pyspark.sql.functions as F
sales_join = closing_stock_table.join(sales_table, on=['location_code', 'item_no'],how='left').filter(closing_stock_table.closing_date <= sales_table.sales_date).filter(sales_table.sales_date > F.date_add(closing_stock_table.closing_date, 7))

temp = sqlContext.sql("""select * from sales_join where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))""")
sqlContext.sql("""select sales_date, weekofyear(sales_date), year(sales_date) from sales where DATEDIFF(sales_date, to_date('2018/07/29','yyyy/MM/dd')) < 7 and DATEDIFF(sales_date, to_date('2019/09/08','yyyy/MM/dd')) < 7""")


# Transfer to file
temp.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'purchase_join_2018-07-29 and 2019-09-08.csv',header = 'true')
purchase_item_master.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'purchase_item_master.csv',header = 'true')

transfer_join.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'transfer_join.csv',header = 'true')


sqlContext.sql("""select distinct closing_week, year from closing_stock where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))""").show()

sqlContext.sql("""select * from sales_weekly where sales_week in (30, 36) and sales_year in (2018, 2018)""").show()

sqlContext.sql("""select distinct * from closing_stock where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd')) and item_no in (5120031, 5121915) and location_code = 'S02'""").show()


sales_join = sqlContext.sql(sales_new_join_query)

sales_join.createOrReplaceTempView("sales_join")

temp = sqlContext.sql("""select * from purchase_join where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))""")


#Subset

small_summary = transfer_join.filter("closing_date <= date'2020-02-02' and closing_date >= date'2019-07-28'")

>>> sales_join = sqlContext.sql(sales_new_join_query)
>>> sales_join.createOrReplaceTempView("sales_join")
>>> temp = sqlContext.sql("""select * from transfer_join where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))""")



weeks_sql = select
distinct closing_date
from closing_stock


weeks = sqlContext.sql(weeks_sql)


>>> item_master.count()
31990

>>> purchase_item_master = purchase_store_master.join(item_master, purchase_store_master.item_no == item_master.item_no)
>>> purchase_item_master.count()
5711045

>>> purchase_store_master.select('item_no').distinct().count()
20422

>>> item_master.count()
31990

>>> item_master.select('item_no').distinct().count()
31224

pyspark --num-executors 5 --driver-memory 3g --executor-memory 3g

#AND b.store_in_date > a.closing_date
#AND b.store_in_date <= date_add(a.closing_date, 7)#
#AND weekofyear(a.closing_date) = weekofyear(b.store_in_date)
#AND year(a.closing_date) = year(b.store_in_date)
#AND DATEDIFF(a.closing_date,b.store_in_date) < 7

purchase_store_master.join(item_master, purchase_store_master.item_no == item_master.item_no, how='left').select(purchase_store_master['item_no'], 'case_size_range', 'gender', 'movement', 'material', 'dial_color', 'strap_type', 'strap_color', 'precious_stone', 'glass', 'case_shape', 'watch_type')

closing_sql = """SELECT
`Location Code` location_code,
`Item No_` item_no,
first(`Brand`) brand,
first(`Department`) department,
sum(cast(Quantity as int)) quantity,
sum(float(`Cost Amount`)) cost_amount,
sum(float(`Purchase MRP`)) purchase_mrp,
sum(float(`Prevailing MRP as on Stock Date`)) stock_prevailing_mrp,
first(to_date(`Purchase Date`, 'yyyy/MM/dd')) purchase_date,
first(`State`) state,
first(`Region`) region,
to_date(closing_date, 'yyyy/MM/dd') closing_date
from closing_stock_raw
group by item_no, location_code,closing_date"""


sales_test_sql = """select 
CASE WHEN sales_date = to_date('2019/03/31', 'yyyy/MM/dd')
 THEN 'W52-FY1819'
  WHEN sales_date = to_date('2020/03/29', 'yyyy/MM/dd')
 THEN 'W52-FY1920'
   WHEN sales_date = to_date('2020/03/30', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
    WHEN sales_date = to_date('2020/03/31', 'yyyy/MM/dd')
 THEN 'W53-FY1920'
     WHEN sales_date = to_date('2017/04/02', 'yyyy/MM/dd')
 THEN 'W52-FY1617'
    WHEN sales_date = to_date('2019/04/01', 'yyyy/MM/dd')
 THEN 'W01-FY1920'
 ELSE CONCAT('W', lpad(weekofyear(date_sub(sales_date, 91)),2,0), '-FY', year(date_sub(sales_date, 91)) % 100, year(date_sub(sales_date, 91)) % 100 + 1) END week,
 sales_date
 from sales
"""

sales_test = sqlContext.sql(sales_test_sql)

'''




















