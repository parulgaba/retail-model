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
'''    

# Load annual data
sales3 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.19 to 06.02.2020.csv")
sales2 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.17 to 31.03.18.csv")
sales1 = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Sale Data 01.04.18 to 31.03.19.csv")
sales_data = sales1.unionAll(sales2).unionAll(sales3)

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
store_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "store_master.csv")
item_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Attributes_Encoded_final.csv")

item_master_old = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Master.csv")

item_master_union = item_master.unionAll(item_master_old)
item_master_union = item_master_union.dropDuplicates(subset = ['item_no'])

store_master.createOrReplaceTempView("store_master")
item_master_union.createOrReplaceTempView("item_master") 
# load weekly closing data
weekly_closing_dir_path = '/Users/parulgaba/Desktop/Capstone-Ethos/Encoded/weekly-closing-stock/'
weekly_closing_file_path = weekly_closing_dir_path + '*.csv'

weekly_closing_output_path = '/Users/parulgaba/Desktop/Capstone-Ethos/Closing-Stock-Data.csv'


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
data_path
closing_stock_data.createOrReplaceTempView("closing_stock_raw")


'''         
for f in glob.glob(path):
    print (f)
'''
    
## Aggregate duplicate closing tock file by item_no,location_code,closing_date 
closing_sql = """select
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
group by item_no,location_code,closing_date
"""

closing_stock_table = sqlContext.sql(closing_sql)
closing_stock_table.createOrReplaceTempView("closing_stock")


sales_sql = '''select
     `Store No_` location_code,
     to_date(`Date`, 'yyyy/MM/dd') sales_date,
     cast(`Item No_` as int) item_no,
     `Brand` brand,
     `Department` sales_department,
     `Customer No_` customer_no,
     cast(Quantity as int) quantity,
     float(`Price`) price,
     float(`Total Price`) total_price,
     float(`Line Discount Amount`) line_discount,
     float(`CRM Line Disc_ Amount`) crm_line_discount,
     float(`Discount Amount`) discount,
     float(`Tax Amount`) tax,
     float(`Cost Amount`) cost,
     float(`Billing`) billing,
     float(`Contribution`) contribution,
     to_date(`Receipt Date`, 'yyyy/MM/dd') receipt_date,
     float(`Trade Incentive %`) trade_incentive,
     float(`Trade Incentives Value`) trade_incentive_value,
     float(`Total Contribution`) total_contribution,
     `State` state,
     `Region` region
from sales_raw
'''

sales_table = sqlContext.sql(sales_sql)
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
"""

purchase_table = sqlContext.sql(purchase_sql)
purchase_table.createOrReplaceTempView("purchase")

transfer_sql = """select `Store Out` store_out, to_date(`Store Out Date`, 'yyyy/MM/dd') store_out_date, `Store In` store_in, to_date(`Store In Date`, 'yyyy/MM/dd') store_in_date, `Item No_` item_no, `Brand` brand, `Product Group Code` product_group_code, `Quantity` quantity, `Cost Amount` cost_amount, float(MRP) mrp, to_date(`Purchase Date`, 'yyyy/MM/dd') purchase_date, `State` state, `Region` region from transfer_raw"""

transfer_table = sqlContext.sql(transfer_sql)
transfer_table.createOrReplaceTempView("transfer")

sales_join_data_query = '''SELECT
     a.location_code location_code,
     a.item_no item_no,
     a.closing_date closing_date,
     a.brand brand,
     a.department department,
     a.quantity quantity,
     a.cost_amount cost_amount,
     a.purchase_mrp purchase_mrp,
     a.stock_prevailing_mrp stock_prevailing_mrp,
     a.purchase_date purchase_date,
     a.state state,
     a.region region,
    first(b.sales_department) sales_department,
    mean(DATEDIFF(b.sales_date, a.closing_date)) days_to_sell,
    sum(b.customer_no) num_of_customers,
    sum(b.quantity) sales_quantity,
    sum(b.price) sales_price,
    sum(b.total_price) sales_total_price,
    sum(b.line_discount) total_line_discount,
    sum(b.crm_line_discount) total_crm_line_discount,
    sum(b.discount) total_discount,
    sum(b.tax) total_tax,
    sum(b.cost) total_cost,
    sum(b.billing) total_billing,
    sum(b.contribution) contribution,
    sum(b.trade_incentive) total_trade_incentive,
    sum(b.trade_incentive_value) total_trade_incentive_value,
    sum(b.total_contribution) total_contribution
    from closing_stock a LEFT JOIN sales b
ON a.location_code = b.location_code
AND a.item_no = b.item_no
AND b.sales_date > a.closing_date
AND b.sales_date <= date_add(a.closing_date, 7)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
'''
sales_join = sqlContext.sql(sales_join_data_query)
sales_join.createOrReplaceTempView("sales_join")

purchase_join_query = """
SELECT
     a.location_code location_code,
     a.item_no item_no,
     a.closing_date closing_date,
     a.brand brand,
     a.department department,
     a.quantity quantity,
     a.cost_amount cost_amount,
     a.purchase_mrp purchase_mrp,
     a.stock_prevailing_mrp stock_prevailing_mrp,
     a.purchase_date purchase_date,
     a.days_to_sell days_to_sell,
     a.state state,
     a.region region,
     a.sales_department,
     a.num_of_customers,
    a.sales_quantity,
    a.sales_price,
    a.sales_total_price,
    a.total_line_discount,
    a.total_crm_line_discount,
    a.total_discount,
    a.total_tax,
    a.total_cost,
    a.total_billing,
    a.contribution,
    a.total_trade_incentive,
    a.total_trade_incentive_value,
    a.total_contribution,
    sum(b.quantity) total_purchase_quantity,
    sum(b.purchase_mrp) total_purchase_mrp,
    sum(b.cost_amount) total_purchase_cost
    from sales_join a LEFT JOIN purchase b
ON a.location_code = b.location_code
AND a.item_no = b.item_no
AND b.posting_date > a.closing_date
AND b.posting_date <= date_add(a.closing_date, 7)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
"""
purchase_join = sqlContext.sql(purchase_join_query)
purchase_join.createOrReplaceTempView("purchase_join")


## Join item master and store master

purchase_store_master = purchase_join.join(store_master, store_master.store_code == purchase_join.location_code)
purchase_store_master.createOrReplaceTempView("purchase_store_master")
# purchase_item_master = purchase_store_master.join(item_master, purchase_store_master.item_no == item_master.item_no)

purchase_item_query = """
SELECT
     a.location_code location_code,
     a.item_no item_no,
     a.closing_date closing_date,
     b.brand brand,
     b.department department,
     a.quantity quantity,
     a.cost_amount cost_amount,
     a.purchase_mrp purchase_mrp,
     a.stock_prevailing_mrp stock_prevailing_mrp,
     a.purchase_date purchase_date,
     a.state state,
     a.region region,
     a.days_to_sell days_to_sell,
     a.sales_department sales_department,
     a.num_of_customers num_of_customers,
    a.sales_quantity sales_quantity,
    a.sales_price sales_price,
    a.sales_total_price sales_total_price,
    a.total_line_discount total_line_discount,
    a.total_crm_line_discount total_crm_line_discount,
    a.total_discount total_discount,
    a.total_tax total_tax,
    a.total_cost total_cost,
    a.total_billing total_billing,
    a.contribution contribution,
    a.total_trade_incentive total_trade_incentive,
    a.total_trade_incentive_value total_trade_incentive_value,
    a.total_contribution total_contribution,
    a.total_purchase_quantity total_purchase_quantity,
    a.total_purchase_mrp total_purchase_mrp,
    a.total_purchase_cost total_purchase_cost,
    a.store_code,
    a.store_type,
    a.store_location,
    a.city_type,
    b.case_size,
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
from purchase_store_master a LEFT JOIN item_master b
ON a.item_no = b.item_no
"""
purchase_item_master = sqlContext.sql(purchase_item_query)

purchase_item_master.createOrReplaceTempView("purchase_item_master")

### This is the final summary query 

transfer_join_query = """
SELECT
         a.location_code location_code,
     a.item_no item_no,
     a.closing_date closing_date,
     a.brand brand,
     a.department department,
     a.quantity quantity,
     a.cost_amount cost_amount,
     a.purchase_mrp purchase_mrp,
     a.stock_prevailing_mrp stock_prevailing_mrp,
     a.purchase_date purchase_date,
     a.state state,
     a.region region,
     a.days_to_sell days_to_sell,
     a.sales_department sales_department,
     a.num_of_customers num_of_customers,
    a.sales_quantity sales_quantity,
    a.sales_price sales_price,
    a.sales_total_price sales_total_price,
    a.total_line_discount total_line_discount,
    a.total_crm_line_discount total_crm_line_discount,
    a.total_discount total_discount,
    a.total_tax total_tax,
    a.total_cost total_cost,
    a.total_billing total_billing,
    a.contribution contribution,
    a.total_trade_incentive total_trade_incentive,
    a.total_trade_incentive_value total_trade_incentive_value,
    a.total_contribution total_contribution,
    a.total_purchase_quantity total_purchase_quantity,
    a.total_purchase_mrp total_purchase_mrp,
    a.total_purchase_cost total_purchase_cost,
    a.store_code,
    a.store_type,
    a.store_location,
    a.city_type,
    a.case_size,
    a.case_size_range,
    a.gender,
    a.movement,
    a.material,
    a.dial_color,
    a.strap_type,
    a.strap_color,
    a.precious_stone,
    a.glass,
    a.case_shape,
    a.watch_type,
    first(b.product_group_code) product_group_code,
    sum(b.quantity) transfer_quantity,
    sum(b.cost_amount) transfer_cost_amount,
    sum(b.mrp) transfer_mrp
from purchase_item_master a LEFT JOIN transfer b
ON a.location_code = b.store_in
AND a.item_no = b.item_no
AND b.store_in_date >= a.closing_date
AND b.store_in_date < date_add(a.closing_date, 7)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47
"""

#AND weekofyear(a.closing_date) = weekofyear(b.store_in_date)
#AND year(a.closing_date) = year(b.store_in_date)
#AND DATEDIFF(a.closing_date,b.store_in_date) < 7



purchase_summary = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path + 'purchase_item_master.csv')
# purchase_summary.createOrReplaceTempView("purchase_item_master")

purchase_item_master.createOrReplaceTempView("purchase_item_master")
transfer_join = sqlContext.sql(transfer_join_query)
transfer_join.createOrReplaceTempView("transfer_join")
 

## All these queries will be handy for tally

sales_group_by = '''select
 b.item_no item_no,
 b.location_code location_code,
 first(brand) brand,
 first(b.sales_department) sales_department,
 count(b.customer_no) num_of_customers,
 sum(b.quantity) sales_quantity,
 sum(b.price) sales_price,
 sum(b.total_price) sales_total_price,
 sum(b.line_discount) total_line_discount,
 sum(b.crm_line_discount) total_crm_line_discount,
 sum(b.discount) total_discount,
 sum(b.tax) total_tax,
 sum(b.cost) total_cost,
 sum(b.billing) total_billing,
 sum(b.contribution) contribution,
 sum(b.trade_incentive) total_trade_incentive,
 sum(b.trade_incentive_value) total_trade_incentive_value,
 sum(b.total_contribution) total_contribution,
 weekofyear(b.sales_date) sales_week, 
 year(b.sales_date) sales_year
from sales b
group by item_no, location_code, sales_week, sales_year
'''

sales_weekly = sqlContext.sql(sales_group_by).createOrReplaceTempView("sales_weekly")

## Tally item master data

unique_items_in_item_master = item_master.select('item_no').distinct() 
# Unique = 31224 # Total count = 31990

unique_items_in_transactional_data = purchase_store_master.select('item_no').distinct() 
# 20422 # Total count = 5770009 (doesn't matter here ofcourse)

# 5723450

unique_items_in_transactional_data.subtract(unique_items_in_transactional_data.intersect(unique_items_in_item_master)).count()
## 1288 items not found in item_master - Id's captured in csv file


import pyspark.sql.functions as F
sales_join = closing_stock_table.join(sales_table, on=['location_code', 'item_no'],how='left').filter(closing_stock_table.closing_date <= sales_table.sales_date).filter(sales_table.sales_date > F.date_add(closing_stock_table.closing_date, 7))


data_summary = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load("/Users/parulgaba/Desktop/Capstone-Ethos/summarized_data_new.csv")

closing_stock_table.join(item_master, how='left').join(store_master, how='left')

purchase_table.repartition(1).write.csv(data_path + 'purchase.csv')

purchase_table.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'purchase.csv',header = 'true')

closing_stock_table.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'closing_stock.csv',header = 'true')
sales_table.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'sales.csv',header = 'true')
transfer_table.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'transfer.csv',header = 'true')


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

small_summary = purchase_summary.filter("closing_date <= date'2020-02-02' and closing_date >= date'2019-07-23'")

'''
 first(b.num_of_customers) num_of_customers,
 first(b.sales_quantity) sales_quantity,
 first(b.sales_price) sales_price,
 first(b.sales_total_price) sales_total_price,
 first(b.total_line_discount) total_line_discount,
 first(b.total_crm_line_discount) total_crm_line_discount,
 first(b.total_discount) total_discount,
 first(b.total_tax) total_tax,
 first(b.total_cost) total_cost,
 first(b.total_billing) total_billing,
 first(b.contribution) contribution,
 first(b.total_trade_incentive) total_trade_incentive,
 first(b.total_trade_incentive_value) total_trade_incentive_value,
 first(b.total_contribution) total_contribution


>>> sales_join = sqlContext.sql(sales_new_join_query)
>>> sales_join.createOrReplaceTempView("sales_join")
>>> temp = sqlContext.sql("""select * from transfer_join where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))""")


closing_sql = select
location_code,
item_no,
first(brand) brand,
first(department) department,
sum(quantity) quantity,
sum(cost_amount) cost_amount,
sum(purchase_mrp) purchase_mrp,
sum(stock_prevailing_mrp) stock_prevailing_mrp,
first(to_date(purchase_date, 'yyyy/MM/dd')) purchase_date,
first(state) state,
first(region) region,
to_date(closing_date, 'yyyy/MM/dd') closing_date
from closing_stock_raw
group by item_no,location_code,closing_date


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
'''


purchase_store_master.join(item_master, purchase_store_master.item_no == item_master.item_no, how='left').select(purchase_store_master['item_no'], 'case_size_range', 'gender', 'movement', 'material', 'dial_color', 'strap_type', 'strap_color', 'precious_stone', 'glass', 'case_shape', 'watch_type')


















