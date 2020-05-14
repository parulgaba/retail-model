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
sales_data.cache()

purchase_data.createOrReplaceTempView("purchase_raw")
purchase_data.cache()

transfer_data.createOrReplaceTempView("transfer_raw")
transfer_data.cache()
   
# Load master data
store_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "store_master.csv")
item_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Attributes_Encoded_final.csv")
store_master.createOrReplaceTempView("store_master")
item_master.createOrReplaceTempView("item_master") 
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
combined_csv.to_csv(weekly_closing_output_path, index=False, encoding='utf-8')

closing_stock_data = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(weekly_closing_output_path)
closing_stock_data.createOrReplaceTempView("closing_stock_raw")
closing_stock_data.cache()      
'''
1. Closign Data weekly
2. store_master.csv - merge with 1
3. 

Store master
['Store Code', 'Store type', 'Store location', 'City type', 'State', 'Region']

Item master
['s_no',
 'item_no',
 'brand',
 'department',
 'case_size',
 'case_size_range',
 'gender',
 'movement',
 'material',
 'dial_color',
 'strap_type',
 'strap_color',
 'precious_stone',
 'glass',
 'case_shape',
 'watch_type']

Puschase Data
['Location Code',
 'Posting Date',
 'Item No_',
 'Brand',
 'Department',
 'Quantity',
 'Purchase MRP',
 'Cost Amount',
 'State Code',
 'Region']

Transfer Data
['Store Out',
 'Store Out Date',
 'Store In',
 'Store In Date',
 'Item No_',
 'Brand',
 'Product Group Code',
 'Quantity',
 'Cost Amount',
 'MRP',
 'Purchase Date',
 'State',
 'Region']

sales_data.columns
['Store No_',
 'Date',
 'Item No_',
 'Brand',
 'Department',
 'Customer No_',
 'Quantity',
 'Price',
 'Total Price',
 'Line Discount Amount',
 'CRM Line Disc_ Amount',
 'Discount Amount',
 'Tax Amount',
 'Cost Amount',
 'Billing',
 'Contribution',
 'Receipt Date',
 'Trade Incentive %',
 'Trade Incentives Value',
 'Total Contribution',
 'State',
 'Region']

closing_stock_data.columns

['Location Code',
 'Item No_',
 'Brand',
 'Department',
 'Quantity',
 'Cost Amount',
 'Purchase MRP',
 'Prevailing MRP as on Stock Date',
 'Purchase Date',
 'State',
 'Region',
 'closing_date']

'''         
for f in glob.glob(path):
    print (f)
    
    
closing_sql = '''select
    `Location Code` location_code,
    `Item No_` item_no,
    `Brand` brand,
    `Department` department,
    cast(Quantity as int) quantity,
    float(`Cost Amount`) cost_amount,
    float(`Purchase MRP`) purchase_mrp,
    float(`Prevailing MRP as on Stock Date`) stock_prevailing_mrp,
    to_date(`Purchase Date`, 'yyyy/MM/dd') purchase_date,
    `State` state,
    `Region` region,
    to_date(closing_date, 'yyyy/MM/dd') closing_date
from closing_stock_raw
'''

closing_stock_table = sqlContext.sql(closing_sql)
closing_stock_table.createOrReplaceTempView("closing_stock")
closing_stock_table.cache()

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

## Duplicates in sales table - 216667 reduced to 201184
# Droppong duplicates on ['store_no', 'sales_date', 'item_no']

sales_table = sqlContext.sql(sales_sql)
sales_table.createOrReplaceTempView("sales")
sales_table.cache()

join_data_query = '''
select 
 a.location_code location_code,
 a.item_no item_no,
 a.closing_date,
 first(a.brand) brand,
 first(a.department) department,
 first(a.quantity) quantity,
 first(a.cost_amount) cost_amount,
 first(a.purchase_mrp) purchase_mrp,
 first(a.stock_prevailing_mrp) stock_prevailing_mrp,
 first(a.purchase_date) purchase_date,
 first(a.state) state,
 first(a.region) region,
 first(b.sales_department),
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
 sum(b.contribution) total_contribution,
 sum(b.trade_incentive) total_trade_incentive,
 sum(b.trade_incentive_value) total_trade_incentive_value,
 sum(b.total_contribution) total_contribution
 from closing_stock a LEFT JOIN sales b
 ON a.location_code = b.store_no 
 AND a.item_no = b.item_no
 AND b.sales_date >= a.closing_date
 AND b.sales_date < date_add(a.closing_date, 7)
 GROUP BY a.closing_date,a.location_code,a.item_no
'''

import pyspark.sql.functions as F
sales_join = closing_stock_table.join(sales_table, on=['location_code', 'item_no'],how='left').filter(closing_stock_table.closing_date <= sales_table.sales_date).filter(sales_table.sales_date > F.date_add(closing_stock_table.closing_date, 7))


data_summary = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load("/Users/parulgaba/Desktop/Capstone-Ethos/summarized_data_new.csv")
