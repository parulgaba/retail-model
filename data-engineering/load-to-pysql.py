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

from pyspark.sql.functions import desc, asc

sc = SparkContext(appName="PythonStreamingQueueStream") 
sqlContext = SQLContext(sc)
'''


# filepath = '/Users/parulgaba/Desktop/Capstone-Ethos/ethos-retail-model/data-engineering/ethos-load-to-pysql.py'

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

area_codes = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path + "area_codes.csv")
sales_data.createOrReplaceTempView("sales_raw")
purchase_data.createOrReplaceTempView("purchase_raw")
transfer_data.createOrReplaceTempView("transfer_raw")
   
# Load master data
item_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + 'item_master_imputed.csv')
#item_master = item_master.drop('s_no')
#item_master_old = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + "Item_Master.csv")
#item_master = item_master.unionAll(item_master_old)
item_master = item_master.dropDuplicates(subset = ['item_no'])

item_master = item_master.drop('strap_color_imp', 'case_size_range_imp', 'glass_imp', 'dial_color_imp', 'strap_type_imp', 'case_shape_imp', 'material_imp', 'case_size_imp', 'movement_imp')
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
avg(cost_amount) cost_amount,
avg(purchase_mrp) purchase_mrp,
avg(stock_prevailing_mrp) stock_prevailing_mrp,
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
     `Customer No_` customer_no,
     first(`Brand`) brand,
     first(`Department`) sales_department,
     sum(cast(Quantity as int)) quantity,
     avg(float(`Price`)) price,
     avg(float(`Total Price`)) total_price,
     avg(float(`Line Discount Amount`)) line_discount,
     avg(float(`CRM Line Disc_ Amount`)) crm_line_discount,
     avg(float(`Discount Amount`)) discount,
     avg(float(`Tax Amount`)) tax,
     avg(float(`Cost Amount`)) cost,
     avg(float(`Billing`)) billing,
     avg(float(`Contribution`)) contribution,
     first(to_date(`Receipt Date`, 'yyyy/MM/dd')) receipt_date,
     avg(float(`Trade Incentive %`)) trade_incentive,
     avg(float(`Trade Incentives Value`)) trade_incentive_value,
     avg(float(`Total Contribution`)) total_contribution,
     first(`State`) state,
     first(`Region`) region
from sales_raw
where `Date` is not null and cast(Quantity as int) >= 0 and cast(Quantity as int) < 2
group by 1,2,3,4
'''

sales3.createOrReplaceTempView("sales3")

sales3_sql = """
select
     `Store No_` location_code,
     to_date(`Date`, 'dd/MM/yyyy') sales_date,
     cast(`Item No_` as int) item_no,
     `Customer No_` customer_no,
     first(`Brand`) brand,
     first(`Department`) sales_department,
     sum(cast(Quantity as int)) quantity,
     avg(float(`Price`)) price,
     avg(float(`Total Price`)) total_price,
     avg(float(`Line Discount Amount`)) line_discount,
     avg(float(`CRM Line Disc_ Amount`)) crm_line_discount,
     avg(float(`Discount Amount`)) discount,
     avg(float(`Tax Amount`)) tax,
     avg(float(`Cost Amount`)) cost,
     avg(float(`Billing`)) billing,
     avg(float(`Contribution`)) contribution,
     first(to_date(`Receipt Date`, 'dd/MM/yy')) receipt_date,
     avg(float(`Trade Incentive %`)) trade_incentive,
     avg(float(`Trade Incentives Value`)) trade_incentive_value,
     avg(float(`Total Contribution`)) total_contribution,
     first(`State`) state,
     first(`Region`) region
from sales3
where `Date` is not null and cast(Quantity as int) >= 0 and cast(Quantity as int) < 2
group by 1,2,3,4
"""

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
 first(`Brand`) brand,
 first(`Department`) department,
 sum(`Quantity`) quantity,
 avg(`Purchase MRP`) purchase_mrp,
 avg(`Cost Amount`) cost_amount,
 first(`State Code`) state_code,
 first(`Region`) region
from purchase_raw
where `Posting Date` is not null
group by 1,2,3
"""

purchase_table = sqlContext.sql(purchase_sql)
purchase_table.createOrReplaceTempView("purchase")

transfer_sql = """select 
    `Store Out` store_out, 
    to_date(`Store Out Date`, 'yyyy/MM/dd') store_out_date, 
    `Store In` store_in, 
    to_date(`Store In Date`, 'yyyy/MM/dd') store_in_date, 
    `Item No_` item_no, 
    `Brand` brand, 
    `Product Group Code` product_group_code, 
    `Quantity` quantity, 
    `Cost Amount` cost_amount, 
    float(MRP) mrp, 
    to_date(`Purchase Date`, 'yyyy/MM/dd') purchase_date, 
    `State` state, 
    `Region` region 
from transfer_raw where `Store In Date` is not null"""

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
 avg(purchase_mrp) purchase_mrp,
 avg(cost_amount) cost_amount,
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
avg(cost_amount) cost_amount,
avg(mrp) mrp,
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
    a.state,
    a.region,
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
 avg(b.price) price,
 avg(b.total_price) total_price,
 avg(b.line_discount) line_discount,
 avg(b.crm_line_discount) crm_line_discount,
 avg(b.discount) discount,
 avg(b.tax) tax,
 avg(b.cost) cost,
 avg(b.billing) billing,
 avg(b.contribution) contribution,
 avg(b.trade_incentive) trade_incentive,
 avg(b.trade_incentive_value) trade_incentive_value,
 avg(b.total_contribution) total_contribution,
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
# week_sales_date_mapping
store_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path + "store_master_with_state_and_regions.csv")
# store_master = store_master.drop('state', 'region')
store_master = store_master.drop('_c7', '_c8', '_c9', 'Status')

# store_regions = sales_join.filter("region is not null and state is not null").select('location_code', 'region', 'state').distinct()
# store_master = store_master.join(store_regions, store_master.store_code == store_regions.location_code, how = 'left_outer').drop('location_code')

sales_join = sales_join.drop('state', 'region')

store_join = sales_join.join(store_master, store_master.store_code == sales_join.location_code, how='left').drop('store_code')
store_join = store_join.na.fill(0)

store_join.createOrReplaceTempView("store_join")

item_join_query = """select a.*,
    substr(a.week, 0, 3) week_no,
    substr(a.week, 5) year,
    (a.quantity + a.purchase_quantity + a.transfer_quantity) available_quantity,
    b.case_size case_size,
    b.case_size_range,
    b.gender,
    lower(b.movement) movement,
    lower(b.material) material,
    lower(b.dial_color) dial_color,
    lower(b.strap_type) strap_type,
    lower(b.strap_color) strap_color,
    lower(b.precious_stone) precious_stone,
    lower(b.glass) glass,
    lower(b.case_shape) case_shape,
    lower(b.watch_type) watch_type
from store_join a LEFT JOIN item_master b
ON a.item_no = b.item_no
"""

ethos_transaction_summary = sqlContext.sql(item_join_query) #.filter('week is not null and brand is not null')

ethos_transaction_summary = ethos_transaction_summary.join(area_codes, ethos_transaction_summary.state == area_codes.state_code, how='left')

ethos_transaction_summary = ethos_transaction_summary.drop('state_code').drop('department').filter('brand is not null and week is not null')

print('Done.')

ethos_transaction_summary.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'summary_all_3',header = 'true')
# transfer_in_join.select('week', 'closing_date').distinct().show(100)
#ethos_transaction_summary.filter('brand is not null')

print('Done Export.')

"""
ethos_transaction_summary.filter('Status != "To be removed"').filter('brand is not null').repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'summary_all', header='true')
ethos_transaction_summary.filter('Status != "To be removed"').filter('brand is not null').filter('area_code == 2')

ethos_transaction_summary_sorted = ethos_transaction_summary.orderBy('week_no', 'year')
ethos_transaction_summary.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'summary_area_code.csv',header = 'true')

#>>> path = '/Users/parulgaba/Desktop/Capstone-Ethos/ethos-retail-model/'
# >>> execfile(path + 'data-engineering/ethos-load-to-pysql.py')
ethos_transaction_summary.groupBy().sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()

ethos_transaction_summary.filter("week like 'W52-%'").groupBy('week').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()
ethos_transaction_summary.filter("week like 'W01-%'").groupBy('week').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()
execfile(path + 'data-engineering/ethos-load-to-pysql.py')
ethos_transaction_summary.select([count(when(col(c).isNull(), c)).alias(c) for c in ethos_transaction_summary.columns]).show()

#store_regions = ethos_transaction_summary.filter("region is null or state is null").select('location_code', 'region', 'state').distinct()
#store_regions.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'stores_without_region.csv',header = 'true')

#ethos_weekly = ethos_transaction_summary.groupBy('week').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity')
#ethos_weekly.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'test_weekly.csv',header = 'true')


ethos_transaction_summary.groupBy('region').sum('quantity', 'purchase_quantity', 'transfer_quantity', 'sales_quantity', 'available_quantity').show()

ethos_transaction_summary.select([sf.count(sf.when(sf.col(c).isNull(), c)).alias(c) for c in ethos_transaction_summary.columns]).show()

item_no_agg_summary = ethos_transaction_summary.groupBy('item_no').agg(sf.sum('stock_prevailing_mrp').alias('mrp'), sf.sum('sales_quantity').alias('sales_quantity'))

item_master.select([count(when(col(c).isNull(), c)).alias(c) for c in item_master.columns]).show()

item_master_filter = item_master.join(item_no_agg_summary, on='item_no', how='right_outer')



item_master_filter = item_master_filter.fillna({ 'precious_stone': 'no' })
item_master_filter.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'item_master_for_imputation.csv',header = 'true')

new_df = item_master_filter.withColumn('null_cnt', reduce(lambda x, y: x + y, map(lambda x: sf.when(sf.isnull(sf.col(x)) == 'true', 1).otherwise(0), item_master_filter.schema.names)))

new_df.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'abcd.csv',header = 'true')


# ethos_transaction_summary.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'summary_after_imputation.csv',header = 'true')
### -------- JOIN LOGIC ENDS HERE --- ONLY TESTING and TALLYING BELOW ------- ####
 

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

sales_table.filter("sales_date > date'2017-04-02'and sales_date <= date'2017-04-09'").join(ethos_transaction_summary
.filter("closing_date == date'2017-04-02'").select('item_no', 'location_code', 'closing_date')
, on=['item_no', 'location_code'], how = 'left_anti').select(sales_table["*"])



# Get unique items in summary to avoid tallying with sales/transfer/purchase data not mapped with closing stock weekly data
unique_items = ethos_transaction_summary.select('item_no', 'location_code').distinct()
unique_items_week = ethos_transaction_summary.filter("closing_date == date'2019-09-08'").select('item_no', 'location_code').distinct()


sales_week_mapping.select(sf.col('sales_date'), sf.to_date(sf.col('sales_date'), "dd/mm/YY")).show()

sales_join = closing_stock_table.join(sales_table, on=['location_code', 'item_no'],how='left').filter(closing_stock_table.closing_date <= sales_table.sales_date).filter(sales_table.sales_date > F.date_add(closing_stock_table.closing_date, 7))

temp = sqlContext.sql('''select * from sales_join where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))''')
sqlContext.sql('''select sales_date, weekofyear(sales_date), year(sales_date) from sales where DATEDIFF(sales_date, to_date('2018/07/29','yyyy/MM/dd')) < 7 and DATEDIFF(sales_date, to_date('2019/09/08','yyyy/MM/dd')) < 7''')


# Transfer to file
temp.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'purchase_join_2018-07-29 and 2019-09-08.csv',header = 'true')
purchase_item_master.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'purchase_item_master.csv',header = 'true')

transfer_join.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'transfer_join.csv',header = 'true')


sqlContext.sql('''select distinct closing_week, year from closing_stock where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))''').show()

sqlContext.sql('''select * from sales_weekly where sales_week in (30, 36) and sales_year in (2018, 2018)''').show()

sqlContext.sql('''select distinct * from closing_stock where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd')) and item_no in (5120031, 5121915) and location_code = 'S02'''').show()


sales_join = sqlContext.sql(sales_new_join_query)

sales_join.createOrReplaceTempView("sales_join")

temp = sqlContext.sql('''select * from purchase_join where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'))''')


#Subset

small_summary = transfer_join.filter("closing_date <= date'2020-02-02' and closing_date >= date'2019-07-28'")

>>> sales_join = sqlContext.sql(sales_new_join_query)
>>> sales_join.createOrReplaceTempView("sales_join")
>>> temp = sqlContext.sql'''select * from transfer_join where closing_date in (to_date('2018/07/29','yyyy/MM/dd'), to_date('2019/09/08','yyyy/MM/dd'''



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

closing_sql = '''SELECT
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
group by item_no, location_code,closing_date'''


sales_test_sql = '''select 
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
'''

sales_test = sqlContext.sql(sales_test_sql)


sales_week = '''SELECT
 item_no,
 location_code,
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
 sales_department,
 customer_no,
 quantity,
 price,
 total_price,
 line_discount,
 crm_line_discount,
 discount,
 tax,
 cost,
 billing,
 contribution,
 trade_incentive,
 trade_incentive_value,
 total_contribution,
 state,
 region
 from sales
'''

ethos_transaction_summary.filter('region is null or state is null').select('location_code').distinct().repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'no_state_code.csv', header = 'true')


ethos_transaction_summary.filter("location_code == 'S28' and brand == 'B063'").select('item_no').distinct().count()
ethos_transaction_summary.filter("location_code == 'S28'").select('item_no').distinct().count()
"""











