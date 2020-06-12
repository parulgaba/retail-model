#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 14:15:35 2020

@author: parulgaba
"""
from pyspark.sql.functions import desc, asc,expr
import pyspark.sql.functions as sf
from pyspark.sql import Window

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

item_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(dirpath + 'item_master_imputed.csv')

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

closing_stock_dates = closing_stock_table.select('closing_date').distinct().orderBy(asc('closing_date'))


  # row_number val 
windowSpec = Window.orderBy("closing_date")
closing_stock_dates = closing_stock_dates.withColumn("period_number",sf.row_number().over(windowSpec))

input_period = 6
period = 7 * input_period

closing_stock_dates_filtered = closing_stock_dates.withColumn("mod", expr('period_number % {0}'.format(input_period))).filter("mod == 1")
# .select('closing_date')
# closing_stock_dates_array = [int(row.closing_date) for row in closing_stock_dates.collect()]

closing_stock_table_filtered = closing_stock_dates_filtered.join(closing_stock_table, on='closing_date', how='left')

closing_stock_table_filtered.createOrReplaceTempView("closing_stock")

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
where `Date` is not null
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
where `Date` is not null
group by 1,2,3,4
"""

sales_table = sqlContext.sql(sales_sql)
sales_3_data = sqlContext.sql(sales3_sql)
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
    to_date(`Purchase Date`, 'yyyy/MM/dd') purchase_date
from transfer_raw 
    where `Store In Date` is not null"""

transfer_table = sqlContext.sql(transfer_sql)
transfer_table.createOrReplaceTempView("transfer")


purchase_join_query = """SELECT
    CASE WHEN a.location_code is not null THEN a.location_code ELSE b.location_code END location_code,
    CASE WHEN a.item_no is not null THEN a.item_no ELSE b.item_no END item_no,
    a.closing_date closing_date,
    a.period_number period_number,
    first(a.purchase_date) purchase_date,
    first(b.posting_date) posting_date,
    CASE when avg(a.stock_prevailing_mrp) is not null THEN avg(a.stock_prevailing_mrp) ELSE avg(b.purchase_mrp) END stock_prevailing_mrp,
    CASE WHEN first(a.brand) is not null THEN first(a.brand) ELSE first(b.brand) END brand,
    avg(a.quantity) quantity,
    sum(b.quantity) purchase_quantity,
    avg(a.cost_amount) cost_amount,
    avg(a.purchase_mrp) purchase_mrp
from closing_stock a FULL OUTER JOIN purchase b
    ON a.location_code = b.location_code 
    AND a.item_no = b.item_no
    AND b.posting_date > a.closing_date
    AND b.posting_date <= date_add(a.closing_date, {0})
    group by 1,2,3,4
""".format(period)


purchase_join = sqlContext.sql(purchase_join_query)
purchase_join.createOrReplaceTempView("purchase_join") 

transfer_join_query = """SELECT 
    CASE WHEN a.location_code is not null THEN a.location_code ELSE b.store_in END location_code,
    CASE WHEN a.item_no is not null THEN a.item_no ELSE b.item_no END item_no,
    a.closing_date closing_date,
    a.period_number period_number,
    first(a.purchase_date) purchase_date,
    first(a.posting_date) posting_date,
    first(b.store_in_date) store_in_date,
    CASE when avg(a.stock_prevailing_mrp) is not null THEN avg(a.stock_prevailing_mrp) ELSE avg(b.mrp) END stock_prevailing_mrp,
    CASE WHEN first(a.brand) is not null THEN first(a.brand) ELSE first(b.brand) END brand,
    avg(a.quantity) quantity,
    avg(a.purchase_quantity) purchase_quantity,
    sum(b.quantity) transfer_quantity,
    avg(a.cost_amount) cost_amount,
    avg(a.purchase_mrp) purchase_mrp,
    first(b.product_group_code) product_group_code,
    avg(b.cost_amount) transfer_cost_amount
from purchase_join a FULL OUTER JOIN transfer b
    ON a.location_code = b.store_in 
    AND a.item_no = b.item_no
    AND b.store_in_date > a.closing_date
    AND b.store_in_date <= date_add(a.closing_date, {0})
    group by 1,2,3,4
""".format(period)

transfer_in_join = sqlContext.sql(transfer_join_query)
transfer_in_join.createOrReplaceTempView("transfer_in_join")

sales_join_query = """SELECT
    CASE WHEN a.location_code is not null THEN a.location_code ELSE b.location_code END location_code,
    CASE WHEN a.item_no is not null THEN a.item_no ELSE b.item_no END item_no,
    a.closing_date closing_date,
    a.period_number period_number,
    first(a.purchase_date) purchase_date,
    first(a.posting_date) posting_date,
    first(a.store_in_date) store_in_date,
    CASE when avg(a.stock_prevailing_mrp) is not null THEN avg(a.stock_prevailing_mrp) ELSE avg(b.price) END stock_prevailing_mrp,
    CASE WHEN first(a.brand) is not null THEN first(a.brand) ELSE first(b.brand) END brand,
    avg(a.quantity) quantity,
    avg(a.purchase_quantity) purchase_quantity,
    avg(a.transfer_quantity) transfer_quantity,
    avg(a.cost_amount) cost_amount,
    avg(a.purchase_mrp) purchase_mrp,
    first(a.product_group_code) product_group_code,
    avg(a.transfer_cost_amount) transfer_cost_amount,
    sum(b.quantity) sales_quantity,
    DATEDIFF(first(a.closing_date), first(a.purchase_date)) days_to_sell,
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
    avg(b.total_contribution) total_contribution
from transfer_in_join a FULL OUTER JOIN sales b
 ON a.location_code = b.location_code 
 AND a.item_no = b.item_no
 AND b.sales_date > a.closing_date
 AND b.sales_date <= date_add(a.closing_date, {0})
 where b.quantity <= 2
group by 1,2,3,4
""".format(period)

sales_join = sqlContext.sql(sales_join_query)
sales_join.createOrReplaceTempView("sales_join")

store_master = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path + "store_master_with_state_and_regions.csv")

store_master = store_master.drop('_c7', '_c8', '_c9', 'Status')
store_join = sales_join.join(store_master, store_master.store_code == sales_join.location_code, how='left').drop('store_code')
store_join = store_join.na.fill(0)

store_join.createOrReplaceTempView("store_join")

item_join_query = """select a.*,
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

ethos_transaction_summary = sqlContext.sql(item_join_query)
ethos_transaction_summary = ethos_transaction_summary.join(area_codes, ethos_transaction_summary.state == area_codes.state_code, how='left')

ethos_transaction_summary = ethos_transaction_summary.drop('state_code')

ethos_transaction_summary.printSchema()


ethos_transaction_summary.groupBy().sum('quantity', 'sales_quantity', 'purchase_quantity', 'transfer_quantity').show()
print (ethos_transaction_summary.filter('sales_quantity == 0').count())
print (ethos_transaction_summary.filter('sales_quantity > 0').count())

# ethos_transaction_summary.groupBy('item_no', 'location_code', 'closing_date', 'sales_quantity').agg(sf.count("closing_date")).filter('sales_quantity == 0 and closing_date is not null').orderBy('closing_date')

# ethos_transaction_summary.groupBy('item_no', 'location_code', 'closing_date', 'sales_quantity').agg(sf.count("closing_date")).filter('sales_quantity > 0 and closing_date is not null').orderBy('closing_date')

# filepath = '/Users/parulgaba/Desktop/Capstone-Ethos/ethos-retail-model/data-engineering/periodic_summary.py'









