#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 19:57:59 2020

@author: parulgaba
"""

from pyspark.sql.functions import desc, asc,expr
import pyspark.sql.functions as sf
from pyspark.sql import Window

dirpath = '/Users/parulgaba/Desktop/Capstone-Ethos/ConfidentialData/csvdata/'

data_path = '/Users/parulgaba/Desktop/Capstone-Ethos/ethos-retail-model/data/'

ethos_transaction_summary = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path + 'summary_all_3.csv')
stores_to_be_removed = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(data_path +  "stores_to_be_removed.csv")

ethos_transaction_summary = ethos_transaction_summary.join(stores_to_be_removed, on='location_code', how='left_anti')
input_period = 12

print('\n\n Summary for aggregation of {0} weeks'.format(str(input_period)))

windowSpec = Window.orderBy('year', 'week_no')
period_aggregator = ethos_transaction_summary.select('year', 'week_no', 'week').distinct().withColumn("row_number",sf.row_number().over(windowSpec) - 1)

period_aggregator = period_aggregator.withColumn("period",sf.floor(expr('row_number / {0}'.format(input_period))) + 1).drop('row_number').orderBy('year', 'week_no')

ethos_transaction_summary = ethos_transaction_summary.join(period_aggregator, on=['week','year', 'week_no'], how='left')


ethos_transaction_summary = ethos_transaction_summary.orderBy('year', 'week_no')

ethos_transaction_summary.createOrReplaceTempView('ethos_transaction_summary')

aggregated_summary_query = """select
 period,
 location_code,
 item_no,
 first(brand) brand,
 first(store_type) store_type,
 first(store_location) store_location,
 first(city_type) city_type,
 first(region) region,
 first(state) state,
 first(quantity) quantity,
 sum(purchase_quantity) purchase_quantity,
 sum(transfer_quantity) transfer_quantity,
 (first(quantity) + sum(purchase_quantity) + sum(transfer_quantity)) available_quantity,
 sum(sales_quantity) sales_quantity,
 avg(purchase_cost_amount) purchase_cost_amount,
 avg(purchase_mrp) purchase_mrp,
 first(purchase_date) purchase_date,
 avg(stock_prevailing_mrp) stock_prevailing_mrp,
 first(store_in) store_in,
 first(product_group_code) product_group_code,
 avg(transfer_cost_amount) transfer_cost_amount,
 first(sales_department) sales_department,
 avg(days_to_sell) days_to_sell,
 sum(num_of_customers) num_of_customers,
 avg(total_price) total_price,
 avg(line_discount) line_discount,
 avg(crm_line_discount) crm_line_discount,
 (avg(line_discount) + avg(crm_line_discount)) discount,
 avg(tax) tax,
 avg(cost) cost,
 (avg(stock_prevailing_mrp) - (avg(line_discount) + avg(crm_line_discount))) billing,
 (avg(stock_prevailing_mrp) - (avg(line_discount) + avg(crm_line_discount) + avg(tax) + avg(cost))) contribution,
 avg(trade_incentive) trade_incentive,
 avg(trade_incentive_value) trade_incentive_value,
 ((avg(stock_prevailing_mrp) - (avg(line_discount) + avg(crm_line_discount) + avg(tax) + avg(cost))) + avg(trade_incentive_value)) total_contribution,
 first(case_size) case_size,
 first(case_size_range) case_size_range,
 first(gender) gender,
 first(movement) movement,
  first(material) material,
  first(dial_color) dial_color,
  first(strap_type) strap_type,
  first(strap_color) strap_color,
  first(precious_stone) precious_stone,
  first(glass) glass,
  first(case_shape) case_shape,
  first(watch_type) watch_type,
 first(area_code) area_code
from ethos_transaction_summary
group by period, item_no, location_code
"""


aggregated_summary = sqlContext.sql(aggregated_summary_query)

aggregated_summary.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'aggregated_summary_period_{0}_weeks.csv'.format(input_period), header='true')

# print(aggregated_summary.filter('sales_quantity > 0').count())

# print (aggregated_summary.count())


print('\n\n STORETYPE AGG, Summary for aggregation of {0} weeks'.format(str(input_period)))

aggregated_summary_query_store_type = """select
 period,
 item_no,
 store_location,
 store_type,
 first(brand) brand,
 first(city_type) city_type,
 first(region) region,
 first(state) state,
 first(quantity) quantity,
 sum(purchase_quantity) purchase_quantity,
 sum(transfer_quantity) transfer_quantity,
 (first(quantity) + sum(purchase_quantity) + sum(transfer_quantity)) available_quantity,
 sum(sales_quantity) sales_quantity,
 avg(purchase_cost_amount) purchase_cost_amount,
 avg(purchase_mrp) purchase_mrp,
 first(purchase_date) purchase_date,
 avg(stock_prevailing_mrp) stock_prevailing_mrp,
 first(store_in) store_in,
 first(product_group_code) product_group_code,
 avg(transfer_cost_amount) transfer_cost_amount,
 first(sales_department) sales_department,
 avg(days_to_sell) days_to_sell,
 sum(num_of_customers) num_of_customers,
 avg(total_price) total_price,
 avg(line_discount) line_discount,
 avg(crm_line_discount) crm_line_discount,
 (avg(line_discount) + avg(crm_line_discount)) discount,
 avg(tax) tax,
 avg(cost) cost,
 (avg(stock_prevailing_mrp) - (avg(line_discount) + avg(crm_line_discount))) billing,
 (avg(stock_prevailing_mrp) - (avg(line_discount) + avg(crm_line_discount) + avg(tax) + avg(cost))) contribution,
 avg(trade_incentive) trade_incentive,
 avg(trade_incentive_value) trade_incentive_value,
 ((avg(stock_prevailing_mrp) - (avg(line_discount) + avg(crm_line_discount) + avg(tax) + avg(cost))) + avg(trade_incentive_value)) total_contribution,
 first(case_size) case_size,
 first(case_size_range) case_size_range,
 first(gender) gender,
 first(movement) movement,
  first(material) material,
  first(dial_color) dial_color,
  first(strap_type) strap_type,
  first(strap_color) strap_color,
  first(precious_stone) precious_stone,
  first(glass) glass,
  first(case_shape) case_shape,
  first(watch_type) watch_type,
 first(area_code) area_code
from ethos_transaction_summary
group by item_no, period, store_type, store_location
"""

aggregated_summary_store_type = sqlContext.sql(aggregated_summary_query_store_type)

aggregated_summary_store_type.repartition(1).write.format('com.databricks.spark.csv').save(data_path + 'aggregated_summary_store_type_{0}_weeks.csv'.format(input_period), header='true')
#print(aggregated_summary_store_type.filter('sales_quantity > 0').count())
#print (aggregated_summary_store_type.count())
