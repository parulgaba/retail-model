import pandas as pd
import csv,os
import numpy as np
import time
from datetime import datetime, date, time, timedelta

#Define you directory #Replace your data directory
data_dir = '/Users/nirajkulkarni/Desktop/Niraj/ISB-CBA/Capstone/data/'
final_file = 'summarized_data.csv'
"""
d1=store master
d2=Item Master
d3=Closing Stock as on_dd.mm.yy ---> Closing Stock as on_01.12.19
"""


#Read Item and Master
d1 = pd.read_excel(data_dir + 'Encoded/store master.xlsx')
d2 = pd.read_excel(data_dir + 'item-data/Item Master.xlsx')
#d3 = pd.read_excel(data_dir + 'Encoded/Closing Stock as on_01.12.19.xlsx') #Closing Stock as on_19.01.2020.xlsx')

def read_sales():
    sales_col = ['Store No','Date','Item No', 'Brand','Department','Customer No', 'Quantity','Price',
                 'Total Price','Line Discount Amount', 'CRM Line Disc_ Amount', 'Discount Amount',
                 'Tax Amount' ,'Cost Amount', 'Billing', 'Contribution', 'Receipt Date','Trade Incentive' ,
                 'Trade Incentives Value', 'Total Contribution', 'State','Region']

    #read all sales file and rename the columns for easiness
    sale_17_18 = pd.read_excel(data_dir + 'item-data/2017-18/Sale Data 01.04.17 to 31.03.18.xlsx')
    sale_17_18.columns = sales_col

    sale_18_19 = pd.read_excel(data_dir + 'item-data/2018-19/Sale Data 01.04.18 to 31.03.19.xlsx')
    sale_18_19.columns = sales_col

    sale_19_20 = pd.read_excel(data_dir + 'item-data/2019-20/Sale Data 01.04.19 to 06.02.2020.xlsx')
    sale_19_20.columns = sales_col

    #COncat the all sales files
    #concat all sales file
    all_sales_list = [sale_17_18,sale_18_19,sale_19_20]
    all_sales = pd.concat(all_sales_list)
    return all_sales


def read_purchase():
    # read all purchase files
    purchase_17_18 = pd.read_excel(data_dir + 'item-data/2017-18/Purchase Data 01.04.17 to 31.03.18.xlsx')
    purchase_18_19 = pd.read_excel(data_dir + 'item-data/2018-19/Purchase Data 01.04.18 to 31.03.19.xlsx')
    purchase_19_20 = pd.read_excel(data_dir + 'item-data/2019-20/Purchase Data 01.04.19 to 06.02.2020.xlsx')

    # concat all sales file
    all_purchase_list = [purchase_17_18, purchase_18_19, purchase_19_20]
    all_purchase = pd.concat(all_purchase_list)
    return all_purchase

all_sales = read_sales()
all_purchase = read_purchase()

#Define stock datadir to loop all stock files. Please make sure all files are kept in here.
stock_data_dir = data_dir + "Encoded"
file_cnt=0

#Create file structure
final_summ_file = pd.DataFrame(columns=['Col_PK','Item_No', 'Location_Code', 'Purchase_MRP',
                                        'Prevailing_MRP_as_on_Stock_Date', 'State', 'Region','Stock_Quantity', 'Days_In_Stock',
                                        'Week_Start','Month','Year','Store_Code', 'Store_type', 'Store_location',
                                        'City_type',  'No_','Brand', 'Movement', 'Case_Material',
                                        'Watchband_Strap_Type', 'Dial_Color', 'Case_Shape', 'Case_Size',
                                        'Sales_Quantiy_In_Week','Days_To_Sell','Avg_Billing_In_Week','Purchase_Quantity','Total_Available_Qty'])

final_summ_file.to_csv(data_dir + final_file, index=False)


print("Files to be processed 53")
for filename in os.listdir(stock_data_dir):
    if filename.startswith("Closing"):

        # create a final datafram table
        final_summ_file = pd.DataFrame()

        file_cnt = file_cnt + 1

        d3 = pd.read_excel(data_dir + 'Encoded/' + filename)
        print("####  Running for  " + filename + "  ####")
        print("File Count : " + str(file_cnt))

        # New Col- Day_in_Stock
        file_week = os.path.basename(filename)
        file_week = file_week[20:26] + '20' + file_week[26:28]
        file_week = datetime.strptime(file_week, '%d.%m.%Y')
        d3['days_in_stock_init'] = file_week - d3['Purchase Date']
        # instead of agg, have to to use this logic to avoid error of aggreagte timedelta type
        days_in_stock_aggdf = d3.groupby(["Item No_", "Location Code"])['days_in_stock_init'].agg(['sum', 'count'])
        days_in_stock_aggdf['days_in_stock_init'] = days_in_stock_aggdf['sum'] / days_in_stock_aggdf['count']
        merged_days_in_stock_df = d3.merge(days_in_stock_aggdf, right_index=True, how='inner',
                                           left_on=["Item No_", "Location Code"],
                                           right_on=["Item No_", "Location Code"],
                                           suffixes=('_x', '_y'))  # , indicator= True)

        d3 = merged_days_in_stock_df #adding a new colum of day in stick after aggregating
        merged_days_in_stock_df.rename(columns={'days_in_stock_init_y': 'days_in_stock_agg'}, inplace=True)

        # distinct store code/location code and item no and do quantity of stock group by, or keep without duplicated?
        d3_unique = d3.drop_duplicates(["Location Code", "Item No_"])

        # Quantity Col 19 - Stock quantity grouped and summed
        stock_quantitydf = d3.groupby(["Location Code", "Item No_"])['Quantity'].agg(['sum'])  # count()
        merge_quantity = d3_unique.merge(stock_quantitydf, right_index=True, left_on=['Location Code', 'Item No_'],
                                         right_on=['Location Code', 'Item No_'],
                                         suffixes=('_m', '_n'))
        merge_quantity.rename(columns={'sum_n': 'stock_quantity'}, inplace=True)

        # Col - 1 - 7
        final_summ_file['Col_PK'] = merge_quantity['Item No_'].map(str) + '_' + merge_quantity['Location Code']
        final_summ_file[['Item No_', 'Location Code', "Purchase MRP", "Prevailing MRP as on Stock Date",
                        'State','Region','stock_quantity','days_in_stock_agg']] =\
            merge_quantity.loc[:,['Item No_', 'Location Code', "Purchase MRP", "Prevailing MRP as on Stock Date",
                                  'State','Region','stock_quantity','days_in_stock_agg']]

        # Col 2 - Week
        final_summ_file['Week'] = file_week

        # Col -3 Month
        final_summ_file['Month'] = file_week.month

        # Col- 4 Year
        final_summ_file['Year'] = file_week.year


        #merging for files with joining store data #I think its better to take state and region from weekly stock table
        merged_store_df = final_summ_file.merge(
            d1[['Store Code', 'Store type', 'Store location', 'City type']],
            how='inner', left_on=["Location Code"], right_on=["Store Code"],
            suffixes=('_m', '_n'))  # , indicator= True)

        final_summ_file  = merged_store_df

        #merging for files with joining item data
        merged_item_df = final_summ_file.merge(d2[['No_', 'Brand', 'Movement', 'Case Material',
                                                   'Watchband _ Strap Type', 'Dial Color', 'Case Shape', 'Case Size']],
                                               how='inner', left_on=["Item No_"], right_on=["No_"],
                                               suffixes=('_m', '_n'))  # , indicator= True)
        final_summ_file = merged_item_df

        ### RUNNING FOR SALES MERGING NOW ######
        # date time conversion of sales file
        #file_week = os.path.basename(filename)
        week_start = file_week #datetime.strptime(file_week, '%d.%m.%Y')
        end_date = week_start + timedelta(days=7)

        # create temporary dataframe for each week which it runs
        #pd.to_datetime(final_summ_file['Week'], format='%d.%m.%Y')
        temp_df_date_range = all_sales[(all_sales['Date'] > file_week) & (all_sales['Date'] <= end_date)]

        # Col - 20 Sales QUantity
        sales_quantitydf = temp_df_date_range.groupby(["Item No", "Store No"])['Quantity'].agg(['sum'])  # count()
        merged_sales_quantity_df = final_summ_file.merge(sales_quantitydf, right_index=True, how='left',
                                                         left_on=["Item No_", "Location Code"],
                                                         right_on=["Item No", "Store No"],
                                                         suffixes=('_m', '_n'))  # , indicator= True)
        merged_sales_quantity_df.rename(columns={'sum': 'sales_quantity'}, inplace=True)
        final_summ_file = merged_sales_quantity_df

        # New Col - Days_to_Sell
        # find the different between receipt_date which is purchase date and the Data field which is sale date
        temp_df_date_range['sale_diff'] = temp_df_date_range.loc[:, 'Date'] - temp_df_date_range.loc[:, 'Receipt Date']
        # instead of agg, have to to use this logic to avoid error of aggreagte timedelta type
        Days_to_Sell_Aggdf = temp_df_date_range.groupby(["Item No", "Store No"])['sale_diff'].agg(['sum', 'count'])
        Days_to_Sell_Aggdf['days_to_sell'] = Days_to_Sell_Aggdf['sum'] / Days_to_Sell_Aggdf['count']
        try:
            #try exception as there is no sales data after 6th Feb 2020 but the stock data is present
            merged_days_sell_df = final_summ_file.merge(Days_to_Sell_Aggdf['days_to_sell'], right_index=True, how='left',
                                                        left_on=["Item No_", "Location Code"],
                                                        right_on=["Item No", "Store No"],
                                                        suffixes=('_x', '_y'))  # , indicator= True)
            final_summ_file = merged_days_sell_df

        except:
            print("No Data for week : " + str(week_start))
            final_summ_file['days_to_sell'] = None


        # Col - 21 Average BIlling Value
        avg_sales_billing = temp_df_date_range.groupby(["Item No", "Store No"])['Billing'].agg(['mean'])  # count()
        merged_avg_billing_df = final_summ_file.merge(avg_sales_billing, right_index=True, how='left',
                                                      left_on=["Item No_", "Location Code"],
                                                      right_on=["Item No", "Store No"],
                                                      suffixes=('_m', '_n'))  # , indicator= True)
        merged_avg_billing_df.rename(columns={'mean': 'avg_billing'}, inplace=True)
        final_summ_file = merged_avg_billing_df

        #create tem purchase dataframe to store running week's data
        purchase_temp_df_date_range = all_purchase[
            (all_purchase['Posting Date'] > file_week) & (all_purchase['Posting Date'] <= end_date)]
        purchase_quantitydf = purchase_temp_df_date_range.groupby(["Item No_", "Location Code"])['Quantity'].agg(['sum'])
        merged_purchase_quantity_df = final_summ_file.merge(purchase_quantitydf, right_index=True, how='left',
                                                            left_on=["Item No_", "Location Code"],
                                                            right_on=["Item No_", "Location Code"],
                                                            suffixes=('_i', '_j'))  # , indicator= True)
        merged_purchase_quantity_df.rename(columns={'sum': 'purchase_quantity'}, inplace=True)
        final_summ_file = merged_purchase_quantity_df

        #print(final_summ_file.columns)
        # Replace NaN with 0
        final_summ_file[['sales_quantity','days_to_sell','avg_billing','purchase_quantity','stock_quantity']] = \
            final_summ_file[['sales_quantity','days_to_sell','avg_billing','purchase_quantity','stock_quantity']].fillna(0)

        # Stock Qty + purchase Qty
        final_summ_file['total_available_qty'] = final_summ_file['stock_quantity'] + final_summ_file[
            'purchase_quantity']

        ##### Append the data in the final file.
        final_summ_file.to_csv(data_dir + final_file, index=False, mode='a', header=False)
        #if file_cnt == 30:
        #    break

print("Total Files Processed : ", str(file_cnt))

