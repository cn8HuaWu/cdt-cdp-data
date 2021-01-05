# -*- coding: utf-8 -*-

# example command
# python excel2csv 

import pandas as pd
import argparse
import logging
import csv
import xlrd

def read_excel_file(file_path, sheetname_in, skip=0):
    if sheetname_in:
        df_made = pd.read_excel(file_path, sheet_name=sheetname_in, encoding='utf8', dtype=str, na_values=' ',skiprows= skip,keep_default_na=False)
    else:
        df_made = pd.read_excel(file_path, encoding='utf8', dtype=str, na_values=' ', keep_default_na=False)
    logger.info('row count: ' + str(len(df_made)))
    return df_made

def format_df( df_in):
        df_in = df_in.dropna(axis=0).applymap(str)
        for data_index in df_in.index:
            row_str = "".join(df_in.loc[data_index].values)
            if(row_str is None or row_str.strip() == ''):
                df_in.drop(data_index, inplace=True)
        logging.info('write row count: ' + str(len(df_in)))
        df_in.replace(r'[\n|\r|\\]', ' ', regex=True, inplace=True)
        df_in.fillna('', inplace=True)
    
        column_repaired = []
        for column in df_in.columns:
            column = column.replace('\n', '').replace('\r', '').replace(' ', '_')
            column_repaired.append(column)
        df_in.columns = column_repaired
        return df_in

def get_sheets(xsl_path):
    b=xlrd.open_workbook(input_excel)
    sheets = b.sheets()
    name_list = []
    for sheet in sheets:
        name_list.append(sheet.name)
    return name_list

import sys, os
if __name__ == '__main__':
    args = sys.argv  
    # config logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('EXCEL2CSV')
    func = args[1] if len(args)>1  else None
    input_excel = args[2] if len(args)>2 else None
    output_path = args[3] if len(args)>3 else None
    
    if ( func == 'L'):
        print(os.path.basename(input_excel))
        sheets = get_sheets(input_excel)
        print(sheets)
    elif (func == 'T'):
        sheets = get_sheets(input_excel)
        for sht in sheets:
            df = read_excel_file(input_excel, sht)
            ff = format_df(df)
            ff.to_csv(output_path + "\\" + sht  + ".csv", index=False, quoting=csv.QUOTE_ALL)
    else:
        home_dir = "/data/sftpcdp/data/input/historical/JD_B2B"
    
        # name_sheet_tuple=[
        #     ("TM_201903.xlsx", "订单ASP(3月 )","tm_order_201903.csv"),
        #     ("TM_201904.xlsx", "订单ASP","tm_order_201904.csv"),
        #     ("TM_201905.xlsx", "订单ASP（5月）","tm_order_201905.csv"),
        #     ("TM_201906.xlsx", "订单ASP（6月）","tm_order_201906.csv"),
        #     ("TM_201907.xlsx", "订单ASP（7月）","tm_order_201907.csv"),
        #     ("TM_201908.xlsx", "订单ASP（8月）","tm_order_201908.csv"),
        #     ("TM_201909.xlsx", "订单ASP（9月）","tm_order_201909.csv"),
        #     ("TM_201910.xlsx", "订单ASP（10月）","tm_order_201910.csv"),
        #     ("TM_201911.xlsx", "订单ASP（11月）","tm_order_201911.csv"),
        #     ("TM_201912.xlsx", "订单ASP（12月）","tm_order_201912.csv"),
        #     ("TM_202001.xlsx", "订单ASP（1月）","tm_order_202001.csv"),
        #     ("TM_detail_201903.xlsx", "宝贝RRP（3月）","tm_order_dtl_201903.csv"),
        #     ("TM_detail_201904.xlsx", "宝贝RRP","tm_order_dtl_201904.csv"),
        #     ("TM_detail_201905.xlsx", "宝贝RRP（5月）","tm_order_dtl_201905.csv"),
        #     ("TM_detail_201906.xlsx", "宝贝RRP（6月）","tm_order_dtl_201906.csv"),
        #     ("TM_detail_201907.xlsx", "RRP订单（7月）","tm_order_dtl_201907.csv"),
        #     ("TM_detail_201908.xlsx", "RRP订单（8月）","tm_order_dtl_201908.csv"),
        #     ("TM_detail_201909.xlsx", "RRP订单（9月）","tm_order_dtl_201909.csv"),
        #     ("TM_detail_201910.xlsx", "RRP订单（10月）","tm_order_dtl_201910.csv"),
        #     ("TM_detail_201911.xlsx", "RRP订单（11月）","tm_order_dtl_201911.csv"),
        #     ("TM_detail_201912.xlsx", "RRP订单（12月）","tm_order_dtl_201912.csv"),
        #     ("TM_detail_202001.xlsx", "RRP订单（1月）","tm_order_dtl_202001.csv"),
        # ]
        
        # for fname, sheetname, output_name in name_sheet_tuple:
        #     intput_path = os.path.join( home_dir, fname)
        #     output_path = os.path.join( home_dir, output_name)
        #     print("processing " + intput_path)
        #     df = read_excel_file(intput_path, sheetname)
        #     ff = format_df(df)
        #     ff.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
        
        for maindir, subdir, file_name_list in os.walk(home_dir):
            for filename in file_name_list:
                
                input_path = os.path.join( home_dir, os.path.basename(filename)[:-4]+".csv")
                output_name = os.path.join( home_dir, filename)
                print(input_path)
                df = read_excel_file(input_path, 'sheet', skip=1)
                # output_path = os.path.join( home_dir, output_name)
                # ff = format_df(df)
                # ff.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)