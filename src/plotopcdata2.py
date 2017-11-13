#!/usr/bin/env python3

from optparse import OptionParser
import sys
import re
import xlsxwriter
import pandas as pd
import numpy as np
# from pandas import Series, DataFrame

"""
Global variables:
"""
g_summary_file_name = ""
FN_NFSv3_OPS = "nfs3.ops.txt"

"""
Constant values
"""
NFSV3_OPS_STAT_NAME_DICT={
    "date_time" : 0,
    "total_ops_num" : 1,
    "access" : 2,
    "commit" : 3,
    "create" : 4,
    "fsstat" : 5,
    "getattr" : 6,
    "lookup" : 7,
    "read" : 8,
    "readdirplus" : 9,
    "remove" : 10,
    "rename" : 11,
    "setattr" : 12,
    "write" : 13
}

NFSV3_OPS_STAT_NAME_DICT_LEN = len(NFSV3_OPS_STAT_NAME_DICT)

def get_lines_between_marker_lines(start_str, end_str):
    lines_between = []
    with open(g_summary_file_name, "r") as fh:
        is_start = False
        for line in fh.read().split('\n'):
            if line.startswith(start_str):
                is_start = True
                continue
            elif line.startswith(end_str):
                break
            elif not is_start:
                continue
            else:
                lines_between.append(line)
    return lines_between

def is_date(date_str):
    pattern = re.compile("\d\d\d\d-\d+-\d+")
    if pattern.match(date_str):
        return True
    else:
        return False

def draw_line_chart2(df):
    excel_file_name = 'pandas_simple.xlsx'
    sheet_name = 'NFS_IOPS_BREAKDOWN'
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(excel_file_name, engine='xlsxwriter')
    df.to_excel(writer, sheet_name=sheet_name)

    workbook = writer.book
    # Create a new chart object. In this case an embedded chart.
    chart1 = workbook.add_chart({'type': 'line'})
    worksheet = workbook.get_worksheet_by_name(sheet_name)
#     number_fmt = workbook.add_format().set_num_format(0)
#     number_fmt = workbook.add_format({'num_format': '0.0', 'bold': False})
#     worksheet.set_column('C:O', 15, number_fmt)
    
#     # Configure the first series.
#     chart1.add_series({
#         'name':       '=Sheet1!$B$1',
#         'categories': '=Sheet1!$A$2:$A$7',
#         'values':     '=Sheet1!$B$2:$B$7',
#     })
    
    data = df.values.tolist()
    # Configure second series. Note use of alternative syntax to define ranges.
    """ Total Ops Number """
    chart1.add_series({
        'name':       [sheet_name, 0, 2],
        'categories': [sheet_name, 1, 1, len(data), 1],
        'values':     [sheet_name, 1, 2, len(data), 2],
    })
    
    """ Individual operation breakdown """
    chart1.add_series({
        'name':       [sheet_name, 0, 3],
        'categories': [sheet_name, 1, 1, len(data), 1],
        'values':     [sheet_name, 1, 3, len(data), 3],
    })
    
    # Add a chart title and some axis labels.
    chart1.set_title ({'name': 'Results of NFS IOPS BREAKDOWN'})
    chart1.set_x_axis({'name': 'DateTime'})
    chart1.set_y_axis({'name': 'IOPS'})
    
    # Set an Excel chart style. Colors with white outline and shadow.
    chart1.set_style(10)
    
    # Insert the chart into the worksheet (with an offset).
    worksheet.insert_chart(len(data)+5, 0, chart1, {'x_offset': 25, 'y_offset': 10, 'x_scale': 2, 'y_scale': 2})
    
    workbook.close()
    
def draw_nfs_combined_chart(df):
    workbook = xlsxwriter.Workbook('chart_line.xlsx')
    worksheet = workbook.add_worksheet()
    bold = workbook.add_format({'bold': 1})
    
    # Add the worksheet data that the charts will refer to.
    headings = list(df.columns.values)
    data = df.values.tolist()
    
    worksheet.write_row('A1', headings, bold)
    worksheet.write_column('A2', data[0])
    worksheet.write_column('B2', data[1])
    worksheet.write_column('C2', data[2])
    
    # Create a new chart object. In this case an embedded chart.
    chart1 = workbook.add_chart({'type': 'line'})
    
    # Configure the first series.
    chart1.add_series({
        'name':       '=Sheet1!$B$1',
        'categories': '=Sheet1!$A$2:$A$7',
        'values':     '=Sheet1!$B$2:$B$7',
    })
    
    # Configure second series. Note use of alternative syntax to define ranges.
    chart1.add_series({
        'name':       ['Sheet1', 0, 2],
        'categories': ['Sheet1', 1, 0, 6, 0],
        'values':     ['Sheet1', 1, 2, 6, 2],
    })
    
    # Add a chart title and some axis labels.
    chart1.set_title ({'name': 'Results of sample analysis'})
    chart1.set_x_axis({'name': 'Test number'})
    chart1.set_y_axis({'name': 'Sample length (mm)'})
    
    # Set an Excel chart style. Colors with white outline and shadow.
    chart1.set_style(10)
    
    # Insert the chart into the worksheet (with an offset).
    worksheet.insert_chart('D2', chart1, {'x_offset': 25, 'y_offset': 10})
    workbook.close()

def analyze_nfs_ops_breakdown():
    start_str = "NFS Operations during peak NFSv3 ops:"
    end_str = "Highest NFSv3 Read and Write operations broken down by time:"
    lines = get_lines_between_marker_lines(start_str, end_str)
    cur_date_time = ""
    former_date_time = ""
    row_list = []
    rows_list = []
    for line in lines:
        word_list = line.split()
        cur_one_ops_num = ""
        cur_one_ops_name = ""
        if is_date(word_list[0]):
            cur_date_time = word_list[0] + " " + word_list[1]
            if cur_date_time != former_date_time and len(row_list) != 0:
                rows_list.append(row_list)
            row_list = [0] * NFSV3_OPS_STAT_NAME_DICT_LEN
            row_list[0] = cur_date_time
            
            cur_total_iops_num = word_list[2]
            row_list[1] = int(cur_total_iops_num)
            cur_one_ops_num = word_list[3]
            cur_one_ops_name = word_list[4]
            ops_index = NFSV3_OPS_STAT_NAME_DICT[cur_one_ops_name]
            row_list[ops_index] = int(cur_one_ops_num)
            
        else:
            former_date_time = cur_date_time
            cur_one_ops_num = word_list[0]
            cur_one_ops_name = word_list[1]
            ops_index = NFSV3_OPS_STAT_NAME_DICT[cur_one_ops_name]
            row_list[0] = cur_date_time
            row_list[ops_index] = int(cur_one_ops_num)
            
#     print(rows_list)
#     print(rows_list[1][2])
#     s = pd.Series(lines)
#     print(s)
    df = pd.DataFrame(np.array(rows_list).reshape(len(rows_list), NFSV3_OPS_STAT_NAME_DICT_LEN), columns = NFSV3_OPS_STAT_NAME_DICT.keys())
    for key in NFSV3_OPS_STAT_NAME_DICT:
        if key == 'date_time':
            continue
        df[key] = df[key].astype(int)

    print(df)
#     print(df.values.tolist())
#     print(df.iat(0,1))
#     print(df.iat(0,2))
#     print(df.iat(0,3))
#     print(df.iat(0,4))
    return df

def plot_excel_charts():
    print(g_summary_file_name)
    df = analyze_nfs_ops_breakdown()
    draw_line_chart2(df)
    
if __name__ == "__main__":
    #global g_summary_file_name
    helpString = (
        "\n%prog [options]"
        "\n%prog --help"
        "\n"
    )
    
    # All the command line options
    parser = OptionParser(helpString)
    parser.add_option("--inputFile",
                      help="Path to data file. (default: %default)",
                      dest="inputFile", default="")
    parser.add_option("--opcDataDir",
                      help="Path to OPC Data Collection Directory. (default: %default)",
                      dest="opcDataDir", default="../rawdata/cfeis01nas832017-11-03_18_49_02_UTC")
    parser.add_option("--outputFile",
                      help="Output file. Results will be written to this file."
                      " (default: %default)",
                      dest="outputFile", default="default_output.csv")
    options, args = parser.parse_args(sys.argv[1:])
    
    
    g_summary_file_name = options.inputFile
    # Run it
    plot_excel_charts()



