#!/usr/bin/env python3

from optparse import OptionParser
import sys
import re
# import xlsxwriter
import pandas as pd
import numpy as np

"""
Global variables:
"""
g_summary_file_name = ""
g_excel_file_name = ""
g_excel_writer = None

"""
Constant values
"""
NFSV3_OPS_STAT_NAME_DICT = {
    "date_time": 0,
    "total_ops_num": 1,
    "access": 2,
    "commit": 3,
    "create": 4,
    "fsstat": 5,
    "getattr": 6,
    "lookup": 7,
    "read": 8,
    "readdirplus": 9,
    "remove": 10,
    "rename": 11,
    "setattr": 12,
    "write": 13
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


def draw_line_chart_sheet(df):
    sheet_name = 'NFS_IOPS_BREAKDOWN'
    sheet_name2 = 'NFS_IOPS_BREAKDOWN2'
    draw_nfs_combined_chart(df, sheet_name)
    draw_nfs_combined_chart(df, sheet_name2)


def draw_nfs_combined_chart(df, sheet_name):
    df.to_excel(g_excel_writer, sheet_name=sheet_name)

    headers = df.columns.values.tolist()
    workbook = g_excel_writer.book
    worksheet = workbook.get_worksheet_by_name(sheet_name)
    worksheet.set_column(1, 2, 17)
    worksheet.set_column(2, len(headers), 12)

    """ Create a new chart object """
    chart = workbook.add_chart({'type': 'line'})
    chart.set_title({'name': 'Results of NFS IOPS BREAKDOWN'})
    chart.set_x_axis({'name': 'DateTime', 'name_font': {
                     'size': 14, 'bold': True}})
    chart.set_y_axis({'name': 'IOPS', 'name_font': {'size': 14, 'bold': True}})

    """ Set an Excel chart style """
    chart.set_style(10)
    chart.set_legend({'font': {'size': 14, 'bold': 1}})

    data = df.values.tolist()
    for i in range(2, len(headers) + 1):
        chart.add_series({
            'name':       [sheet_name, 0, i],
            'categories': [sheet_name, 1, 1, len(data), 1],
            'values':     [sheet_name, 1, i, len(data), i],
        })

    """ Insert the chart into the worksheet (with an offset) """
    worksheet.insert_chart(len(
        data) + 5, 0, chart, {'x_offset': 25, 'y_offset': 10, 'x_scale': 2.5, 'y_scale': 2.5})
    workbook.set_size(1920, 1280)


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

    df = pd.DataFrame(np.array(rows_list).reshape(
        len(rows_list), NFSV3_OPS_STAT_NAME_DICT_LEN), columns=NFSV3_OPS_STAT_NAME_DICT.keys())
    for key in NFSV3_OPS_STAT_NAME_DICT:
        if key == 'date_time':
            continue
        df[key] = df[key].astype(int)

    print(df)
    return df


def plot_excel_charts():
    print(g_summary_file_name)
    df = analyze_nfs_ops_breakdown()
    draw_line_chart_sheet(df)


if __name__ == "__main__":
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
    options, args = parser.parse_args(sys.argv[1:])

    g_summary_file_name = options.inputFile
    g_excel_file_name = g_summary_file_name.split('.')[0] + ".xlsx"
    """ Create a Pandas Excel g_excel_writer using XlsxWriter as the engine """
    g_excel_writer = pd.ExcelWriter(g_excel_file_name, engine='xlsxwriter')
    # Run it
    plot_excel_charts()
    g_excel_writer.book.close()
