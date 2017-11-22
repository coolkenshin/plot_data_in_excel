#!/usr/bin/env python3

"""
Author: Kevin Yu
Email: kevin.y.yu@oracle.com
Date: 2017/11/11
"""

from optparse import OptionParser
import sys
import re
import os
# import xlsxwriter
import pandas as pd
import numpy as np

"""
Global variables:
"""
g_summary_file_name = ""
g_one_storage_data_dir_name = ""
g_excel_file_name = ""
g_excel_writer = None
g_io_profile_chart_offset = 5
g_os_load_chart_offset = 5

"""
Constant values
"""
""" Sheet Names """
SN_IO_PROFILE = 'IO Profile'
SN_OS_LOAD = 'OS Load'
SN_REPLICATION_LOAD = "Replication Load"
SHEET_NAME_LIST = [SN_IO_PROFILE, SN_OS_LOAD, SN_REPLICATION_LOAD]

NFSV3_OPS_STAT_BY_SIZE_NAME_DICT = {
    "date_time": 0,
    "iops": 1,
}

ISCSI_OPS_STAT_NAME_DICT = {
    "date_time": 0,
    "iops": 1,
}

NFSV4_HIGHEST_100_OPS_STAT_NAME_DICT = {
    "date_time": 0,
    "iops": 1,
}

DISK_IO_OPS_RW_STAT_NAME_DICT = {
    "date_time": 0,
    "total_ops_per_sec": 1,
    "write": 2,
    "read": 3,
}

DISK_IO_KB_PER_SEC_RW_STAT_NAME_DICT = {
    "date_time": 0,
    "total_kb_per_sec": 1,
    "write": 2,
    "read": 3,
}

NFSV3_OPS_LATENCY_STAT_NAME_DICT = {
    "date_time": 0,
    "latency_count": 2,
    "latency_in_sec": 3
}

"""
Reference: https://grok.cz.oracle.com/source/xref/ak-2013-rel-fish-clone/usr/src/appliance/nas/modules/stat/nfsstat/common/nas_nfsstat.c
"""
NFSV3_OPS_STAT_NAME_DICT = {
    "date_time": 0,
    "total_ops_num": 1,
    "access": 2,
    "commit": 3,
    "create": 4,
    "fsinfo": 5,
    "fsstat": 6,
    "getattr": 7,
    "link": 8,
    "lookup": 9,
    "mkdir": 10,
    "mknod": 11,
    "pathconf": 12,
    "read": 13,
    "readdir": 14,
    "readdirplus": 15,
    "readlink": 16,
    "remove": 17,
    "rename": 18,
    "rmdir": 19,
    "setattr": 20,
    "symlink": 21,
    "write": 22
}

NFSV4_OPS_STAT_NAME_DICT = {
    "date_time": 0,
    "total_ops_num": 1,
    "access": 2,
    "close": 3,
    "commit": 4,
    "create": 5,
    "delegpurge": 6,
    "delegreturn": 7,
    "getattr": 8,
    "getfh": 9,
    "link": 10,
    "lock": 11,
    "lockt": 12,
    "locku": 13,
    "lookup": 14,
    "lookupp": 15,
    "nverify": 16,
    "open": 17,
    "openattr": 18,
    "open-confirm": 19,
    "open-downgrade": 20,
    "putfh": 21,
    "putpubfh": 22,
    "putrootfh": 23,
    "read": 24,
    "readdir": 25,
    "readlink": 26,
    "remove": 27,
    "rename": 28,
    "renew": 29,
    "restorefh": 30,
    "savefh": 31,
    "secinfo": 32,
    "setattr": 33,
    "setclientid": 34,
    "setclientid-confirm": 35,
    "verify": 36,
    "write": 37,
    "release-lockowner": 38
}


def is_date(date_str):
    pattern = re.compile("\d\d\d\d-\d+-\d+")
    if pattern.match(date_str):
        return True
    else:
        return False


def get_offset(chart_sheet_name):
    if chart_sheet_name == SN_IO_PROFILE:
        offset = g_io_profile_chart_offset
    elif chart_sheet_name == SN_OS_LOAD:
        offset = g_os_load_chart_offset
    return offset


def increase_offset(chart_sheet_name, offset_step=40):
    global g_io_profile_chart_offset
    global g_os_load_chart_offset
    if chart_sheet_name == SN_IO_PROFILE:
        g_io_profile_chart_offset += offset_step
    elif chart_sheet_name == SN_OS_LOAD:
        g_os_load_chart_offset += offset_step


def sort_df_by_date_time(df):
    """
    Convert it to datetime type and sort and then restore to string type, otherwise excel chart
    is totally messing up
    """
    col0 = 'date_time'
    df[col0] = pd.to_datetime(df[col0], format='%Y-%m-%d %H:%M:%S')
    df = df.sort_values(by=df.columns[0])
    df[col0] = df[col0].dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df.reset_index(drop=True)
    return df


def init_chart_worksheets():
    workbook = g_excel_writer.book
    for sn in SHEET_NAME_LIST:
        workbook.add_worksheet(sn)


def read_irregular_file(fn):
    """ A tricky workaround here to read irregular file """
    """
                        1            2      3      4      5
        0       2017-11-2     18:47:56  10139  7255    write
        1            2884        read    NaN    NaN    NaN
    """
    df = pd.read_csv(fn, delim_whitespace=True,
                     skiprows=1, low_memory=False)
    df.loc[-1] = df.columns.tolist()
    df.index = df.index + 1
    df = df.sort_index()
    df.columns = [0, 1, 2, 3, 4]
    df.replace(['-', '-.1', '-.2'], [0, 0, 0], inplace=True)
    return df


def parse_opts(sys_args):
    helpString = (
        "\n%prog [options]"
        "\n%prog --help"
        "\n"
    )

    # All the command line options
    parser = OptionParser(helpString)
    parser.add_option("--opcFile",
                      help="Path to data file. (default: %default)",
                      dest="opcSummaryFile", default="")
    parser.add_option("--opcDir",
                      help="Path to OPC Data Collection Directory for only One Storage. (default: %default)",
                      dest="opcDataDir", default="../rawdata2/cfeis01nas41_analytics")
    return parser.parse_args(sys_args)


def get_file_list_in_dir(dir_name, file_name):
    file_name_list = []
    for root, subFolders, files in os.walk(dir_name):
        if file_name in files:
            file_name_list.append(os.path.join(root, file_name))
    return file_name_list


def draw_nfs_combined_chart(df, chart_sheet_name, data_sheet_name, chart_title, chart_y_axis='IOPS', x_scale=2.5, y_scale=2.5):
    df.to_excel(g_excel_writer, data_sheet_name)
    workbook = g_excel_writer.book
    data_sheet = workbook.get_worksheet_by_name(data_sheet_name)
    data_sheet.set_column(1, 2, 17)
    headers = df.columns.values.tolist()
    data_sheet.set_column(2, len(headers), 12)

    chart_sheet = workbook.get_worksheet_by_name(chart_sheet_name)

    """ Create a new line_chart object """
    line_chart = workbook.add_chart({'type': 'line'})
    line_chart.set_title({'name': chart_title})
    line_chart.set_x_axis({'name': 'DateTime', 'name_font': {
        'size': 14, 'bold': True}})
    line_chart.set_y_axis(
        {'name': 'IOPS', 'name_font': {'size': 14, 'bold': True}})

    """ Set an Excel line_chart style """
    line_chart.set_style(10)
    line_chart.set_legend({'font': {'size': 14, 'bold': 1}})
    data = df.values.tolist()
    line_chart.add_series({
        'name':       [data_sheet_name, 0, 2],
        'categories': [data_sheet_name, 1, 1, len(data), 1],
        'values':     [data_sheet_name, 1, 2, len(data), 2],
    })

    column_chart = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})
    for i in range(3, len(headers) + 1):
        column_chart.add_series({
            'name':       [data_sheet_name, 0, i],
            'categories': [data_sheet_name, 1, 1, len(data), 1],
            'values':     [data_sheet_name, 1, i, len(data), i],
            'gap':        2,
        })
    column_chart.set_legend({'font': {'size': 14, 'bold': 1}})
    # Combine the charts.
    line_chart.combine(column_chart)
    line_chart.set_x_axis({
        'num_font':  {'rotation': -90},
    })

    """ Insert the line_chart into the chart_sheet (with an offset) """
    offset = get_offset(chart_sheet_name)
    line_chart.set_size({'width': 600, 'height': 260})
    chart_sheet.insert_chart(offset, 1, line_chart, {
        'x_offset': 0, 'y_offset': 0, 'x_scale': x_scale, 'y_scale': y_scale})
    increase_offset(chart_sheet_name)
    workbook.set_size(1920, 1280)


def draw_generic_line_chart(df, chart_sheet_name, data_sheet_name, chart_title, chart_y_axis='IOPS', x_scale=2.5, y_scale=2.5):
    """ Write into a new data sheet """
    df.to_excel(g_excel_writer, sheet_name=data_sheet_name)

    """ Get an existing chart sheet """
    workbook = g_excel_writer.book
    chart_sheet = workbook.get_worksheet_by_name(chart_sheet_name)
    chart_sheet.set_column(1, 2, 17)
    headers = df.columns.values.tolist()
    chart_sheet.set_column(2, len(headers), 12)

    """ Create a new line_chart object """
    line_chart = workbook.add_chart({'type': 'line'})
    line_chart.set_title({'name': chart_title})
    line_chart.set_x_axis({'name': 'DateTime', 'name_font': {
        'size': 14, 'bold': True}})
    line_chart.set_y_axis(
        {'name': chart_y_axis, 'name_font': {'size': 14, 'bold': True}})

    """ Set an Excel line_chart style """
    line_chart.set_style(10)
    line_chart.set_legend({'font': {'size': 14, 'bold': 1}})
    data = df.values.tolist()
    for i in range(2, len(headers) + 1):
        line_chart.add_series({
            'name':       [data_sheet_name, 0, i],
            'categories': [data_sheet_name, 1, 1, len(data), 1],
            'values':     [data_sheet_name, 1, i, len(data), i],
            'gap':        2,
        })

    line_chart.set_x_axis({
        'num_font':  {'rotation': -90},
    })

    """ Insert the line_chart into the chart_sheet (with an offset) """
    offset = get_offset(chart_sheet_name)
    line_chart.set_size({'width': 600, 'height': 260})
    chart_sheet.insert_chart(offset, 1, line_chart, {
        'x_offset': 0, 'y_offset': 0, 'x_scale': x_scale, 'y_scale': y_scale})
    increase_offset(chart_sheet_name)
    workbook.set_size(1920, 1280)


# def analyze_generic_datetime_and_one_data(start_str, end_str, columns_dict):
#     dict_len = len(columns_dict)
#     lines = get_lines_between_marker_lines(start_str, end_str)
#     cur_date_time = ""
#     former_date_time = ""
#     row_list = []
#     rows_list = []
#     for line in lines:
#         word_list = line.split()
#         cur_one_ops_num = ""
#         if is_date(word_list[0]):
#             cur_date_time = word_list[0] + " " + word_list[1]
#             if cur_date_time != former_date_time and len(row_list) != 0:
#                 rows_list.append(row_list)
#             row_list = [0] * dict_len
#             row_list[0] = cur_date_time
#
#             cur_one_ops_num = word_list[2]
#             if cur_one_ops_num == '-':
#                 cur_one_ops_num = 0
#             row_list[1] = cur_one_ops_num
#
#     df = pd.DataFrame(np.array(rows_list).reshape(
#         len(rows_list), dict_len), columns=columns_dict.keys())
#     for key in columns_dict:
#         if key == 'date_time':
#             continue
#         df[key] = df[key].astype(int)
#     df = df.sort_values(by=df.columns[0])
#     df = df.reset_index(drop=True)
#     return df

def analyze_generic_file_list(file_name_list, column_name_list, interval=3600, skiprows=1, header=None):
    """ To handle column_name_list = ['date_time', 'value', 'max', 'min', 'mean'] """
    df_cat = pd.DataFrame(columns=column_name_list)
    index = 0
    for fn in file_name_list:
        df = pd.read_csv(fn, delim_whitespace=True,
                         skiprows=skiprows, header=header)
        #df.columns = column_name_list
        for i in range(0, int(len(df) / interval)):
            s = df.iloc[i * interval:(i + 1) * interval][2]
            s_max = s.max()
            s_min = s.min()
            s_mean = int(s.mean())
            s_date_time = df.iloc[i * interval][0] + \
                ' ' + df.iloc[i * interval][1]
            s_value = df.iloc[i * interval][2]
            df_cat.loc[index] = [s_date_time, s_value, s_max, s_min, s_mean]
            index += 1

    df_cat = sort_df_by_date_time(df_cat)
    return df_cat


def analyze_nfs_generic_breakdown_file_list(file_name_list, column_name_dict, interval=3600):
    df_cat = pd.DataFrame(columns=column_name_dict.keys())
    index = 0
    for fn in file_name_list:
        """ A tricky workaround here to read irregular file """
        """
                1            2      3      4      5
0       2017-11-2     18:47:56  27154  15924   read
1            7255        write    NaN    NaN    NaN
2            2884      getattr    NaN    NaN    NaN
3             939       access    NaN    NaN    NaN
4              66       commit    NaN    NaN    NaN
5              58       fsstat    NaN    NaN    NaN
6              19  readdirplus    NaN    NaN    NaN
7               6       lookup    NaN    NaN    NaN
8               2       remove    NaN    NaN    NaN
9               1       create    NaN    NaN    NaN
        
        """
        df = read_irregular_file(fn)

        non_na_index_list = df.dropna().index.tolist()
        dict_len = len(column_name_dict)
        df_interval = pd.DataFrame(columns=column_name_dict.keys())
        df_index = 0
        """ Only deal with data from interval seconds, otherwise too time-consuming """
        index = 0
        while index < len(non_na_index_list):
            elem = non_na_index_list[index]

            next_elem = non_na_index_list[index + 1]
            row_list = [0] * dict_len
            row_list[0] = df.iloc[elem, 0] + ' ' + df.iloc[elem, 1]
            row_list[1] = df.iloc[elem, 2]
            """ Skip to analyze 0 OPS/SEC further """
            if row_list[1] != '0' and row_list[1] != 0:
                row_list[column_name_dict[df.iloc[elem, 4]]] = df.iloc[elem, 3]
                for other_ops_index in range(elem + 1, next_elem):
                    row_list[column_name_dict[df.iloc[other_ops_index, 1]]
                             ] = df.iloc[other_ops_index, 0]
            df_interval.loc[df_index] = row_list
            df_index += 1
            index += interval
        df_cat = df_cat.append(df_interval)

    for key in column_name_dict:
        if key == 'date_time':
            continue
        else:
            df_cat[key] = df_cat[key].astype(int)

    df_cat = sort_df_by_date_time(df_cat)
    return df_cat


def analyze_nfs_latency_breakdown_file_list(file_name_list, column_name_dict, interval=3600):
    df_cat = pd.DataFrame(columns=column_name_dict.keys())
    index = 0
    for fn in file_name_list:
        df = read_irregular_file(fn)

        non_na_index_list = df.dropna().index.tolist()
        dict_len = len(column_name_dict)
        df_interval = pd.DataFrame(columns=column_name_dict.keys())
        df_index = 0
        """ Only deal with data from interval seconds, otherwise too time-consuming """
        index = 0
        while index < len(non_na_index_list):
            elem = non_na_index_list[index]

            next_elem = non_na_index_list[index + 1]
            row_list = [0] * dict_len
            row_list[0] = df.iloc[elem, 0] + ' ' + df.iloc[elem, 1]
            val = df.iloc[elem, 3]
            if val != '0' and val != 0 and val.find('-') == -1:
                row_list[1] = val
            """ Unit: Second """
            if row_list[2] != '0' and row_list[2] != 0 and row_list[2].find('-') == -1:
                row_list[2] = float((int(df.iloc[elem, 4])) / 1000000.0)
            print(row_list)
            df_interval.loc[df_index] = row_list
            df_index += 1
            index += interval
        df_cat = df_cat.append(df_interval)

    for key in column_name_dict:
        if key == 'date_time':
            continue
        elif key == 'latency_in_sec':
            df_cat[key] = df_cat[key].astype(float)
        else:
            df_cat[key] = df_cat[key].astype(int)

    df_cat = sort_df_by_date_time(df_cat)
    return df_cat


def analyze_disk_io_genric_breakdown_file_list(file_name_list, column_name_dict, interval=3600):
    df_cat = pd.DataFrame(columns=column_name_dict.keys())
    index = 0
    for fn in file_name_list:
        df = read_irregular_file(fn)

        non_na_index_list = df.dropna().index.tolist()
        dict_len = len(column_name_dict)
        df_interval = pd.DataFrame(columns=column_name_dict.keys())
        df_index = 0
        """ Only deal with data from interval seconds, otherwise too time-consuming """
        index = 0
        while index < len(non_na_index_list):
            elem = non_na_index_list[index]
            """
            DISK_IO_OPS_RW_STAT_NAME_DICT = {
                "date_time": 0,
                "total_ops_per_sec": 1,
                "write": 2,
                "read": 3,
            }
            """
            next_elem = non_na_index_list[index + 1]
            row_list = [0] * dict_len
            row_list[0] = df.iloc[elem, 0] + ' ' + df.iloc[elem, 1]
            row_list[1] = df.iloc[elem, 2]
            row_list[2] = df.iloc[elem, 3]
            row_list[3] = df.iloc[elem + 1, 0]
            df_interval.loc[df_index] = row_list
            df_index += 1
            index += interval
        df_cat = df_cat.append(df_interval)

    for key in column_name_dict:
        if key == 'date_time':
            continue
        else:
            df_cat[key] = df_cat[key].astype(int)

    df_cat = sort_df_by_date_time(df_cat)
    print(df_cat)
    return df_cat


def analyze_disk_io_size_breakdown_file_list(file_name_list, interval=3600):
    def get_dict_index(x):
        x = int(x / 1024)
        count = 0
        while x >= 1.0:
            x = x / 2
            count += 1
        count += col_dict['io_size <1k']
        if count >= len(col_dict):
            count = len(col_dict) - 1
        return count

    col_list = ['date_time', 'total_iops', 'io_size <1k', 'io_size >=1k', 'io_size >=2k', 'io_size >=4k',
                'io_size >=8k', 'io_size >=16k', 'io_size >=32k', 'io_size >=64k', 'io_size >=128k', 'io_size >=256k', 'io_size >=512k']
    col_dict = dict(zip(col_list, range(0, len(col_list))))

    df_cat = pd.DataFrame(columns=col_dict.keys())
#     file_num = 0
    for fn in file_name_list:
        #         if file_num > 2:
        #             break
        #         file_num += 1
        df = read_irregular_file(fn)

        non_na_index_list = df.dropna().index.tolist()
        dict_len = len(col_dict)
        df_interval = pd.DataFrame(columns=col_dict.keys())
        df_index = 0
        """ Only deal with data from interval seconds, otherwise too time-consuming """
        index = 0
        while index < len(non_na_index_list):
            elem = non_na_index_list[index]
            next_elem = non_na_index_list[index + 1]

            row_list = [0] * dict_len
            row_list[0] = df.iloc[elem, 0] + ' ' + df.iloc[elem, 1]
            row_list[1] = int(df.iloc[elem, 2])
            if row_list[1] != 0:
                col_index = get_dict_index(int(df.iloc[elem, 4]))
                row_list[col_index] += int(df.iloc[elem, 3])

                for j in range(elem + 1, next_elem):
                    col_index = get_dict_index(int(df.iloc[j, 1]))
                    row_list[col_index] += int(df.iloc[j, 0])
            print(row_list)
            df_interval.loc[df_index] = row_list
            df_index += 1
            index += interval
        df_cat = df_cat.append(df_interval)

    for key in col_dict:
        if key == 'date_time':
            continue
        else:
            df_cat[key] = df_cat[key].astype(int)

    df_cat = sort_df_by_date_time(df_cat)
    print(df_cat)
    return df_cat


def plot_excel_charts():
    """ NFSv3 """

    fn = 'nfs3.ops.txt'
    print("=== Step 1 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'iops', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NFSv3_IOPS', 'Hourly NFSv3 IOPS')

    fn = 'nfs3.ops_op.txt'
    print("=== Step 2 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_generic_breakdown_file_list(
        file_name_list, NFSV3_OPS_STAT_NAME_DICT)
    draw_nfs_combined_chart(df, SN_IO_PROFILE, 'NFSv3_BREAKDOWN_BY_OPS',
                            'Hourly NFSv3 IOPS Breakdown by Operation')

    fn = 'nfs3.ops_size.txt'
    print("=== Step 3 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_disk_io_size_breakdown_file_list(file_name_list)
    draw_nfs_combined_chart(df, SN_IO_PROFILE, 'NFSv3_IOPS_BREAKDOWN_BS',
                            'Hourly NFSv3 IOPS Breakdown by Block Size', 'NFSv3 IOPS')

    fn = 'nfs3.ops_latency.txt'
    print("=== Step 4 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_latency_breakdown_file_list(
        file_name_list, NFSV3_OPS_LATENCY_STAT_NAME_DICT)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'NFSv3_BREAKDOWN_LATENCY',
                            'Hourly NFSv3 IOPS Breakdown by Latency')

    """ NFSv4 """

    fn = 'nfs4.ops.txt'
    print("=== Step 5 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'iops', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NFSv4_IOPS', 'Hourly NFSv4 IOPS')

    fn = 'nfs4.ops_op.txt'
    print("=== Step 6 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_generic_breakdown_file_list(
        file_name_list, NFSV4_OPS_STAT_NAME_DICT)
    draw_nfs_combined_chart(df, SN_IO_PROFILE, 'NFSv4_BREAKDOWN_BY_OPS',
                            'Hourly NFSv4 IOPS Breakdown by Operation')

    fn = 'nfs4.ops_size.txt'
    print("=== Step 7 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_disk_io_size_breakdown_file_list(file_name_list)
    draw_nfs_combined_chart(df, SN_IO_PROFILE, 'NFSv4_IOPS_BREAKDOWN_BS',
                            'Hourly NFSv4 IOPS Breakdown by Block Size', 'NFSv4 IOPS')

    fn = 'nfs4.ops_latency.txt'
    print("=== Step 8 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_latency_breakdown_file_list(
        file_name_list, NFSV3_OPS_LATENCY_STAT_NAME_DICT)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'NFSv4_BREAKDOWN_LATENCY',
                            'Hourly NFSv4 IOPS Breakdown by Latency')

    """ Disk IO """

    fn = 'io.ops_op.txt'
    print("=== Step 9 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, np)
    df = analyze_disk_io_genric_breakdown_file_list(
        file_name_list, DISK_IO_OPS_RW_STAT_NAME_DICT)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'DISK_IOPS_BREAKDOWN_RW',
                            'Hourly Disk IOPS Breakdown by Read/Write', 'DISK IOPS')

    fn = 'io.bytes.txt'
    print("=== Step 10 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, np)
    df = analyze_disk_io_genric_breakdown_file_list(
        file_name_list, DISK_IO_KB_PER_SEC_RW_STAT_NAME_DICT)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'DISK_KB_PER_SEC_BREAKDOWN_RW',
                            'Hourly Disk KB/SEC Breakdown by Read/Write', 'DISK KB/SEC')

    """ NIC IO """

    fn = 'nic.kilobytes.txt'
    print("=== Step 11 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'nic_kb_per_sec', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NIC_KB_PER_SEC', 'Hourly NIC KB/SEC', 'NIC KB/Sec')

    """ SN_OS_LOAD """

    """ ARC """

    fn = 'arc.hitratio.txt'
    print("=== Step 12 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, np)
    column_name_list = ['date_time', 'arc_hit_ratio', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(df, SN_OS_LOAD, 'ZFS_ARC_HIT_RATIO',
                            'Hourly ZFS ARC HIT RATIO', 'ARC Hit Ratio')

    """ CPU """

    fn = 'cpu_utilization.txt'
    print("=== Step 13 ===:" + fn)
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'cpu_utilization', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(df, SN_OS_LOAD, 'CPU_UTIL',
                            'Hourly CPU UTILIZATION', 'CPU Utilization')


if __name__ == "__main__":
    options, args = parse_opts(sys.argv[1:])

    g_one_storage_data_dir_name = options.opcDataDir
    #g_one_storage_data_dir_name = '/Users/mmyu/Tools/eclipse463/workspace/plot_data_in_excel/rawdata2/cfeis01nas41_analytics'
    g_excel_file_name = os.path.basename(
        g_one_storage_data_dir_name).split('_')[0] + ".xlsx"
    """ Create a Pandas Excel g_excel_writer using XlsxWriter as the engine """
    g_excel_writer = pd.ExcelWriter(g_excel_file_name, engine='xlsxwriter')
    init_chart_worksheets()
    plot_excel_charts()
    g_excel_writer.book.close()
