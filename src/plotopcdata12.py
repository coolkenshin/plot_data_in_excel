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
import datetime

"""
Global variables:
"""
g_summary_file_name = ""
g_one_storage_data_dir_name = ""
g_excel_file_name = ""
g_excel_writer = None
g_io_profile_chart_offset = 5
g_os_load_chart_offset = 5
DBG_LEVEL1 = 1
DBG_LEVEL2 = 2
DBG_LEVEL3 = 3


""" Tunables """
g_debug = True
g_debug_level = DBG_LEVEL1

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
    "latency_in_millisecond": 3
}

"""
Reference: https://grok.cz.oracle.com/source/xref/ak-2013-rel-fish-clone/usr/src/appliance/nas/modules/stat/nfsstat/common/nas_nfsstat.c
"""
# NFSV3_OPS_STAT_NAME_DICT = {
#     "date_time": 0,
#     "total_ops_num": 1,
#     "access": 2,
#     "commit": 3,
#     "create": 4,
#     "fsinfo": 5,
#     "fsstat": 6,
#     "getattr": 7,
#     "link": 8,
#     "lookup": 9,
#     "mkdir": 10,
#     "mknod": 11,
#     "pathconf": 12,
#     "read": 13,
#     "readdir": 14,
#     "readdirplus": 15,
#     "readlink": 16,
#     "remove": 17,
#     "rename": 18,
#     "rmdir": 19,
#     "setattr": 20,
#     "symlink": 21,
#     "write": 22
# }
NFSV3_OPS_STAT_NAME_DICT = {
    "date_time": 0,
    "total_ops_num": 0,
    "access": 0,
    "commit": 0,
    "create": 0,
    "fsinfo": 0,
    "fsstat": 0,
    "getattr": 0,
    "link": 0,
    "lookup": 0,
    "mkdir": 0,
    "mknod": 0,
    "pathconf": 0,
    "read": 0,
    "readdir": 0,
    "readdirplus": 0,
    "readlink": 0,
    "remove": 0,
    "rename": 0,
    "rmdir": 0,
    "setattr": 0,
    "symlink": 0,
    "write": 0
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


def d_print(var, level):
    if g_debug:
        if(level >= g_debug_level):
            print(var)


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
    first_line = df.columns.tolist()
    df.loc[-1] = df.columns.tolist()
    df.index = df.index + 1
    df = df.sort_index()
    df.columns = [0, 1, 2, 3, 4]
    d_print(first_line, DBG_LEVEL3)
    df.replace(['-', '-.1', '-.2', '1.1', '2.1', '5706.1', '1239.1', '946.1', '38686.1', '343.1', '584.1', '181.1', '943.1', '148.1', '962.1', '6759.1', '995.1', '1081.1', '4002.1', '874.1', '1772.1'],
               [0, 0, 0, 1, 2, 5706, 1239, 946, 38686, 343, 584, 181, 943, 148, 962, 6759, 995, 1081, 4002, 874, 1772], inplace=True)
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


def draw_generic_combined_chart(df, chart_sheet_name, data_sheet_name, chart_title, chart_y_axis='IOPS', x_scale=2.5, y_scale=2.5):
    df.to_excel(g_excel_writer, data_sheet_name)
    workbook = g_excel_writer.book
    data_sheet = workbook.get_worksheet_by_name(data_sheet_name)
    data_sheet.set_column(1, 2, 17)
    headers = df.columns.values.tolist()
    data_sheet.set_column(2, len(headers), 12)

    """ Get an existing chart sheet """
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
    workbook = g_excel_writer.book
    data_sheet = workbook.get_worksheet_by_name(data_sheet_name)
    data_sheet.set_column(1, 2, 17)
    headers = df.columns.values.tolist()
    data_sheet.set_column(2, len(headers), 12)

    """ Get an existing chart sheet """
    chart_sheet = workbook.get_worksheet_by_name(chart_sheet_name)

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


def _get_row_dict(df, column_name_dict, start_row, end_row):
    mydict = column_name_dict.copy()
#     mydict_keys = list(mydict.keys())
#     print(type(mydict_keys[0]))
#     print(mydict_keys[1])
    mydict['date_time'] = df.iloc[start_row, 0] + ' ' + df.iloc[start_row, 1]
    mydict['total_ops_num'] = int(df.iloc[start_row, 2])

    if mydict['total_ops_num'] != 0 and start_row != end_row:
        mydict[str(df.iloc[start_row, 4])] = int(df.iloc[start_row, 3]) 
        newdf = df.loc[start_row+1:end_row, [1, 0]]
        #newdf[0] = newdf[0].astype(int)
        mydict_extra = newdf.set_index(1)[0].to_dict()
        mydict.update(mydict_extra)
    newdf = pd.DataFrame.from_dict(mydict, 'index').T
    return newdf

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
        if g_debug:
            print(fn)
            #tmp_fn = '../rawdata2/cfeis01nas83_analytics/cfeis01nas832017-11-09_21_07_07_UTC/nfs4.ops_op.tx'
            tmp_fn = '/Users/mmyu/Tools/eclipse463/workspace/plot_data_in_excel/rawdata2/cfeis01nas11_analytics/cfeis01nas112017-11-03_18_49_03_UTC/nfs3.ops_op.txt'
            if fn != tmp_fn:
                continue

        df = read_irregular_file(fn)
        d_print(fn, DBG_LEVEL3)
        d_print(df, DBG_LEVEL2)

        non_na_index_list = df.dropna().index.tolist()
        dict_len = len(column_name_dict)
        df_interval = pd.DataFrame(columns=column_name_dict.keys())
        """ Only deal with data from interval seconds, otherwise too time-consuming """
        index = 0
        while index < len(non_na_index_list):
            elem = non_na_index_list[index]
            if index == len(non_na_index_list)-1:
                next_elem = len(df)
            else:
                next_elem = non_na_index_list[index + 1]
            one_df = _get_row_dict(df, column_name_dict, elem, next_elem-1)
            df_interval = df_interval.append(one_df, ignore_index=True)
            #d_print(one_df, DBG_LEVEL3)
            index += interval
            print("elem: ", elem)
        d_print(df_interval, DBG_LEVEL3)
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
        d_print(fn, DBG_LEVEL3)
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
            if val != 0:
                row_list[1] = val
                """ Unit: Millisecond """
                row_list[2] = float((int(df.iloc[elem, 4])) / 1000.0)
            df_interval.loc[df_index] = row_list
            d_print(row_list, DBG_LEVEL3)
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
        d_print(fn, DBG_LEVEL3)
        d_print(df, DBG_LEVEL3)
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
            ATTENTION: write and read is not in fixed sequence,
                       sometimes only have one, either read or write
            """
            next_elem = non_na_index_list[index + 1]
            row_list = [0] * dict_len
            row_list[0] = df.iloc[elem, 0] + ' ' + df.iloc[elem, 1]
            row_list[1] = df.iloc[elem, 2]

            ops = df.iloc[elem, 4]
            ops_index = column_name_dict[ops]
            row_list[ops_index] = df.iloc[elem, 3]
            
            if (next_elem - elem) == 2:
                ops = df.iloc[elem+1, 1]
                ops_index = column_name_dict[ops]
                row_list[ops_index] = df.iloc[elem+1, 0]
            
            df_interval.loc[df_index] = row_list
            df_index += 1
            index += interval
        df_cat = df_cat.append(df_interval)

    d_print(df_cat, DBG_LEVEL3)
    for key in column_name_dict:
        if key == 'date_time':
            continue
        else:
            d_print(key, DBG_LEVEL3)
            df_cat[key] = df_cat[key].astype(int)

    df_cat = sort_df_by_date_time(df_cat)
#     print(df_cat)
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
#             print(row_list)
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
#     print(df_cat)
    return df_cat


def plot_nfs3_ops(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'iops', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NFSv3_IOPS', 'Hourly NFSv3 IOPS')


def plot_nfs3_ops_op(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_generic_breakdown_file_list(
        file_name_list, NFSV3_OPS_STAT_NAME_DICT, interval=1)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv3_BREAKDOWN_BY_OPS',
                                'Hourly NFSv3 IOPS Breakdown by Operation')


def plot_nfs3_ops_size(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_disk_io_size_breakdown_file_list(file_name_list)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv3_IOPS_BREAKDOWN_BS',
                                'Hourly NFSv3 IOPS Breakdown by Block Size', 'NFSv3 IOPS')


def plot_nfs3_ops_latency(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_latency_breakdown_file_list(
        file_name_list, NFSV3_OPS_LATENCY_STAT_NAME_DICT)
    d_print(df, DBG_LEVEL2)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'NFSv3_BREAKDOWN_LATENCY',
                            'Hourly NFSv3 OPS Breakdown by Latency', 'OPS Count and Latency in Millisecond')


def plot_nfs4_ops(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'iops', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NFSv4_IOPS', 'Hourly NFSv4 IOPS')


def plot_nfs4_ops_op(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_generic_breakdown_file_list(
        file_name_list, NFSV4_OPS_STAT_NAME_DICT)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv4_BREAKDOWN_BY_OPS',
                                'Hourly NFSv4 IOPS Breakdown by Operation')


def plot_nfs4_ops_size(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_disk_io_size_breakdown_file_list(file_name_list)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv4_IOPS_BREAKDOWN_BS',
                                'Hourly NFSv4 IOPS Breakdown by Block Size', 'NFSv4 IOPS')


def plot_nfs4_ops_latency(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_nfs_latency_breakdown_file_list(
        file_name_list, NFSV3_OPS_LATENCY_STAT_NAME_DICT)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'NFSv4_BREAKDOWN_LATENCY',
                            'Hourly NFSv4 OPS Breakdown by Latency', 'OPS Count and Latency in Millisecond')


def plot_io_ops_op(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_disk_io_genric_breakdown_file_list(
        file_name_list, DISK_IO_OPS_RW_STAT_NAME_DICT)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'DISK_IOPS_BREAKDOWN_RW',
                            'Hourly Disk IOPS Breakdown by Read/Write', 'DISK IOPS')


def plot_io_bytes(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    df = analyze_disk_io_genric_breakdown_file_list(
        file_name_list, DISK_IO_KB_PER_SEC_RW_STAT_NAME_DICT)
    draw_generic_line_chart(df, SN_IO_PROFILE, 'DISK_KB_PER_SEC_BREAKDOWN_RW',
                            'Hourly Disk KB/SEC Breakdown by Read/Write', 'DISK KB/SEC')


def plot_nic_kilobytes(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'nic_kb_per_sec', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NIC_KB_PER_SEC', 'Hourly NIC KB/SEC', 'NIC KB/Sec')


def plot_arc_hitratio(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'arc_hit_ratio', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(df, SN_OS_LOAD, 'ZFS_ARC_HIT_RATIO',
                            'Hourly ZFS ARC HIT RATIO', 'ARC Hit Ratio')


def plot_cpu_utilization(fn):
    file_name_list = get_file_list_in_dir(g_one_storage_data_dir_name, fn)
    column_name_list = ['date_time', 'cpu_utilization', 'max', 'min', 'mean']
    df = analyze_generic_file_list(file_name_list, column_name_list)
    draw_generic_line_chart(df, SN_OS_LOAD, 'CPU_UTIL',
                            'Hourly CPU UTILIZATION', 'CPU Utilization')


def plot_excel_all_charts():
    dig_file_name_list = [
        'nfs3.ops.txt',
        'nfs3.ops_op.txt',
        'nfs3.ops_size.txt',
        'nfs3.ops_latency.txt',
        'nfs4.ops.txt',
        'nfs4.ops_op.txt',
        'nfs4.ops_size.txt',
        'nfs4.ops_latency.txt',
        'io.ops_op.txt',
        'io.bytes.txt',
        'nic.kilobytes.txt',
        'arc.hitratio.txt',
        'cpu_utilization.txt'
    ]
    plot_func_name_list = [
        plot_nfs3_ops,
        plot_nfs3_ops_op,
        plot_nfs3_ops_size,
        plot_nfs3_ops_latency,
        plot_nfs4_ops,
        plot_nfs4_ops_op,
        plot_nfs4_ops_size,
        plot_nfs4_ops_latency,
        plot_io_ops_op,
        plot_io_bytes,
        plot_nic_kilobytes,
        plot_arc_hitratio,
        plot_cpu_utilization
    ]
    fname_2_func = dict(zip(dig_file_name_list, plot_func_name_list))
    for i, fname in enumerate(fname_2_func):
        print("=== Step {} ===: {}".format(i + 1, fname))
        start_time = datetime.datetime.now().replace(microsecond=0)
        func = fname_2_func[fname]
        func(fname)
        end_time = datetime.datetime.now().replace(microsecond=0)
        print("Time used: {}".format(end_time - start_time))

# if __name__ == "__main__":
#     options, args = parse_opts(sys.argv[1:])
#  
#     g_one_storage_data_dir_name = options.opcDataDir
#     #g_one_storage_data_dir_name = '/Users/mmyu/Tools/eclipse463/workspace/plot_data_in_excel/rawdata2/cfeis01nas41_analytics'
#     g_excel_file_name = os.path.basename(
#         g_one_storage_data_dir_name).split('_')[0] + ".xlsx"
#     """ Create a Pandas Excel g_excel_writer using XlsxWriter as the engine """
#     g_excel_writer = pd.ExcelWriter(g_excel_file_name, engine='xlsxwriter')
#     init_chart_worksheets()
#     plot_excel_all_charts()
#     g_excel_writer.book.close()


if __name__ == "__main__":
    options, args = parse_opts(sys.argv[1:])
  
    g_one_storage_data_dir_name = options.opcDataDir
    g_one_storage_data_dir_name = '/Users/mmyu/Tools/eclipse463/workspace/plot_data_in_excel/rawdata2/cfeis01nas11_analytics'
    g_excel_file_name = os.path.basename(
        g_one_storage_data_dir_name).split('_')[0] + ".xlsx"
    """ Create a Pandas Excel g_excel_writer using XlsxWriter as the engine """
    g_excel_writer = pd.ExcelWriter(g_excel_file_name, engine='xlsxwriter')
    init_chart_worksheets()
  
    #plot_nfs3_ops_latency('nfs3.ops_latency.txt')
    #plot_nfs4_ops_op('nfs4.ops_op.txt')
    #plot_io_ops_op('io.ops_op.txt')
    start_time = datetime.datetime.now().replace(microsecond=0)
    plot_nfs3_ops_op('nfs3.ops_op.txt')
    end_time = datetime.datetime.now().replace(microsecond=0)
    print("Time used: {}".format(end_time - start_time))
  
    g_excel_writer.book.close()
