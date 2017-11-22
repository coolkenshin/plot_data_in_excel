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

NFSV3_OPS_RW_STAT_NAME_DICT = {
    "date_time": 0,
    "write": 1,
    "read": 2,
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


def draw_nfs_combined_chart(df, sheet_name, chart_title, x_scale=2.5, y_scale=2.5):
    data_sheet_name = sheet_name + "_DATA"
    df.to_excel(g_excel_writer, data_sheet_name)

    headers = df.columns.values.tolist()
    workbook = g_excel_writer.book
    #worksheet = workbook.get_worksheet_by_name(sheet_name)
    chart_sheet_name = sheet_name + "_CHART"
    worksheet = workbook.add_worksheet(chart_sheet_name)
    worksheet.set_column(1, 2, 17)
    worksheet.set_column(2, len(headers), 12)

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
        'num_font':  {'rotation': -45},
        'major_gridlines': {
            'visible': True,
            'line': {'width': 0.15, 'dash_type': 'dash'}
        },
    })

    """ Insert the line_chart into the worksheet (with an offset) """
    worksheet.insert_chart(5, 1, line_chart, {
                           'x_offset': 25, 'y_offset': 10, 'x_scale': x_scale, 'y_scale': y_scale})
    workbook.set_size(1920, 1280)


def draw_generic_line_chart(df, chart_sheet_name, data_sheet_name, chart_title, x_scale=2.5, y_scale=2.5):
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
        {'name': 'IOPS', 'name_font': {'size': 14, 'bold': True}})

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
        'major_gridlines': {
            'visible': True,
            'line': {'width': 0.15, 'dash_type': 'dash'}
        },
    })

    """ Insert the line_chart into the chart_sheet (with an offset) """
    chart_sheet.insert_chart(5, 1, line_chart, {
        'x_offset': 25, 'y_offset': 10, 'x_scale': x_scale, 'y_scale': y_scale})
    workbook.set_size(1920, 1280)


def analyze_iscsi_ops_breakdown():
    start_str = "Max iSCSI I/O size:"
    end_str = "NFS Client Count:"
    return analyze_generic_datetime_and_one_data(start_str, end_str, ISCSI_OPS_STAT_NAME_DICT)


def analyze_nfsv4_highest_100():
    start_str = "NFSv4 Op Size, highest 100 ops/sec broken down by size:"
    end_str = "NFSv4 Max operations KB/sec :"
    return analyze_generic_datetime_and_one_data(start_str, end_str, NFSV4_HIGHEST_100_OPS_STAT_NAME_DICT)


def analyze_nfsv3_breakdown_by_size():
    start_str = "NFSv3 Op Size, highest 100 ops/sec broken down by size:"
    end_str = "Max NFSv4 IOPs"
    return analyze_generic_datetime_and_one_data(start_str, end_str, NFSV3_OPS_STAT_BY_SIZE_NAME_DICT)


def analyze_generic_datetime_and_one_data(start_str, end_str, columns_dict):
    dict_len = len(columns_dict)
    lines = get_lines_between_marker_lines(start_str, end_str)
    cur_date_time = ""
    former_date_time = ""
    row_list = []
    rows_list = []
    for line in lines:
        word_list = line.split()
        cur_one_ops_num = ""
        if is_date(word_list[0]):
            cur_date_time = word_list[0] + " " + word_list[1]
            if cur_date_time != former_date_time and len(row_list) != 0:
                rows_list.append(row_list)
            row_list = [0] * dict_len
            row_list[0] = cur_date_time

            cur_one_ops_num = word_list[2]
            if cur_one_ops_num == '-':
                cur_one_ops_num = 0
            row_list[1] = cur_one_ops_num

    df = pd.DataFrame(np.array(rows_list).reshape(
        len(rows_list), dict_len), columns=columns_dict.keys())
    for key in columns_dict:
        if key == 'date_time':
            continue
        df[key] = df[key].astype(int)
    df = df.sort_values(by=df.columns[0])
    df = df.reset_index(drop=True)
    return df


def analyze_nfsv3_rw_ops(start_str, end_str, columns_dict):
    dict_len = len(columns_dict)
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
            row_list = [0] * dict_len
            row_list[0] = cur_date_time

            cur_one_ops_num = int(word_list[2])
            cur_one_ops_name = word_list[3]
            if cur_one_ops_name == '-':
                continue
            ops_index = columns_dict[cur_one_ops_name]
            row_list[ops_index] = cur_one_ops_num

    df = pd.DataFrame(np.array(rows_list).reshape(
        len(rows_list), dict_len), columns=columns_dict.keys())
    for key in columns_dict:
        if key == 'date_time':
            continue
        df[key] = df[key].astype(int)
    df = df.sort_values(by=df.columns[0])
    df = df.reset_index(drop=True)
    return df


def analyze_nfsv3_rw():
    start_str = "Highest NFSv3 Read and Write operations broken down by time:"
    end_str = "Avg NFSv3 IOPs:"
    return analyze_nfsv3_rw_ops(start_str, end_str, NFSV3_OPS_RW_STAT_NAME_DICT)


def analyze_nfsv3_breakdown_by_ops_name():
    start_str = "NFS Operations during peak NFSv3 ops:"
    end_str = "Highest NFSv3 Read and Write operations broken down by time:"
    return analyze_nfs_ops_breakdown(start_str, end_str, NFSV3_OPS_STAT_NAME_DICT)


def analyze_nfsv4_breakdown_by_ops_name():
    start_str = "NFS Operations during peak NFSv4 ops:"
    end_str = "Avg NFSv4 IOPs:"
    return analyze_nfs_ops_breakdown(start_str, end_str, NFSV4_OPS_STAT_NAME_DICT)


def analyze_nfs_ops_breakdown(start_str, end_str, columns_dict):
    dict_len = len(columns_dict)
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
            row_list = [0] * dict_len
            row_list[0] = cur_date_time

            cur_total_iops_num = word_list[2]
            row_list[1] = int(cur_total_iops_num)
            cur_one_ops_num = word_list[3]
            cur_one_ops_name = word_list[4]
            if cur_one_ops_name == '-':
                continue
            ops_index = columns_dict[cur_one_ops_name]
            row_list[ops_index] = int(cur_one_ops_num)

        else:
            former_date_time = cur_date_time
            cur_one_ops_num = word_list[0]
            cur_one_ops_name = word_list[1]
            ops_index = columns_dict[cur_one_ops_name]
            row_list[0] = cur_date_time
            row_list[ops_index] = int(cur_one_ops_num)

    df = pd.DataFrame(np.array(rows_list).reshape(
        len(rows_list), dict_len), columns=columns_dict.keys())
    for key in columns_dict:
        if key == 'date_time':
            continue
        df[key] = df[key].astype(int)

    return df


# def plot_excel_charts():
#     df = analyze_nfsv3_breakdown_by_ops_name()
#     draw_nfs_combined_chart(df, 'NFSv3_BREAKDOWN_BY_OPS',
#                             'NFS Operations During Peak NFSv3 Ops', 2.5, 4.0)
#
#     df = analyze_nfsv4_breakdown_by_ops_name()
#     draw_nfs_combined_chart(df, 'NFSv4_BREAKDOWN_BY_OPS',
#                             'NFS Operations During Peak NFSv4 Ops', 5.0, 5.0)
#
#     df = analyze_nfsv3_rw()
#     draw_generic_line_chart(df, 'NFSv3_RW_BREAKDOWN',
#                             'Highest NFSv3 Read and Write Operations Broken Down By Time', 2.5, 2.5)
#
#     df = analyze_nfsv4_highest_100()
#     draw_generic_line_chart(df, 'NFSv4_TOP_100_IOPS',
#                             'Highest NFSv4 IOPS', 2.5, 2.5)
#
#     df = analyze_iscsi_ops_breakdown()
#     draw_generic_line_chart(df, 'iSCSI_BREAKDOWN_BY_LATENCY', 'iSCSI IOPS')
#
#     df = analyze_nfsv3_breakdown_by_size()
#     draw_generic_line_chart(df, 'NFSv3_BREAKDOWN_BY_SIZE',
#                             'NFSv3 Highest 100 IOPS Broken Down By Size')


def plot_excel_charts2():
    #     file_name_list = get_file_list_in_dir_by_file_name(g_one_storage_data_dir_name, 'nfs3.ops.txt')
    #     column_name_list = ['date_time', 'value', 'max', 'min', 'mean']
    #     df = analyze_generic_file_list(file_name_list, column_name_list)
    #     draw_generic_line_chart(df, SN_IO_PROFILE, 'NFSv3_IOPS', 'NFSv3 IOPS')

    file_name_list = get_file_list_in_dir_by_file_name(
        g_one_storage_data_dir_name, 'nfs3.ops_op.txt')
    df = analyze_nfs_generic_breakdown_file_list(
        file_name_list, NFSV3_OPS_STAT_NAME_DICT)
    print(df)


def get_file_list_in_dir_by_file_name(dir_name, file_name):
    file_name_list = []
    for root, subFolders, files in os.walk(dir_name):
        if file_name in files:
            file_name_list.append(os.path.join(root, file_name))
            with open(os.path.join(root, file_name), 'r') as fin:
                print(os.path.join(root, file_name))

    return file_name_list


def analyze_generic_file_list(file_name_list, column_name_list, interval=3600, skiprows=1, header=None):
    """ column_name_list = ['date_time', 'value', 'max', 'min', 'mean'] """
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
    return df_cat


def analyze_nfs_generic_breakdown_file_list(file_name_list, column_name_dict, interval=3600):
    df_cat = pd.DataFrame(columns=column_name_dict.keys())
    index = 0
    for fn in file_name_list:
        with open(fn, 'r') as fh:
            df = pd.DataFrame(columns=column_name_dict.keys())
            dict_len = len(column_name_dict)
            cur_date_time = ""
            former_date_time = ""
            row_list = []
#             rows_list = []
            skip_first_line = True
            df_index = 0
            fh.readline()
            #for line in fh.read().splitlines():
            for line in fh.readlines():
                #line = line.strip().strip('\n').rstrip('\n')
                #                 if skip_first_line:
                #                     skip_first_line = False
                #                     continue
                #                 line = line.strip().strip('\n')
                #                 print("line_start")
                #                 print(line)
                #                 print("line_end")
                word_list = line.split()
#                 print("word_list_start")
#                 print(word_list)
#                 print("word_list_end")
                cur_one_ops_num = ""
                if is_date(word_list[0]):
                    cur_date_time = word_list[0] + " " + word_list[1]
                    if cur_date_time != former_date_time and len(row_list) != 0:
                        #                         rows_list.append(row_list)
                        df.loc[df_index] = row_list
                        print(row_list)
                        df_index += 1
#                         print("row_list_start")
#                         print(row_list)
#                         print(df)
#                         print("df_end")
#                         break

                    row_list = [0] * dict_len
                    row_list[0] = cur_date_time

                    row_list[1] = int(word_list[2])
                    if word_list[4] == '-':
                        continue
                    ops_index = column_name_dict[word_list[4]]
                    row_list[ops_index] = int(word_list[3])

                else:
                    former_date_time = cur_date_time
                    ops_index = column_name_dict[word_list[1]]
#                     row_list[0] = cur_date_time
                    row_list[ops_index] = int(word_list[0])
            print("final_df_start")
            print(df)
            print("final_df_end")
            for i in range(0, int(len(df) / interval)):
                """ date_time column """
                df_cat.loc[index][0] = df.iloc[i * interval][0]
                for j in range(1, len(column_name_dict)):
                    s = df.iloc[i * interval:(i + 1) * interval][j]
                    df_cat.loc[index][j] = int(s.mean())
                    print("df_cat = " % df_cat)
                    #break
                index += 1
        break

#     df_cat = pd.DataFrame(np.array(rows_list).reshape(
#         len(rows_list), dict_len), columns=columns_dict.keys())
    for key in column_name_dict:
        if key == 'date_time':
            continue
        df_cat[key] = df_cat[key].astype(int)

    return df_cat


def analyze_nfs_generic_breakdown_file_list2(file_name_list, column_name_dict, interval=3600):
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
        df = pd.read_csv(fn, delim_whitespace=True, skiprows=1)
        first_line = df.columns.tolist()
        df.loc[-1] = first_line
        df.index = df.index + 1
        df = df.sort_index()
        df.columns = [1, 2, 3, 4, 5]

        non_na_index_list = df.dropna().index.tolist()
        dict_len = len(column_name_dict)
        df_res = pd.DataFrame(columns=column_name_dict.keys())
        df_index = 0
        for index, elem in enumerate(non_na_index_list):
            next_elem = non_na_index_list[index + 1]
            df = pd.DataFrame(columns=column_name_dict.keys())
            row_list = []
            row_list[0] = df.iloc[elem, 0] + ' ' + df.iloc[elem, 1]
            row_list[1] = df.iloc[elem, 2]
            ops_index = column_name_dict[df.iloc[elem, 4]]
            row_list[ops_index] = column_name_dict[df.iloc[elem, 3]]
            for other_ops_index in range(elem, next_elem):
                ops_index = column_name_dict[df.iloc[other_ops_index, 1]]
                row_list[ops_index] = column_name_dict[df.iloc[other_ops_index, 0]]
            
            df_res.loc[df_index] = row_list
            df_index += 1

        with open(fn, 'r') as fh:
            df = pd.DataFrame(columns=column_name_dict.keys())
            dict_len = len(column_name_dict)
            cur_date_time = ""
            former_date_time = ""
            row_list = []
#             rows_list = []
            skip_first_line = True
            df_index = 0
            fh.readline()
            #for line in fh.read().splitlines():
            for line in fh.readlines():
                #line = line.strip().strip('\n').rstrip('\n')
                #                 if skip_first_line:
                #                     skip_first_line = False
                #                     continue
                #                 line = line.strip().strip('\n')
                #                 print("line_start")
                #                 print(line)
                #                 print("line_end")
                word_list = line.split()
#                 print("word_list_start")
#                 print(word_list)
#                 print("word_list_end")
                cur_one_ops_num = ""
                if is_date(word_list[0]):
                    cur_date_time = word_list[0] + " " + word_list[1]
                    if cur_date_time != former_date_time and len(row_list) != 0:
                        #                         rows_list.append(row_list)
                        df.loc[df_index] = row_list
                        print(row_list)
                        df_index += 1
#                         print("row_list_start")
#                         print(row_list)
#                         print(df)
#                         print("df_end")
#                         break

                    row_list = [0] * dict_len
                    row_list[0] = cur_date_time

                    row_list[1] = int(word_list[2])
                    if word_list[4] == '-':
                        continue
                    ops_index = column_name_dict[word_list[4]]
                    row_list[ops_index] = int(word_list[3])

                else:
                    former_date_time = cur_date_time
                    ops_index = column_name_dict[word_list[1]]
#                     row_list[0] = cur_date_time
                    row_list[ops_index] = int(word_list[0])
            print("final_df_start")
            print(df)
            print("final_df_end")
            for i in range(0, int(len(df) / interval)):
                """ date_time column """
                df_cat.loc[index][0] = df.iloc[i * interval][0]
                for j in range(1, len(column_name_dict)):
                    s = df.iloc[i * interval:(i + 1) * interval][j]
                    df_cat.loc[index][j] = int(s.mean())
                    print("df_cat = " % df_cat)
                    #break
                index += 1
        break

#     df_cat = pd.DataFrame(np.array(rows_list).reshape(
#         len(rows_list), dict_len), columns=columns_dict.keys())
    for key in column_name_dict:
        if key == 'date_time':
            continue
        df_cat[key] = df_cat[key].astype(int)

    return df_cat


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


def init_chart_worksheets():
    workbook = g_excel_writer.book
    for sn in SHEET_NAME_LIST:
        workbook.add_worksheet(sn)


if __name__ == "__main__":
    options, args = parse_opts(sys.argv[1:])

#     g_summary_file_name = options.opcSummaryFile
#     g_excel_file_name = os.path.basename(g_summary_file_name).split('.')[0] + ".xlsx"
#     """ Create a Pandas Excel g_excel_writer using XlsxWriter as the engine """
#     g_excel_writer = pd.ExcelWriter(g_excel_file_name, engine='xlsxwriter')
#     plot_excel_charts()

    g_one_storage_data_dir_name = options.opcDataDir
    g_excel_file_name = os.path.basename(
        g_one_storage_data_dir_name).split('_')[0] + ".xlsx"
    print(g_excel_file_name)
    """ Create a Pandas Excel g_excel_writer using XlsxWriter as the engine """
    g_excel_writer = pd.ExcelWriter(g_excel_file_name, engine='xlsxwriter')
    init_chart_worksheets()
    plot_excel_charts2()
    g_excel_writer.book.close()
