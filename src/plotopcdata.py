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
import datetime
import pandas as pd
import subprocess
# import xlsxwriter
#import numpy as np
#import multiprocessing

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
DBG_LEVEL4 = 4
g_nfs_breakdown_second_results = []

""" Tunables """
g_debug = True
g_debug_level = DBG_LEVEL3

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

def init_chart_worksheets():
    workbook = g_excel_writer.book
    for sn in SHEET_NAME_LIST:
        workbook.add_worksheet(sn)

def read_df_from_csv_with_col_list(fn_csv, column_name_list):
    df = pd.read_csv(fn_csv, header=None, names=column_name_list)
    return df

def read_df_from_csv_with_col_dict(fn_csv, column_name_dict):
    return read_df_from_csv_with_col_list(fn_csv, column_name_dict.keys())

def write_df_to_csv(fn_csv, df):
    with open(fn_csv, 'w') as f:
        df.to_csv(f, header=True)

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def call_shell(script, dir, fn):
    script_file_path = get_script_path() + '/' + script
    subprocess.call([script_file_path, dir, fn])

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
        {'name': chart_y_axis, 'name_font': {'size': 14, 'bold': True}})

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

def plot_nfs3_ops(fn):
    column_name_list = ['date_time', 'iops_mean', 'max', 'min']
    script = 'generic_mean_max_min.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_list(fn_csv, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NFSv3_IOPS', 'Hourly NFSv3 IOPS')
    
def plot_nfs3_ops_op(fn):
    column_name_dict = NFSV3_OPS_STAT_NAME_DICT
    script = 'nfsv3_ops_op_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv3_BREAKDOWN_BY_OPS',
                                'Hourly NFSv3 IOPS Breakdown by Operation')

def plot_nfs3_ops_size(fn):
    col_list = ['date_time', 'total_iops', 'io_size <1k', 'io_size >=1k', 'io_size >=2k', 'io_size >=4k',
            'io_size >=8k', 'io_size >=16k', 'io_size >=32k', 'io_size >=64k', 'io_size >=128k', 'io_size >=256k', 'io_size >=512k']
    column_name_dict = dict(zip(col_list, range(0, len(col_list))))

    script = 'nfsv3_ops_size_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)
    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv3_IOPS_BREAKDOWN_BS',
                                'Hourly NFSv3 IOPS Breakdown by Block Size', 'NFSv3 IOPS')

def plot_nfs3_ops_latency(fn):
    col_list = ['date_time', 'total_iops', 'latency <10μs', 'latency >=10μs', 'latency >=100μs', 'latency >=1ms',
                'latency >=10ms', 'latency >=100ms', 'latency >=1s', 'latency >=10s']
    column_name_dict = dict(zip(col_list, range(0, len(col_list))))

    script = 'nfsv3_ops_latency_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)
    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv3_BREAKDOWN_LATENCY',
                            'Hourly NFSv3 OPS Breakdown by Latency', 'IOPS')

def plot_nfs4_ops(fn):
    column_name_list = ['date_time', 'iops_mean', 'max', 'min']
    script = 'generic_mean_max_min.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_list(fn_csv, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NFSv4_IOPS', 'Hourly NFSv4 IOPS')
    
def plot_nfs4_ops_op(fn):
    column_name_dict = NFSV4_OPS_STAT_NAME_DICT
    script = 'nfsv4_ops_op_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv4_BREAKDOWN_BY_OPS',
                                'Hourly NFSv4 IOPS Breakdown by Operation')
    
def plot_nfs4_ops_size(fn):
    col_list = ['date_time', 'total_iops', 'io_size <1k', 'io_size >=1k', 'io_size >=2k', 'io_size >=4k',
            'io_size >=8k', 'io_size >=16k', 'io_size >=32k', 'io_size >=64k', 'io_size >=128k', 'io_size >=256k', 'io_size >=512k']
    column_name_dict = dict(zip(col_list, range(0, len(col_list))))

    script = 'nfsv3_ops_size_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)
    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv4_IOPS_BREAKDOWN_BS',
                                'Hourly NFSv4 IOPS Breakdown by Block Size', 'NFSv4 IOPS')

def plot_nfs4_ops_latency(fn):
    col_list = ['date_time', 'total_iops', 'latency <10μs', 'latency >=10μs', 'latency >=100μs', 'latency >=1ms',
                'latency >=10ms', 'latency >=100ms', 'latency >=1s', 'latency >=10s']
    column_name_dict = dict(zip(col_list, range(0, len(col_list))))

    script = 'nfsv3_ops_latency_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)
    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'NFSv4_BREAKDOWN_LATENCY',
                            'Hourly NFSv4 OPS Breakdown by Latency', 'IOPS')

def plot_io_ops_op(fn):
    column_name_dict = DISK_IO_OPS_RW_STAT_NAME_DICT
    script = 'io_ops_op_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'DISK_IOPS_BREAKDOWN_RW',
                                'Hourly Disk IOPS Breakdown by Read/Write', 'DISK IOPS')
    
def plot_io_bytes(fn):
    column_name_dict = DISK_IO_KB_PER_SEC_RW_STAT_NAME_DICT
    script = 'io_ops_op_bd.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_dict(fn_csv, column_name_dict)
    draw_generic_combined_chart(df, SN_IO_PROFILE, 'DISK_KB_PER_SEC_BREAKDOWN_RW',
                                'Hourly Disk KB/SEC Breakdown by Read/Write', 'DISK KB/SEC')

def plot_nic_kilobytes(fn):
    column_name_list = ['date_time', 'nic_kb_per_sec_mean', 'max', 'min']
    script = 'generic_mean_max_min.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_list(fn_csv, column_name_list)
    draw_generic_line_chart(
        df, SN_IO_PROFILE, 'NIC_KB_PER_SEC', 'Hourly NIC KB/SEC', 'NIC KB/Sec')
    
def plot_arc_hitratio(fn):
    column_name_list = ['date_time', 'arc_hit_ratio_mean', 'max', 'min']
    script = 'generic_mean_max_min.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_list(fn_csv, column_name_list)
    draw_generic_line_chart(df, SN_OS_LOAD, 'ZFS_ARC_HIT_RATIO',
                            'Hourly ZFS ARC HIT RATIO', 'ARC Hit Ratio')

def plot_cpu_utilization(fn):
    column_name_list = ['date_time', 'cpu_util_mean', 'max', 'min']
    script = 'generic_mean_max_min.sh'
    fn_csv = g_one_storage_data_dir_name + '/' + fn + '.csv'
    if not os.path.isfile(fn_csv):
        call_shell(script, g_one_storage_data_dir_name, fn)

    df = read_df_from_csv_with_col_list(fn_csv, column_name_list)
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
    all_start_time = datetime.datetime.now().replace(microsecond=0)
    for i, fn in enumerate(fname_2_func):
        print("=== Step {} ===: {}".format(i + 1, fn))
        start_time = datetime.datetime.now().replace(microsecond=0)
        func = fname_2_func[fn]
        func(fn)
        end_time = datetime.datetime.now().replace(microsecond=0)
        print("Time used: {}".format(end_time - start_time))
    all_end_time = datetime.datetime.now().replace(microsecond=0)
    print("Finished - All Time Used: {}".format(all_end_time - all_start_time))

if __name__ == "__main__":
    options, args = parse_opts(sys.argv[1:])

    g_one_storage_data_dir_name = options.opcDataDir
    g_excel_file_name = os.path.basename(
        g_one_storage_data_dir_name).split('_')[0] + ".xlsx"
    """ Create a Pandas Excel g_excel_writer using XlsxWriter as the engine """
    g_excel_writer = pd.ExcelWriter(g_excel_file_name, engine='xlsxwriter')
    init_chart_worksheets()
    plot_excel_all_charts()
    g_excel_writer.book.close()
