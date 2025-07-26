import datetime as dt
import os
import re
import traceback
from calendar import monthrange
from pathlib import Path

import pandas as pd

import config


def catch_errors(func):
    """
    A decorator to catch and log errors in the decorated function.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error in function '{func.__name__}': {e}")

    return wrapper


def get_month_days(year, month):
    return monthrange(year, month)


def get_empty_df(cols):
    return pd.DataFrame(columns=cols)


def construct_pd_datetime_obj(year, month, day):
    return pd.to_datetime(dt.date(year, month, day))


def construct_list_of_dates(st_date_obj, end_date_obj, freq):
    return pd.date_range(start=st_date_obj, end=end_date_obj, freq=freq)


def concat_dfs(df_list):
    return pd.concat(df_list)


def convert_df_col_to_numeric(series):
    return pd.to_numeric(series)


def convert_df_col_to_date(series):
    return pd.to_datetime(series).dt.date


def remove_empty_str_values(arr):
    return [a for a in arr if a]


def convert_date_str_to_obj(date_string):
    date_obj = dt.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S").date()
    return date_obj


def convert_date_obj_to_str(date_obj, date_format="%Y-%m-%d"):
    return dt.datetime.strftime(date_obj, date_format)


def get_df_from_dict(dict_data):
    return pd.DataFrame(dict_data)


def is_start_before_end(start_date_string, end_date_string):
    # Make sure that the end date is not before the start date
    # input date string: 2021-08-01 13:09:43
    start_date_obj = convert_date_str_to_obj(start_date_string)
    end_date_obj = convert_date_str_to_obj(end_date_string)
    return start_date_obj <= end_date_obj


def filter_df_on_dates(df, start_date, end_date, col_name):
    return df[(df[col_name] >= start_date) & (df[col_name] <= end_date)]


def filter_df_on_retoucher_name(df, r_name, retoucher_col_name):
    return df[(df[retoucher_col_name] == r_name) & (df["Reject Retouchers Pay"] != "Y")]


def filter_df_on_column_value(df, col_name, col_value):
    return df[df[col_name] == col_value]


def sum_df_on_a_column(df, col_name):
    return df[col_name].sum()


def df_column_to_uniques_list(df, col_name):
    return df[col_name].unique().tolist()


def read_excel_file(excel_filepath, sheet_name=None):
    df = pd.DataFrame()
    errors = []

    try:
        df = pd.read_excel(excel_filepath, sheet_name=sheet_name)
    except Exception as e:
        msg = f'Unable to read data from excel "{excel_filepath}" --> "{e}"'
        print(msg, "\n")
        errors.append(msg)
    return df, errors


def read_csv_file(csv_filepath, dayfirst=False, date_parser=None):
    msg = ""
    df = pd.DataFrame()
    try:
        df = pd.read_csv(csv_filepath, dayfirst=dayfirst, date_parser=date_parser)
    except Exception as e:
        msg = f"Unreadable csv '{os.path.basename(csv_filepath)}'|||Error->{e}"
        print(msg)
    return df, msg


import logging
from concurrent.futures import ThreadPoolExecutor
from time import time
from typing import Callable, List, Optional, Tuple


def read_data_files(
    file_path_list,
    dayfirst=False,
    date_parser=None,
    colsExpected: list = [],
    use_threads: bool = True,
    show_progress: bool = True,
):

    errors: list[str] = []
    df_lens: list[int] = []
    df_src_filenames: list[str] = []
    warnings: list[str] = []
    file_path_list = sorted(file_path_list)
    df_list = []

    def read_and_process(file):
        filename = os.path.basename(file)
        df, warn = read_csv_file(file, dayfirst, date_parser)
        return filename, df, warn

    reader = ThreadPoolExecutor().map if use_threads else map
    jobs = reader(read_and_process, file_path_list)

    if show_progress:
        from tqdm import tqdm

        jobs = tqdm(jobs, total=len(file_path_list), desc="Reading CSV files")

    for i, (filename, temp_df, warn) in enumerate(jobs):
        print(f"{i}. Reading: {filename}")
        if warn:
            warnings.append(warn)
            continue

        original_cols = temp_df.columns.tolist()

        # Apply backward mapping if needed
        mapped_cols = [config.BACKWARD_COLUMN_COMPATIBILITY.get(col, col) for col in original_cols]

        # If mapping changed, rename the columns in the DataFrame
        if mapped_cols != original_cols:
            temp_df.rename(columns=config.BACKWARD_COLUMN_COMPATIBILITY, inplace=True)

        # Check if mapped columns match expected
        if colsExpected and mapped_cols != colsExpected:
            extra = list(set(mapped_cols) - set(colsExpected))
            missing = list(set(colsExpected) - set(mapped_cols))
            version = temp_df.get("jobsheet_fileversion", pd.Series(["?"])).iloc[0]
            warnings.append(
                f'Check "{filename}" --> Columns mismatch. Extra: "{extra}". Missing: "{missing}". jobsheet version: {version}'
            )

        df_list.append(temp_df)
        df_lens.append(len(temp_df))
        df_src_filenames.append(filename)

    final_df = pd.concat(df_list, ignore_index=True)

    return final_df, errors, warnings, df_lens, df_src_filenames


def multiple_dfs_on_same_sheet(writer, df_list, sheet_name, spaces, row, index=True):
    for dataframe in df_list:
        dataframe.to_excel(writer, sheet_name=sheet_name, startrow=row, startcol=0, index=index)
        row = row + len(dataframe.index) + spaces + 1
    return


def calc_KPI_from_PPJ(df, ppj_dict, sub_catg_list):

    df["KPI Points"] = 0

    df_catg_name = df.index.name

    if df_catg_name not in ppj_dict:
        df["KPI Points"] = "-"
        return df

    for indx, row in df.iterrows():

        kpi_sum = 0
        for c in df.columns:
            # if c in ['start_date', 'end_date', 'Project_name',
            #         '#_projects_worked', 'KPI Points', 'Images']:
            # omit the columns and continue if they are not invloved in calcultion of KPI
            if c.lower() not in sub_catg_list:
                continue

            ppj = ppj_dict[df_catg_name][c]
            val = df.loc[indx, c]
            kpi_sum += val * ppj

        df.loc[indx, "KPI Points"] = kpi_sum

    return df


def filter_index_for_active_staff(df, active_staff):
    temp_df = df.filter(items=active_staff, axis=0)
    return temp_df


def filter_column_values_for_active_staff(df, active_staff):
    col_name = df.index.name
    temp_df = df.reset_index()
    temp_df = temp_df[temp_df[col_name].isin(active_staff)].set_index(col_name)
    return temp_df


def write_to_file(filepath, msg_list):
    seen = set()
    msg_list_2 = [x for x in msg_list if not (x in seen)]
    msg_list_2 = [x for x in msg_list_2 if x]

    msg = ";\n".join(msg_list_2)
    with open(filepath, "w") as f:
        f.write(msg)


def get_filename_and_row(row, df_lens, df_src_filenames):
    len_sum = 0
    for df_len, filename in zip(df_lens, df_src_filenames):
        len_sum = len_sum + df_len
        if row < len_sum:
            return filename, row - (len_sum - df_len)


# @handle_errors(default_return="Something went wrong!")
def filter_data_files(all_kpi_files):
    warnings = []
    remaining_files = set(all_kpi_files)

    for file_path in all_kpi_files:

        filename: str = os.path.basename(file_path).lower()

        # Rule 1: Skip conflicted files
        if "conflicted" in filename.lower():
            warnings.append(f"{filename}|||Skipped|||Conflicted")
            remaining_files.discard(file_path)
            continue

        # Rule 2: Skip JOBSHEET_* or JPLA_JOBSHEET_*
        if filename.startswith("jobsheet_") or filename.startswith("jpla_jobsheet"):
            tag = "JPLA_JOBSHEET_*" if filename.startswith("jpla") else "JOBSHEET_*"
            warnings.append(f"{filename}|||Skipped|||{tag}")
            remaining_files.discard(file_path)
            continue

    # Rule 3: Handle duplicates like 'xyz 5 Items.xlsx' vs 'xyz.xlsx'
    remaining_files = remove_duplicate_files(remaining_files, warnings)
    return remaining_files, warnings


def remove_duplicate_files(remaining_files: set, warnings: list) -> list:
    remaining_file_list = list(remaining_files)

    for base_file in remaining_file_list:
        base_stem = Path(base_file).stem

        for other_file in remaining_file_list:
            if base_file == other_file:
                continue

            other_name = os.path.basename(other_file)
            # Match pattern like "BaseName 3 Items.xlsx"
            if re.match(rf"^{re.escape(base_stem)} \d+ Items\.", other_name):
                # Keep the newer file
                if os.path.getmtime(base_file) > os.path.getmtime(other_file):
                    to_keep, to_remove = base_file, other_file
                else:
                    to_keep, to_remove = other_file, base_file

                warnings.append(f"{os.path.basename(to_keep)}|||Scanned|||Duplicate")
                warnings.append(f"{os.path.basename(to_remove)}|||Skipped|||Duplicate")
                remaining_files.discard(to_remove)

    return list(remaining_files)
