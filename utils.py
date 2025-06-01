import config
from pathlib import Path
import pandas as pd
import datetime as dt
from calendar import monthrange
import os
import re


import traceback

def handle_errors(default_return=None, log_traceback=True):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_traceback:
                    print(f"[ERROR] in '{func.__name__}': {e}")
                    traceback.print_exc()
                return default_return
        return wrapper
    return decorator


def get_month_days(year, month):
    return monthrange(year, month)


def get_empty_df(cols):
    return pd.DataFrame(columns=cols)


def construct_pd_datetime_obj(year, month, day):
    return pd.to_datetime(dt.date(year, month, day))


def construct_list_of_dates(st_date_obj, end_date_obj, freq):
    return pd.date_range(start = st_date_obj, end = end_date_obj, freq=freq)


def concat_dfs(df_list):
    return pd.concat(df_list)


def convert_df_col_to_numeric(series):
    return pd.to_numeric(series)


def convert_df_col_to_date(series):
    return pd.to_datetime(series).dt.date


def remove_empty_str_values(arr):
    return [a for a in arr if a]


def convert_date_str_to_obj(date_string):
    date_obj = dt.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S').date()
    return date_obj


def convert_date_obj_to_str(date_obj, date_format='%Y-%m-%d'):
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
    return df[(df[retoucher_col_name] == r_name) & (df['Reject Retouchers Pay'] != 'Y')]


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
        print(msg, '\n')
        errors.append(msg)
    return df, errors


def read_csv_file(csv_filepath, dayfirst=False, date_parser=None):
    msg = ''
    df = pd.DataFrame()
    try:
        df = pd.read_csv(csv_filepath, dayfirst=dayfirst, date_parser=date_parser)
    except Exception as e:
        msg = f'Unreadable csv "{os.path.basename(csv_filepath)}"'
        print(msg, '\n')
    return df, msg

############################################################
#
# Modified by Kamel Mohamed on 19/03/2024
# to include filename, row number and value in the error reporting
#
############################################################

def read_data_files(file_path_list, dayfirst=False, date_parser=None, colsExpected=None):
    df = pd.DataFrame()
    errors = []
    df_lens = []
    df_src_filenames = []

    for file in file_path_list:

        temp_df, err = read_csv_file(file, dayfirst, date_parser)
        df_lens.append(len(temp_df))
        df_src_filenames.append(os.path.basename(file))
        if err:
            errors.append(err)
            continue

        csvCols = temp_df.columns.tolist()

        csvCols_mapped = []
        is_col_name_changed = False
        for col in csvCols:
            if col in config.BACKWARD_COLUMN_COMPATIBILITY:
                csvCols_mapped.append(config.BACKWARD_COLUMN_COMPATIBILITY[col])
                is_col_name_changed = True
            else:
                csvCols_mapped.append(col)

        if not (colsExpected == csvCols_mapped):
            extra_cols_in_csv = list(set(csvCols_mapped) - set(colsExpected))
            missing_cols_in_csv = list(set(colsExpected) - set(csvCols_mapped))
            msg = f'Check "{os.path.basename(file)}" --> Columns mismatch. Extra: "{extra_cols_in_csv}". Missing: "{missing_cols_in_csv}". jobsheet version: {df.jobsheet_fileversion.iloc[0]}'
            errors.append(msg)

        if is_col_name_changed:
            temp_df.rename(columns=config.BACKWARD_COLUMN_COMPATIBILITY, inplace=True)

        df = pd.concat([df, temp_df], ignore_index=True)

    return df, errors, df_lens, df_src_filenames
    # return pd.concat((pd.read_csv(f, dayfirst=dayfirst, date_parser=date_parser) for f in file_path_list), ignore_index=True)


def multiple_dfs_on_same_sheet(writer, df_list, sheet_name, spaces, row, index=True):
    for dataframe in df_list:
        dataframe.to_excel(writer, sheet_name=sheet_name, startrow=row , startcol=0, index=index)
        row = row + len(dataframe.index) + spaces + 1
    return


def calc_KPI_from_PPJ(df, ppj_dict, sub_catg_list):

    df['KPI Points'] = 0

    df_catg_name = df.index.name

    if df_catg_name not in ppj_dict:
        df['KPI Points'] = '-'
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
            kpi_sum += (val * ppj)

        df.loc[indx, 'KPI Points'] = kpi_sum

    return df


def filter_index_for_active_staff(df, active_staff):
    temp_df = df.filter(items = active_staff, axis=0)
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

    msg = ';\n'.join(msg_list_2)
    with open(filepath, 'w') as f:
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

        filename = os.path.basename(file_path)

        # Rule 1: Skip conflicted files
        if ("conflicted" in filename):
            warnings.append(f"{filename}|||Skipped|||Conflicted")
            remaining_files.discard(file_path)
            continue

        # Rule 2: Skip JOBSHEET_* or JPLA_JOBSHEET_*
        if (filename.startswith('JOBSHEET_') or filename.startswith("JPLA_JOBSHEET_")):
            tag = "JPLA_JOBSHEET_*" if filename.startswith("JPLA") else "JOBSHEET_*"
            warnings.append(f"{filename}|||Skipped|||{tag}")
            remaining_files.discard(file_path)
            continue


    # Rule 3: Handle duplicates like 'xyz 5 Items.xlsx' vs 'xyz.xlsx'
    remaining_files = remove_duplicate_files(list(remaining_files), warnings)

    return remaining_files, warnings


def remove_duplicate_files(file_list, warnings):
    remaining = set(file_list)

    for base_file in file_list:
        base_stem = Path(base_file).stem

        for other_file in file_list:
            if base_file == other_file:
                continue

            other_name = os.path.basename(other_file)
            # Match pattern like "BaseName 3 Items.xlsx"
            if re.match(rf'^{re.escape(base_stem)} \d+ Items\.', other_name):
                # Keep the newer file
                if os.path.getmtime(base_file) > os.path.getmtime(other_file):
                    to_keep, to_remove = base_file, other_file
                else:
                    to_keep, to_remove = other_file, base_file

                warnings.append(f"{os.path.basename(to_keep)}|||Scanned|||Duplicate")
                warnings.append(f"{os.path.basename(to_remove)}|||Skipped|||Duplicate")
                remaining.discard(to_remove)

    return list(remaining)


