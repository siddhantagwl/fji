#!/usr/bin/env python
# coding: utf-8

import os
import glob
from pathlib import Path
import xlsxwriter
import traceback

import sys
from typing import List, Optional, Tuple
import pandas as pd
import warnings

sys.dont_write_bytecode = True
warnings.filterwarnings("ignore")

curr_path = os.getcwd()
sys.path.append(os.path.join(curr_path, "python_program"))

import config
import utils


class KPIDataProcessor:
    """Main class for processing KPI data from jobsheet exports"""

    def __init__(self):
        self.curr_path = os.getcwd()
        self.parent_path = Path().resolve().parent
        self.python_errors_list = []
        self.python_warnings_list = []

        self.kpi_start_date = None
        self.kpi_end_date = None
        self.kpi_working_days = None
        self.yp_start_date = None
        self.yp_end_date = None
        self.include_archives = False
        self.include_overall = False

        self.df_staff_exp_kpi = None
        self.df_catg_ppj = None
        self.df_output_sheet_names = None
        self.df_user_input = None

        self._setup_file_paths()
        self._load_configuration_data()

    def _setup_file_paths(self):
        """Initialize file paths for errors and warnings"""
        self.input_excel_path = os.path.join(self.curr_path, config.INPUT_UI_FILENAME)
        self.python_errors_filepath = os.path.join(self.curr_path, config.PYTHON_CODES_FOLDER_NAME, config.PYTHON_ERRORS_FILENAME)
        self.python_warnings_filepath = os.path.join(self.curr_path, config.PYTHON_CODES_FOLDER_NAME, config.PYTHON_WARNINGS_FILENAME)
        self.datasheet_folder_path = os.path.join(self.curr_path, config.DATESHEETS_FOLDER_NAME)
        self.archive_kpi_path = os.path.join(self.datasheet_folder_path, config.ARCHIVE_FOLDER_NAME)

        self.output_intermediate_excel_filepath = os.path.join(
            self.curr_path,
            config.PYTHON_CODES_FOLDER_NAME,
            config.INTERMEDIATE_FOLDER_NAME,
            config.INTERMEDIATE_OUTPUT_FILENAME,
        )

        self.datasheet_aggregated_filepath = os.path.join(
            self.curr_path,
            config.PYTHON_CODES_FOLDER_NAME,
            config.INTERMEDIATE_FOLDER_NAME,
            config.AGGREGATED_DATASHEET_OUTPUT_FILENAME,
        )

        self._validate_datasheet_folder()

    def _validate_datasheet_folder(self) -> str:
        """Validate that the datasheet folder exists"""
        if not os.path.exists(self.datasheet_folder_path):
            msg = f"**Critical Error - Unable to find folder which contains exported files from FJI Jobsheet** " f"--> {self.datasheet_folder_path}"
            print(msg, "\n")
            self.python_errors_list.append(msg)
            self.handle_errors()
            raise SystemExit(1)

    def _load_configuration_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all required configuration data from Excel files"""
        print("Loading configuration data...")

        # Read staff expected KPI sheet
        df_staff_exp_kpi, errors = utils.read_excel_file(self.input_excel_path, config.STAFF_EXPECTED_KPI_SHEETNAME)
        self.python_errors_list.extend(errors)
        self.df_staff_exp_kpi = df_staff_exp_kpi

        # Read Category Points per Job (PPJ)
        df_catg_ppj, errors = utils.read_excel_file(self.input_excel_path, config.CATEGORY_PPJ_SHEETNAME)
        self.python_errors_list.extend(errors)
        self.df_catg_ppj = df_catg_ppj

        # Read output sheet names
        df_output_sheets, errors = utils.read_excel_file(self.input_excel_path, sheet_name=config.OUTPUT_SHEETNAMES)
        self.python_errors_list.extend(errors)
        self.df_output_sheet_names = df_output_sheets

        # Read date inputs
        user_input_df, errors = utils.read_excel_file(self.input_excel_path, sheet_name=config.DATE_INPUT_SHEETNAME)
        self.python_errors_list.extend(errors)
        self.df_user_input = user_input_df

    def validate_archives_folder(self) -> bool:
        """Validate that the archives folder exists"""
        if not os.path.exists(self.archive_kpi_path):
            msg = f"'{config.ARCHIVE_FOLDER_NAME}' folder doesn't exist. Please check"
            self.python_warnings_list.append(msg)
            print(msg, "\n")
            return False
        return True

    def cleanup_existing_logs(self):
        """Remove existing error and warning log files"""
        for filepath in [self.python_errors_filepath, self.python_warnings_filepath]:
            if not os.path.exists(filepath):
                continue
            os.remove(filepath)
            print(f"Deleted existing file: {filepath}\n")

    def extract_include_archives_and_overall_options(self) -> None:
        """Extract processing options from user input and set class attributes"""
        # check if 'archives' and 'overall' values are missing, if not set the value else use default
        user_input_df = self.df_user_input.copy()
        include_archives = user_input_df["archives"].iloc[0].lower() if not pd.isna(user_input_df["archives"].iloc[0]) else "n"
        include_overall = user_input_df["overall"].iloc[0].lower() if not pd.isna(user_input_df["overall"].iloc[0]) else "n"

        print(f"Include Archives: {include_archives}")
        print(f"Include Overall: {include_overall}")

        self.include_archives = include_archives == "y"
        self.include_overall = include_overall == "y"

    def load_main_data_files(self) -> Tuple[pd.DataFrame, List[int], List[str]]:
        """Load and process main KPI data files"""
        all_kpi_files = glob.glob(os.path.join(self.datasheet_folder_path, "*.csv"))
        print(f'Found {len(all_kpi_files)} KPI file(s) in "{self.datasheet_folder_path}" ...\n')

        # Filter files
        print("Filtering files like duplicates, conflicted, jobsheets ...")
        remaining_kpi_files, file_warnings = utils.filter_data_files(all_kpi_files)
        print(f"Remained {len(remaining_kpi_files)} KPI file(s)")

        self.python_warnings_list.extend(file_warnings)
        if self.python_warnings_list:
            utils.write_to_file(self.python_warnings_filepath, self.python_warnings_list)

        # Read data files
        print(f"Reading remaining {len(remaining_kpi_files)} files ...")
        df, errors, warnings, df_lens, df_src_filenames = utils.read_data_files(
            remaining_kpi_files,
            date_parser=None,  # lambda x: dt.datetime.strptime(x, "%d-%b-%Y")
            colsExpected=config.COLS_TO_EXPECT_IN_CSV,
        )
        print()
        self.python_errors_list.extend(errors)
        self.python_warnings_list.extend(warnings)

        return df, df_lens, df_src_filenames

    def load_archive_data(self) -> Optional[pd.DataFrame]:
        """Load archive data if requested"""

        print("\nReading Archives ...\n")
        archive_kpi_files = glob.glob(os.path.join(self.archive_kpi_path, "*.csv"))
        print(f"Found {len(archive_kpi_files)} KPI file(s) in Archive ...\n")

        if not archive_kpi_files:
            return None

        df_archive, errors, _, _ = utils.read_data_files(archive_kpi_files, date_parser=None, colsExpected=config.COLS_TO_EXPECT_IN_CSV)
        self.python_errors_list.extend(errors)
        return df_archive

    def validate_columns(self, df: pd.DataFrame):
        """Validate that CSV columns match expectations"""

        actual_cols = df.columns.tolist()
        if set(config.COLS_TO_EXPECT_IN_CSV) == set(actual_cols):
            return  # Columns match, no issues

        msg = "\nColumn names in CSVs did not match from what was expected"

        # Check for extra columns
        extra_cols = list(set(actual_cols) - set(config.COLS_TO_EXPECT_IN_CSV))
        if extra_cols:
            msg += f"\nExtra columns found: {', '.join(extra_cols)}"

        # Check for missing columns
        missing_cols = list(set(config.COLS_TO_EXPECT_IN_CSV) - set(actual_cols))
        if missing_cols:
            msg += f"\nMissing columns: {', '.join(missing_cols)}"
        print(msg)

        self.python_errors_list.extend(msg.split("\n"))

        # TODO: Remove this temporary fix once all files are updated to v1.71
        # Currently ignoring specific column mismatch errors
        self.python_errors_list = [entry for entry in self.python_errors_list if not 'Columns mismatch. Extra: "[]"' in entry]

    def datasheets_preprocessing(self, df: pd.DataFrame, df_lens: List[int]) -> Tuple[pd.DataFrame, List[str]]:
        """
        Preprocess datasheet data with improved error reporting
        Modified by Kamel Mohamed on 19/03/2024 to include filename, row number and value in date columns errors
        """
        import numpy as np

        print("Pre processing Datasheets ...\n")
        errors = []
        error_filenames = set()

        # Convert columns to lowercase
        for col in config.COLS_TO_LOWER_CASE:
            try:
                df[col] = df[col].fillna("").str.lower()
            except Exception as e:
                msg = f'Unable to convert to lower case / fillna. Column: "{col}"'
                errors.append(msg)

        # Convert numerical columns to integer
        for col in config.INTEGER_COLS:
            try:
                df[col] = pd.to_numeric(df[col]).fillna(0)
            except Exception as e:
                msg = f'Unable to convert to Number type or fillna. Column: "{col}"'
                errors.append(msg)

        # Convert date columns with detailed error reporting
        for col in config.DATE_COLS:
            try:
                converted = pd.to_datetime(df[col], infer_datetime_format=True, dayfirst=True, errors="coerce")
                invalid_rows: pd.DataFrame = df[converted.isna() & df[col].notna()]
                if not invalid_rows.empty:
                    for idx in invalid_rows.index:
                        error_row_num = self.get_local_row(idx, df_lens)
                        filename = df.loc[idx, config.COL_PROJECT_NAME]
                        val = df.loc[idx, col]
                        msg = f'File: "{filename}" ||| Col: "{col}" ||| Row Num: "{error_row_num + 1}" ||| Row value: "{val}". Error: Invalid date'
                        errors.append(msg)
                        print(msg)
                        error_filenames.add(filename)
                df[col] = converted
            except Exception as e:
                errors.append(f"General failure parsing column: {col}. Error: {str(e)}")

        # ❌ Drop all rows with those filenames
        if error_filenames:
            df = df[~df[config.COL_PROJECT_NAME].isin(error_filenames)].reset_index(drop=True)

        # Convert string columns
        for col in config.STRING_COLS:
            try:
                df[col] = df[col].fillna("").astype(str)
            except Exception as e:
                msg = f'Unable to convert to String type or fillna. Column: "{col}"'
                errors.append(msg)

        # Reset index
        try:
            df = df.reset_index(drop=True)
        except Exception as e:
            msg = "Unable to reset index."
            errors.append(msg)

        try:
            df["extracted_project_date"] = df[config.COL_PROJECT_NAME].str.extract(r"^(20\d{2}\.\d{2}\.\d{2})")
            df["extracted_project_date"] = pd.to_datetime(df["extracted_project_date"], format="%Y.%m.%d", errors="coerce")
        except Exception as e:
            msg = "Unable to extract project date from project name."
            errors.append(msg)

        if errors:
            print("\nSome errors occurred during pre-processing:\n" + "\n".join(errors))

        return df, errors

    def get_local_row(self, row_id: int, df_lengths: List[int]) -> int:
        """Given a global row ID across multiple stacked DataFrames,
        return the local row index within its corresponding DataFrame.
        """

        total_rows_so_far = 0
        for current_df_len in df_lengths:
            total_rows_so_far += current_df_len
            if row_id < total_rows_so_far:
                local_row = row_id - (total_rows_so_far - current_df_len)
                return local_row

    def handle_errors(self):
        """Handle errors by writing to file and potentially exiting"""
        if self.python_errors_list:
            utils.write_to_file(self.python_errors_filepath, self.python_errors_list)

    def prepare_staff_data(self) -> Tuple[List[str], dict]:
        """Prepare staff data including active staff list and categories"""
        df_staff_exp_kpi = self.df_staff_exp_kpi.copy()

        df_staff_exp_kpi = df_staff_exp_kpi.fillna("")
        df_staff_exp_kpi["Names"] = df_staff_exp_kpi["Names"].apply(lambda x: x.lower())

        active_staff = df_staff_exp_kpi.loc[df_staff_exp_kpi["Active"] == "Y", "Names"].values.tolist()

        staff_and_their_categories = {}
        for indx, row in df_staff_exp_kpi.iterrows():
            all_catg = set([row["Category 1"], row["Category 2"], row["Category 3"], row["Category 4"]])
            all_catg = [c for c in all_catg if c]
            staff_and_their_categories[row["Names"]] = all_catg

        return active_staff, staff_and_their_categories

    def prepare_ppj_data(self) -> Tuple[List[str], dict]:
        """Prepare Points Per Job (PPJ) data"""
        df_catg_ppj = self.df_catg_ppj.copy()

        df_catg_ppj = df_catg_ppj.fillna("")
        sub_catg_list = df_catg_ppj["Sub category"].str.lower().unique().tolist()

        ppj_dict = {}
        for n, d in df_catg_ppj.groupby("Category"):
            ppj_dict[n] = dict(zip(d["Sub category"], d["Points per job (PPJ)"]))

        return sub_catg_list, ppj_dict

    def set_date_ranges(self) -> dict:
        """Extract and validate date ranges from user input"""
        df_user_input = self.df_user_input.copy()
        df_user_input["Start Date"] = pd.to_datetime(df_user_input["Start Date"])
        df_user_input["End Date"] = pd.to_datetime(df_user_input["End Date"])

        # Main date range
        user_start_date_obj = df_user_input.iloc[0, 0]
        user_end_date_obj = self.df_user_input.iloc[0, 1]
        working_days = len(pd.bdate_range(user_start_date_obj, user_end_date_obj))

        print(f"Start Date: {user_start_date_obj}\nEnd Date: {user_end_date_obj}")
        print(f"# of working days: {working_days}\n")

        # Yearly performance date range
        user_start_date_yp_obj = df_user_input.iloc[1, 0]
        user_end_date_yp_obj = df_user_input.iloc[1, 1]

        print(f"Yearly Performance rating:\n\nStart Date: {user_start_date_yp_obj}\nEnd Date: {user_end_date_yp_obj}")

        self.kpi_start_date = user_start_date_obj
        self.kpi_end_date = user_end_date_obj
        self.kpi_working_days = working_days
        self.yp_start_date = user_start_date_yp_obj
        self.yp_end_date = user_end_date_yp_obj

        return {
            "start_date": user_start_date_obj,
            "end_date": user_end_date_obj,
            "working_days": working_days,
            "yp_start_date": user_start_date_yp_obj,
            "yp_end_date": user_end_date_yp_obj,
        }

    def file_tag_and_jobsheet_version_analysis(self, df: pd.DataFrame):
        """Analyze file tags and jobsheet versions in the DataFrame"""
        temp_df = df.copy()
        # temp_df[config.COL_FILETAG] = temp_df[config.COL_FILETAG].fillna('')
        # temp_df[config.COL_JOBSHEET_FILEVERSION] = temp_df[config.COL_JOBSHEET_FILEVERSION].fillna('')

        # file_tags = temp_df[config.COL_FILETAG].unique().tolist()
        # jobsheet_versions = temp_df[config.COL_JOBSHEET_FILEVERSION].unique().tolist()

        # Drop duplicate rows based on these columns
        subset = [config.COL_PROJECT_NAME, config.COL_FILETAG, config.COL_JOBSHEET_FILEVERSION]
        temp_df = temp_df[subset]
        df_cleaned = temp_df.drop_duplicates(subset=subset)
        df_cleaned = df_cleaned[subset].reset_index(drop=True)

        # print(f'File Tags: {file_tags}')
        # print(f'Jobsheet Versions: {jobsheet_versions}')

        return df_cleaned

    def generate_summary_reports(self, df: pd.DataFrame) -> dict:
        """Generate all summary reports for photographers, photostackers, and retouchers"""
        from summary_photographers import (
            summary_of_photographers_all_projects,
            summary_of_photographers_project_wise,
        )
        from summary_photostackers import (
            summary_of_photostackers_all_projects,
            summary_of_photostackers_project_wise,
            summary_of_photostackers_by_month,
        )
        from summary_retouchers import (
            summary_of_retouchers_all_projects,
            summary_of_retouchers_project_wise,
            summary_of_retouchers_by_month,
        )
        from summary_photography import summary_of_photography_project_wise

        # Generate summaries for all roles
        print("Generating summary reports for photographers, photostackers, and retouchers ...\n")

        # --------- Photographers summary ---------
        df_photographers = summary_of_photographers_all_projects(df.copy(), self.include_overall, self.kpi_start_date, self.kpi_end_date)
        df_photographers_project_wise = summary_of_photographers_project_wise(df.copy(), self.kpi_start_date, self.kpi_end_date)

        # --------- Photostackers summary ---------
        df_photostackers = summary_of_photostackers_all_projects(df.copy(), self.include_overall, self.kpi_start_date, self.kpi_end_date)
        df_photostackers_project_wise = summary_of_photostackers_project_wise(df.copy(), self.kpi_start_date, self.kpi_end_date)

        # --------- Retouchers summary ---------
        df_retouchers = summary_of_retouchers_all_projects(df.copy(), self.include_overall, self.kpi_start_date, self.kpi_end_date)
        df_retouchers_project_wise = summary_of_retouchers_project_wise(df.copy(), self.kpi_start_date, self.kpi_end_date)

        # --------- Photography summary ---------
        # Photography summary = How many items and images on each Photographer date
        df_photography_summary_project_wise: pd.DataFrame = summary_of_photography_project_wise(
            df.copy(), self.include_overall, self.kpi_start_date, self.kpi_end_date
        )
        df_photography_summary = (
            df_photography_summary_project_wise.groupby("Photography_date")
            .agg({"Items": "sum", "Images": "sum", "Project_name": "count"})
            .reset_index()
            .rename(columns={"Project_name": "#_projects_done"})
        )
        df_photography_summary["Breakdown"] = "View"
        df_photography_summary.sort_values(by="Photography_date", ascending=False, inplace=True)

        return {
            "photographers": df_photographers,
            "photographers_project_wise": df_photographers_project_wise,
            "photostackers": df_photostackers,
            "photostackers_project_wise": df_photostackers_project_wise,
            "retouchers": df_retouchers,
            "retouchers_project_wise": df_retouchers_project_wise,
            "photography_summary": df_photography_summary,
            "photography_summary_project_wise": df_photography_summary_project_wise,
        }

    def calculate_monthly_data(self, summary_data: dict, df: pd.DataFrame) -> dict:
        """Calculate monthly data for photographers, photostackers, and retouchers"""

        from summary_photographers import summary_of_photographers_by_month
        from summary_photostackers import summary_of_photostackers_by_month
        from summary_retouchers import summary_of_retouchers_by_month

        print("Calculating monthly data for photographers, photostackers, and retouchers ...\n")
        df_photographers_by_month_items = summary_of_photographers_by_month(df.copy())
        df_photostackers_by_month_rename, df_photostackers_by_month_adjust, df_photostackers_by_month_photostack = summary_of_photostackers_by_month(df.copy())
        df_retouchers_by_month_transfer, df_retouchers_by_month_retouched, df_retouchers_by_month_variance = summary_of_retouchers_by_month(df.copy())

        summary_data["monthly_data"] = {
            "photographers_items": df_photographers_by_month_items,
            "photostackers_rename": df_photostackers_by_month_rename,
            "photostackers_adjust": df_photostackers_by_month_adjust,
            "photostackers_photostack": df_photostackers_by_month_photostack,
            "retouchers_transfer": df_retouchers_by_month_transfer,
            "retouchers_retouched": df_retouchers_by_month_retouched,
            "retouchers_variance": df_retouchers_by_month_variance,
        }

        return summary_data

    def calculate_kpis(self, summary_data: dict, ppj_dict: dict, sub_catg_list: List[str]) -> dict:
        """Calculate KPIs from PPJ data"""
        summary_data["photographers_project_wise"] = utils.calc_KPI_from_PPJ(summary_data["photographers_project_wise"], ppj_dict, sub_catg_list)
        summary_data["photostackers_project_wise"] = utils.calc_KPI_from_PPJ(summary_data["photostackers_project_wise"], ppj_dict, sub_catg_list)
        summary_data["retouchers_project_wise"] = utils.calc_KPI_from_PPJ(summary_data["retouchers_project_wise"], ppj_dict, sub_catg_list)
        return summary_data

    def add_breakdown_columns(self, summary_data: dict) -> dict:
        """Add breakdown and current date range columns"""
        role_pairs = [
            ("photographers", "photographers_project_wise"),
            ("photostackers", "photostackers_project_wise"),
            ("retouchers", "retouchers_project_wise"),
        ]

        for summary_key, project_wise_key in role_pairs:
            temp_df = summary_data[summary_key]
            temp_df_proj_wise = summary_data[project_wise_key]

            temp_df["Breakdown"] = "View"
            temp_df["Current date range"] = "-"

            for indx, _ in temp_df.iterrows():
                if indx in temp_df_proj_wise.index:
                    temp_df.loc[indx, "Current date range"] = "Available"

        return summary_data

    def filter_for_active_staff(self, summary_data: dict, active_staff: List[str]) -> dict:
        """Filter data for active staff only"""
        print("Filtering summary data for active staff ...\n")
        # Filter index for active staff
        for key in ["photographers", "photostackers", "retouchers"]:
            summary_data[key] = utils.filter_index_for_active_staff(summary_data[key], active_staff)

            # Filter column values for active staff
            project_wise_key = f"{key}_project_wise"
            summary_data[project_wise_key] = utils.filter_column_values_for_active_staff(summary_data[project_wise_key], active_staff)

        # Filter monthly data for active staff
        for key in summary_data["monthly_data"].keys():
            summary_data["monthly_data"][key] = utils.filter_index_for_active_staff(summary_data["monthly_data"][key], active_staff)

        return summary_data

    def calculate_yearly_performance(
        self,
        df: pd.DataFrame,
        staff_and_their_categories: dict,
        active_staff: List[str],
        ppj_dict: dict,
        sub_catg_list: List[str],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate yearly performance ratings"""
        import yearly_performance_points

        print("Calculating yearly KPI performance ratings ...\n")
        df_staff_exp_kpi_modf = yearly_performance_points.modify_date_cols_to_month_year(self.df_staff_exp_kpi)

        user_end_date_yp_obj = yearly_performance_points.calc_month_end_date(self.yp_end_date)
        user_date_list = yearly_performance_points.date_list(self.yp_start_date, user_end_date_yp_obj)

        yearly_summary_data = yearly_performance_points.calculate_yearly_summary_tables(df, user_date_list, ppj_dict, sub_catg_list)

        df_yearly_performance, expected_kpi_set = yearly_performance_points.calculate_yearly_performance_table(
            yearly_summary_data, staff_and_their_categories, df_staff_exp_kpi_modf, active_staff
        )

        df_points_chart = yearly_performance_points.make_points_reference_chart(expected_kpi_set)

        return df_yearly_performance, df_points_chart

    def create_excel_output(
        self,
        df: pd.DataFrame,
        summary_data: dict,
        df_yearly_performance: pd.DataFrame,
        df_points_chart: pd.DataFrame,
        df_file_tag_and_jobsheet_version: pd.DataFrame = None,  # Made optional
    ) -> None:
        """Create the final Excel output file"""

        print("Creating Excel output file ...\n")
        # Save aggregated datasheet
        df.to_excel(self.datasheet_aggregated_filepath, index=False)

        # Setup Excel writer
        overall_summary_sheet = self.df_output_sheet_names.iloc[0, 0]

        if os.path.exists(self.output_intermediate_excel_filepath):
            os.remove(self.output_intermediate_excel_filepath)

        writer = pd.ExcelWriter(self.output_intermediate_excel_filepath, engine="xlsxwriter")
        workbook = writer.book
        worksheet = workbook.add_worksheet(overall_summary_sheet)
        writer.sheets[overall_summary_sheet] = worksheet

        # Write overall summary information
        self._write_overall_summary_header(worksheet, df)

        # Write main summary tables
        df_list = [summary_data["photographers"], summary_data["photostackers"], summary_data["retouchers"]]
        df_table_rows_map = self._write_main_summary_tables(writer, df_list, overall_summary_sheet)

        # Write detailed sheets
        self._write_detailed_sheets(writer, workbook, summary_data, df)

        # Write table rows map
        df_table_rows_map.to_excel(writer, sheet_name="table_rows_map", index=False)

        # Write yearly performance sheet
        self._write_yearly_performance_sheet(writer, workbook, df_yearly_performance, df_points_chart)

        # Write job sheet version analysis if provided
        if df_file_tag_and_jobsheet_version is not None:
            df_file_tag_and_jobsheet_version.to_excel(writer, sheet_name="filetag_jobsheet_ver_analys", index=False)

        df_month_wise_list = [
            summary_data["monthly_data"]["photographers_items"],
            summary_data["monthly_data"]["photostackers_rename"],
            summary_data["monthly_data"]["photostackers_adjust"],
            summary_data["monthly_data"]["photostackers_photostack"],
            summary_data["monthly_data"]["retouchers_transfer"],
            summary_data["monthly_data"]["retouchers_retouched"],
            summary_data["monthly_data"]["retouchers_variance"],
        ]
        df_table_rows_map_month_wise = self._write_month_wise_tables(writer, workbook, df_month_wise_list, self.df_output_sheet_names.iloc[-1, 0])
        # Write table rows map
        df_table_rows_map_month_wise.to_excel(writer, sheet_name="table_rows_map_month_wise", index=False)

        writer.close()

    def _write_overall_summary_header(self, worksheet, df: pd.DataFrame):
        """Write the summary header information"""
        import datetime as dt

        if self.include_overall:
            overall_kpi_start_date = min(df[config.COL_PHOTOGRAPHER_DATE])
            overall_kpi_start_date_str = ""
            try:
                overall_kpi_start_date_str = utils.convert_date_obj_to_str(overall_kpi_start_date)
            except:
                pass

            today_date_obj = dt.datetime.today().date()
            start_date_to_write = overall_kpi_start_date_str
            end_date_to_write = utils.convert_date_obj_to_str(today_date_obj)
            _working_days = len(pd.bdate_range(overall_kpi_start_date_str, today_date_obj))
        else:
            start_date_to_write = utils.convert_date_obj_to_str(self.kpi_start_date)
            end_date_to_write = utils.convert_date_obj_to_str(self.kpi_end_date)
            _working_days = self.kpi_working_days

        # Write the dates and working days to the worksheet
        worksheet.write(0, 0, config.OVERALL_SUMMARY_SHEETNAME)
        worksheet.write(1, 0, "Start Date:")
        worksheet.write(1, 1, start_date_to_write)

        worksheet.write(2, 0, "End Date:")
        worksheet.write(2, 1, end_date_to_write)

        worksheet.write(3, 0, "Working Days")
        worksheet.write(3, 1, _working_days)

    def _write_main_summary_tables(self, writer, df_list: List[pd.DataFrame], summary_sheet: str) -> pd.DataFrame:
        """Write main summary tables and return table rows mapping"""
        df_table_rows_map = pd.DataFrame(columns=["table_name", "start", "end"])
        r = config.START_ROW_EXCEL_OUTPUT

        for temp_df in df_list:
            df_table_rows_map.loc[len(df_table_rows_map)] = [temp_df.index.name, r + 1, r + 1 + len(temp_df)]
            r += len(temp_df) + 2

        utils.multiple_dfs_on_same_sheet(writer, df_list, summary_sheet, spaces=1, row=config.START_ROW_EXCEL_OUTPUT)

        return df_table_rows_map

    def _write_month_wise_tables(self, writer, workbook, df_list: list[pd.DataFrame], sheetName: str):
        """Write month-wise tables"""
        # index name: category name
        category_map = {
            "Items": "Photographers",
            "Rename": "Photostackers",
            "Adjust": "Photostackers",
            "Photostack": "Photostackers",
            "Transfer": "Retouchers",
            "Retouches": "Retouchers",
            "Variance": "Retouchers",
        }

        worksheet = workbook.add_worksheet(sheetName)
        writer.sheets[sheetName] = worksheet

        worksheet.write(0, 0, "Month Wise Summary for all Photographers, Photostackers and Retouchers")

        start_row = config.START_ROW_EXCEL_OUTPUT
        start_col = config.START_COL_EXCEL_OUTPUT

        df_table_rows_map = pd.DataFrame(columns=["table_name", "start", "end", "category"])

        for df in df_list:
            # Write headers
            categ_name = df.index.name
            print(f"Writing month-wise table for category: {categ_name}")
            category = category_map[categ_name]
            worksheet.write(start_row, start_col, category)
            row = start_row + 1
            df.to_excel(
                writer,
                sheet_name=sheetName,
                startrow=row,
                startcol=start_col,
                index=True,
            )
            df_table_rows_map.loc[len(df_table_rows_map)] = [df.index.name, row + 1, row + 1 + len(df), category]
            start_row += len(df) + 1 + 3  # +2 for header and +1 for empty row

        return df_table_rows_map

    def _write_detailed_sheets(
        self,
        writer,
        workbook: "xlsxwriter.Workbook",
        summary_data: dict,
        df: pd.DataFrame,
    ):
        """Write detailed sheets for each data type"""
        import numbers
        import datetime as dt

        unnamed_sheets = 0
        overall_kpi_start_date = min(df[config.COL_PHOTOGRAPHER_DATE])
        overall_kpi_start_date_str = ""
        try:
            overall_kpi_start_date_str = utils.convert_date_obj_to_str(overall_kpi_start_date)
        except:
            print("Error converting overall KPI start date to string. Check date format.")

        df_output_sheets = self.df_output_sheet_names.copy()
        df_dict = {
            df_output_sheets.iloc[1, 0]: summary_data["photography_summary"],
            df_output_sheets.iloc[2, 0]: summary_data["photography_summary_project_wise"],
            df_output_sheets.iloc[3, 0]: summary_data["photographers_project_wise"],
            df_output_sheets.iloc[4, 0]: summary_data["photostackers_project_wise"],
            df_output_sheets.iloc[5, 0]: summary_data["retouchers_project_wise"],
        }

        for sheetname, temp_df in df_dict.items():
            temp_df: pd.DataFrame
            # Remove start and end date columns
            cols = [col for col in temp_df.columns if col not in ["start_date", "end_date", "extracted_project_date"]]
            temp_df = temp_df[cols]

            # Handle unnamed sheets
            if isinstance(sheetname, numbers.Number):
                sheetname = f"Unnamed sheet {unnamed_sheets + 1}"
                print(f"Unnamed sheet : {sheetname}")
                unnamed_sheets += 1

            worksheet = workbook.add_worksheet(sheetname)
            writer.sheets[sheetname] = worksheet

            # Write sheet headers
            if sheetname in [df_output_sheets.iloc[1, 0], df_output_sheets.iloc[2, 0]]:
                index = False
                # worksheet.write(0, 0, "Overall Photoshoots from start to present")
                # worksheet.write(1, 0, "Start Date:")
                # worksheet.write(1, 1, overall_kpi_start_date_str)
            else:
                index = True

            if self.include_overall:
                start_date_to_write = overall_kpi_start_date_str
                today_date_obj = dt.datetime.today().date()
                end_date_to_write = utils.convert_date_obj_to_str(today_date_obj)
                _working_days = len(pd.bdate_range(overall_kpi_start_date_str, today_date_obj))
            else:
                start_date_to_write = utils.convert_date_obj_to_str(self.kpi_start_date)
                end_date_to_write = utils.convert_date_obj_to_str(self.kpi_end_date)
                _working_days = self.kpi_working_days

            worksheet.write(0, 0, "Start Date")
            worksheet.write(0, 1, start_date_to_write)

            worksheet.write(1, 0, "End Date")
            worksheet.write(1, 1, end_date_to_write)

            worksheet.write(2, 0, "Working Days")
            worksheet.write(2, 1, _working_days)

            # Write data
            temp_df.to_excel(
                writer,
                sheet_name=sheetname,
                startrow=config.START_ROW_EXCEL_OUTPUT,
                startcol=config.START_COL_EXCEL_OUTPUT,
                index=index,
            )

    def _write_yearly_performance_sheet(
        self,
        writer,
        workbook,
        df_yearly_performance: pd.DataFrame,
        df_points_chart: pd.DataFrame,
    ):
        """Write the yearly performance sheet"""
        import numbers
        from yearly_performance_points import write_yearly_performance_sheet

        sheetname = self.df_output_sheet_names.iloc[6, 0]
        unnamed_sheets = 0

        if isinstance(sheetname, numbers.Number):
            sheetname = f"Unnamed sheet {unnamed_sheets + 1}"
            print(f"Unnamed sheet : {sheetname}")
            unnamed_sheets += 1

        write_yearly_performance_sheet(
            writer,
            workbook,
            sheetname,
            df_yearly_performance,
            df_points_chart,
            self.yp_start_date,
            self.yp_end_date,
        )

    def process_kpi_data(self):
        """Main processing pipeline with complete workflow"""
        import time

        start_time = time.time()

        try:
            # Setup
            self.cleanup_existing_logs()

            # Check for initial errors
            if self.python_errors_list:
                self.handle_errors()
                raise SystemExit(1)

            # Get processing options
            self.set_date_ranges()
            self.extract_include_archives_and_overall_options()

            # Load main data
            df, df_lens, df_src_filenames = self.load_main_data_files()

            # Load archive data if needed
            if self.validate_archives_folder() and self.include_archives:
                df_archive = self.load_archive_data()
                if df_archive is not None:
                    df = pd.concat([df, df_archive])

            # Validate columns
            self.validate_columns(df)

            # Final error check
            if self.python_errors_list:
                self.handle_errors()
                # raise SystemExit(1)

            if self.python_warnings_list:
                utils.write_to_file(self.python_warnings_filepath, self.python_warnings_list)

            # Preprocess data
            df, preprocessing_errors = self.datasheets_preprocessing(df, df_lens)
            self.python_errors_list.extend(preprocessing_errors)

            if self.python_errors_list:
                self.handle_errors()
                # raise SystemExit(1)

            # Prepare supporting data
            active_staff, staff_and_their_categories = self.prepare_staff_data()
            sub_catg_list, ppj_dict = self.prepare_ppj_data()

            # ---------------------------------------------------------------------------
            # Generate summary reports
            summary_data = self.generate_summary_reports(df)

            # --------------------------------------------------------------------------
            # Calculate monthly data
            summary_data = self.calculate_monthly_data(summary_data, df)

            # Calculate KPIs for each breakdown
            summary_data = self.calculate_kpis(summary_data, ppj_dict, sub_catg_list)

            # Add breakdown columns
            summary_data = self.add_breakdown_columns(summary_data)

            # Filter for active staff
            summary_data = self.filter_for_active_staff(summary_data, active_staff)

            # Calculate yearly performance
            df_yearly_performance, df_points_chart = self.calculate_yearly_performance(
                df, staff_and_their_categories, active_staff, ppj_dict, sub_catg_list
            )

            # Create file tag and job sheet version analysis
            # df_file_tag_and_jobsheet_version = self.file_tag_and_jobsheet_version_analysis(df)

            # Create Excel output
            self.create_excel_output(df, summary_data, df_yearly_performance, df_points_chart)

            end_time = round((time.time() - start_time))
            print(f"Success ... Time taken: {end_time} seconds")

            return {
                "dataframe": df,
                "summary_data": summary_data,
                "excel_filepath": self.output_intermediate_excel_filepath,
                "processing_time": end_time,
            }

        except SystemExit:
            raise
        except Exception as e:
            error_msg = f"Unexpected error during processing: {str(e)}"
            line_info = traceback.format_exc()
            print(error_msg)
            print(line_info)
            self.python_errors_list.append(f"{error_msg}\nTraceback:\n{line_info}")
            self.handle_errors()
            raise SystemExit(1)


def main():
    """Main entry point"""
    print("KPI Aggregation Script Started ...\n")
    processor = KPIDataProcessor()
    result = processor.process_kpi_data()

    if result:
        print(f"Processed {len(result['dataframe'])} records successfully")
        print(f"Excel output saved to: {result['excel_filepath']}")
        print(f"Total processing time: {result['processing_time']} seconds")


if __name__ == "__main__":
    main()
