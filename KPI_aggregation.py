#!/usr/bin/env python
# coding: utf-8

import os
import glob
from pathlib import Path
import datetime as dt

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
        self.setup_file_paths()

    def setup_file_paths(self):
        """Initialize file paths for errors and warnings"""
        self.python_errors_filepath = os.path.join(
            self.curr_path, config.PYTHON_CODES_FOLDER_NAME, config.PYTHON_ERRORS_FILENAME
        )
        self.python_warnings_filepath = os.path.join(
            self.curr_path, config.PYTHON_CODES_FOLDER_NAME, config.PYTHON_WARNINGS_FILENAME
        )
        self.datasheet_folder_path = os.path.join(self.curr_path, config.DATESHEETS_FOLDER_NAME)
        self.validate_datasheet_folder()

    def validate_datasheet_folder(self) -> str:
        """Validate that the datasheet folder exists"""
        if not os.path.exists(self.datasheet_folder_path):
            msg = (
                f"**Critical Error - Unable to find folder which contains exported files from FJI Jobsheet** "
                f"--> {self.datasheet_folder_path}"
            )
            print(msg, "\n")
            self.python_errors_list.append(msg)
            self.handle_errors()
            raise SystemExit(1)

    def cleanup_existing_logs(self):
        """Remove existing error and warning log files"""
        for filepath in [self.python_errors_filepath, self.python_warnings_filepath]:
            if not os.path.exists(filepath):
                continue
            os.remove(filepath)
            print(f"Deleted existing file: {filepath}\n")

    def load_configuration_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all required configuration data from Excel files"""
        print("Loading configuration data...")

        input_excel_path = os.path.join(self.curr_path, config.INPUT_UI_FILENAME)

        # Read staff expected KPI sheet
        df_staff_exp_kpi, errors = utils.read_excel_file(input_excel_path, config.STAFF_EXPECTED_KPI_SHEETNAME)
        self.python_errors_list.extend(errors)

        # Read Category Points per Job (PPJ)
        df_catg_ppj, errors = utils.read_excel_file(input_excel_path, config.CATEGORY_PPJ_SHEETNAME)
        self.python_errors_list.extend(errors)

        # Read output sheet names
        df_output_sheets, errors = utils.read_excel_file(input_excel_path, sheet_name=config.OUTPUT_SHEETNAMES)
        self.python_errors_list.extend(errors)

        # Read date inputs
        user_input_df, errors = utils.read_excel_file(input_excel_path, sheet_name=config.DATE_INPUT_SHEETNAME)
        self.python_errors_list.extend(errors)

        return df_staff_exp_kpi, df_catg_ppj, df_output_sheets, user_input_df

    def get_processing_options(self, user_input_df: pd.DataFrame) -> Tuple[str, str]:
        """Extract processing options from user input"""
        include_archives = user_input_df["archives"].iloc[0].lower() if not pd.isna(user_input_df["archives"].iloc[0]) else "n"
        include_overall = user_input_df["overall"].iloc[0].lower() if not pd.isna(user_input_df["overall"].iloc[0]) else "n"

        print(f"Include Archives: {include_archives}")
        print(f"Include Overall: {include_overall}")

        return include_archives, include_overall

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

    def load_archive_data(self, include_archives: str) -> Optional[pd.DataFrame]:
        """Load archive data if requested"""
        if include_archives != "y":
            return None

        print("\nReading Archives ...\n")
        archive_kpi_path = os.path.join(self.datasheet_folder_path, config.ARCHIVE_FOLDER_NAME)

        if not os.path.exists(archive_kpi_path):
            msg = f"'{config.ARCHIVE_FOLDER_NAME}' folder doesn't exist. Please check"
            self.python_errors_list.append(msg)
            print(msg, "\n")
            return None

        archive_kpi_files = glob.glob(os.path.join(archive_kpi_path, "*.csv"))
        print(f"Found {len(archive_kpi_files)} KPI file(s) in Archive ...\n")

        if not archive_kpi_files:
            return None

        df_archive, errors, _, _ = utils.read_data_files(
            archive_kpi_files, date_parser=None, colsExpected=config.COLS_TO_EXPECT_IN_CSV
        )
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

        if errors:
            print("\nSome errors occurred during pre-processing:\n" + "\n".join(errors))

        return df, errors

    # def get_filename_and_row(self, row_id: int, df_lens: List[int],
    #                         df_src_filenames: List[str]) -> Tuple[str, int]:
    #     """Get filename and row number for error reporting"""

    #     len_sum = 0
    #     for df_len, filename in zip(df_lens, df_src_filenames):
    #         len_sum = len_sum + df_len
    #         if row_id < len_sum:
    #             return filename, row_id - (len_sum - df_len)

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

    def prepare_staff_data(self, df_staff_exp_kpi: pd.DataFrame) -> Tuple[List[str], dict]:
        """Prepare staff data including active staff list and categories"""
        df_staff_exp_kpi = df_staff_exp_kpi.fillna("")
        df_staff_exp_kpi["Names"] = df_staff_exp_kpi["Names"].apply(lambda x: x.lower())

        active_staff = df_staff_exp_kpi.loc[df_staff_exp_kpi["Active"] == "Y", "Names"].values.tolist()

        staff_and_their_categories = {}
        for indx, row in df_staff_exp_kpi.iterrows():
            all_catg = set([row["Category 1"], row["Category 2"], row["Category 3"], row["Category 4"]])
            all_catg = [c for c in all_catg if c]
            staff_and_their_categories[row["Names"]] = all_catg

        return active_staff, staff_and_their_categories

    def prepare_ppj_data(self, df_catg_ppj: pd.DataFrame) -> Tuple[List[str], dict]:
        """Prepare Points Per Job (PPJ) data"""
        df_catg_ppj = df_catg_ppj.fillna("")
        sub_catg_list = df_catg_ppj["Sub category"].str.lower().unique().tolist()

        ppj_dict = {}
        for n, d in df_catg_ppj.groupby("Category"):
            ppj_dict[n] = dict(zip(d["Sub category"], d["Points per job (PPJ)"]))

        return sub_catg_list, ppj_dict

    def extract_date_ranges(self, user_input_df: pd.DataFrame) -> dict:
        """Extract and validate date ranges from user input"""
        user_input_df["Start Date"] = pd.to_datetime(user_input_df["Start Date"])
        user_input_df["End Date"] = pd.to_datetime(user_input_df["End Date"])

        # Main date range
        user_start_date_obj = user_input_df.iloc[0, 0]
        user_end_date_obj = user_input_df.iloc[0, 1]
        working_days = len(pd.bdate_range(user_start_date_obj, user_end_date_obj))

        print(f"Start Date: {user_start_date_obj}\nEnd Date: {user_end_date_obj}")
        print(f"# of working days: {working_days}\n")

        # Yearly performance date range
        user_start_date_yp_obj = user_input_df.iloc[1, 0]
        user_end_date_yp_obj = user_input_df.iloc[1, 1]

        print(f"Yearly Performance rating:\n\nStart Date: {user_start_date_yp_obj}\nEnd Date: {user_end_date_yp_obj}")

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

    def generate_summary_reports(self, df: pd.DataFrame, include_overall: str, date_ranges: dict) -> dict:
        """Generate all summary reports for photographers, photostackers, and retouchers"""
        from summary_photographers import (
            summary_of_photographers_all_projects,
            summary_of_photographers_project_wise,
            summary_of_photographers_by_month,
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
        df_photographers = summary_of_photographers_all_projects(
            df.copy(), include_overall, date_ranges["start_date"], date_ranges["end_date"]
        )
        df_photographers_project_wise = summary_of_photographers_project_wise(
            df.copy(), date_ranges["start_date"], date_ranges["end_date"]
        )
        df_photographers_by_month_items = summary_of_photographers_by_month(df.copy())

        df_photostackers = summary_of_photostackers_all_projects(
            df.copy(), include_overall, date_ranges["start_date"], date_ranges["end_date"]
        )
        df_photostackers_project_wise = summary_of_photostackers_project_wise(
            df.copy(), date_ranges["start_date"], date_ranges["end_date"]
        )
        df_photostackers_by_month_rename, df_photostackers_by_month_adjust, df_photostackers_by_month_photostack = (
            summary_of_photostackers_by_month(df.copy())
        )

        df_retouchers = summary_of_retouchers_all_projects(
            df.copy(), include_overall, date_ranges["start_date"], date_ranges["end_date"]
        )
        df_retouchers_project_wise = summary_of_retouchers_project_wise(
            df.copy(), date_ranges["start_date"], date_ranges["end_date"]
        )
        df_retouchers_by_month_transfer, df_retouchers_by_month_retouched, df_retouchers_by_month_variance = (
            summary_of_retouchers_by_month(df.copy())
        )

        # Photography summary = How many items and images on each Photographer date
        df_photography_summary_project_wise = summary_of_photography_project_wise(df.copy())
        df_photography_summary = (
            df_photography_summary_project_wise.groupby("Photography_date")
            .agg({"Items": "sum", "Images": "sum", "Project_name": "count"})
            .reset_index()
            .rename(columns={"Project_name": "#_projects_done"})
        )
        df_photography_summary["Breakdown"] = "View"

        return {
            "photographers": df_photographers,
            "photographers_project_wise": df_photographers_project_wise,
            "photostackers": df_photostackers,
            "photostackers_project_wise": df_photostackers_project_wise,
            "retouchers": df_retouchers,
            "retouchers_project_wise": df_retouchers_project_wise,
            "photography_summary": df_photography_summary,
            "photography_summary_project_wise": df_photography_summary_project_wise,
            "monthly_data": {
                "photographers_items": df_photographers_by_month_items,
                "photostackers_rename": df_photostackers_by_month_rename,
                "photostackers_adjust": df_photostackers_by_month_adjust,
                "photostackers_photostack": df_photostackers_by_month_photostack,
                "retouchers_transfer": df_retouchers_by_month_transfer,
                "retouchers_retouched": df_retouchers_by_month_retouched,
                "retouchers_variance": df_retouchers_by_month_variance,
            },
        }

    def calculate_kpis(self, summary_data: dict, ppj_dict: dict, sub_catg_list: List[str]) -> dict:
        """Calculate KPIs from PPJ data"""
        summary_data["photographers_project_wise"] = utils.calc_KPI_from_PPJ(
            summary_data["photographers_project_wise"], ppj_dict, sub_catg_list
        )
        summary_data["photostackers_project_wise"] = utils.calc_KPI_from_PPJ(
            summary_data["photostackers_project_wise"], ppj_dict, sub_catg_list
        )
        summary_data["retouchers_project_wise"] = utils.calc_KPI_from_PPJ(
            summary_data["retouchers_project_wise"], ppj_dict, sub_catg_list
        )
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
        # Filter index for active staff
        summary_data["photographers"] = utils.filter_index_for_active_staff(summary_data["photographers"], active_staff)
        summary_data["photostackers"] = utils.filter_index_for_active_staff(summary_data["photostackers"], active_staff)
        summary_data["retouchers"] = utils.filter_index_for_active_staff(summary_data["retouchers"], active_staff)

        # Filter column values for active staff
        summary_data["photographers_project_wise"] = utils.filter_column_values_for_active_staff(
            summary_data["photographers_project_wise"], active_staff
        )
        summary_data["photostackers_project_wise"] = utils.filter_column_values_for_active_staff(
            summary_data["photostackers_project_wise"], active_staff
        )
        summary_data["retouchers_project_wise"] = utils.filter_column_values_for_active_staff(
            summary_data["retouchers_project_wise"], active_staff
        )

        return summary_data

    def calculate_yearly_performance(
        self,
        df: pd.DataFrame,
        df_staff_exp_kpi: pd.DataFrame,
        date_ranges: dict,
        staff_and_their_categories: dict,
        active_staff: List[str],
        ppj_dict: dict,
        sub_catg_list: List[str],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate yearly performance ratings"""
        import yearly_performance_points

        df_staff_exp_kpi_modf = yearly_performance_points.modify_date_cols_to_month_year(df_staff_exp_kpi)

        user_end_date_yp_obj = yearly_performance_points.calc_month_end_date(date_ranges["yp_end_date"])
        user_date_list = yearly_performance_points.date_list(date_ranges["yp_start_date"], user_end_date_yp_obj)

        yearly_summary_data = yearly_performance_points.calculate_yearly_summary_tables(
            df, user_date_list, ppj_dict, sub_catg_list
        )

        df_yearly_performance, expected_kpi_set = yearly_performance_points.calculate_yearly_performance_table(
            yearly_summary_data, staff_and_their_categories, df_staff_exp_kpi_modf, active_staff
        )

        df_points_chart = yearly_performance_points.make_points_reference_chart(expected_kpi_set)

        return df_yearly_performance, df_points_chart

    def create_excel_output(
        self,
        df: pd.DataFrame,
        summary_data: dict,
        df_output_sheets: pd.DataFrame,
        date_ranges: dict,
        include_overall: str,
        df_yearly_performance: pd.DataFrame,
        df_points_chart: pd.DataFrame,
        df_file_tag_and_jobsheet_version: pd.DataFrame = None,  # Made optional
    ):
        """Create the final Excel output file"""
        import datetime as dt
        import numbers

        # Save aggregated datasheet
        datasheet_aggregated_filepath = os.path.join(
            self.curr_path,
            config.PYTHON_CODES_FOLDER_NAME,
            config.INTERMEDIATE_FOLDER_NAME,
            config.AGGREGATED_DATASHEET_OUTPUT_FILENAME,
        )
        df.to_excel(datasheet_aggregated_filepath, index=False)

        # Setup Excel writer
        summary_sheet = df_output_sheets.iloc[0, 0]
        excel_filepath = os.path.join(
            self.curr_path,
            config.PYTHON_CODES_FOLDER_NAME,
            config.INTERMEDIATE_FOLDER_NAME,
            config.INTERMEDIATE_OUTPUT_FILENAME,
        )

        if os.path.exists(excel_filepath):
            os.remove(excel_filepath)

        writer = pd.ExcelWriter(excel_filepath, engine="xlsxwriter")
        workbook = writer.book
        worksheet = workbook.add_worksheet(summary_sheet)
        writer.sheets[summary_sheet] = worksheet

        # Write overall summary information
        self._write_summary_header(worksheet, df, date_ranges, include_overall)

        # Write main summary tables
        df_list = [summary_data["photographers"], summary_data["photostackers"], summary_data["retouchers"]]
        df_table_rows_map = self._write_main_tables(writer, df_list, summary_sheet)

        # Write detailed sheets
        self._write_detailed_sheets(writer, workbook, summary_data, df_output_sheets, date_ranges, df)

        # Write table rows map
        df_table_rows_map.to_excel(writer, sheet_name="table_rows_map", index=False)

        # Write yearly performance sheet
        self._write_yearly_performance_sheet(
            writer, workbook, df_output_sheets, df_yearly_performance, df_points_chart, date_ranges
        )

        # Write job sheet version analysis if provided
        if df_file_tag_and_jobsheet_version is not None:
            df_file_tag_and_jobsheet_version.to_excel(writer, sheet_name="filetag_jobsheet_ver_analys", index=False)

        writer.close()
        return excel_filepath

    def _write_summary_header(self, worksheet, df: pd.DataFrame, date_ranges: dict, include_overall: str):
        """Write the summary header information"""
        import datetime as dt

        worksheet.write(0, 0, config.OVERALL_SUMMARY_SHEETNAME)
        worksheet.write(1, 0, "Start Date:")
        worksheet.write(2, 0, "End Date:")
        worksheet.write(3, 0, "Working Days")

        if include_overall == "y":
            overall_kpi_start_date = min(df[config.COL_PHOTOGRAPHER_DATE])
            overall_kpi_start_date_str = ""
            try:
                overall_kpi_start_date_str = utils.convert_date_obj_to_str(overall_kpi_start_date)
            except:
                pass
                # if python_errors_list:
                #     utils.write_to_file(python_errors_filepath, python_errors_list)

            today_date_obj = dt.datetime.today().date()
            worksheet.write(1, 1, overall_kpi_start_date_str)
            worksheet.write(2, 1, utils.convert_date_obj_to_str(today_date_obj))

            _working_days = len(pd.bdate_range(overall_kpi_start_date_str, today_date_obj))
            worksheet.write(3, 1, _working_days)
        else:
            worksheet.write(1, 1, utils.convert_date_obj_to_str(date_ranges["start_date"]))
            worksheet.write(2, 1, utils.convert_date_obj_to_str(date_ranges["end_date"]))
            worksheet.write(3, 1, date_ranges["working_days"])

    def _write_main_tables(self, writer, df_list: List[pd.DataFrame], summary_sheet: str) -> pd.DataFrame:
        """Write main summary tables and return table rows mapping"""
        df_table_rows_map = pd.DataFrame(columns=["table_name", "start", "end"])
        r = config.START_ROW_EXCEL_OUTPUT

        for temp_df in df_list:
            df_table_rows_map.loc[len(df_table_rows_map)] = [temp_df.index.name, r + 1, r + 1 + len(temp_df)]
            r += len(temp_df) + 2

        utils.multiple_dfs_on_same_sheet(writer, df_list, summary_sheet, spaces=1, row=config.START_ROW_EXCEL_OUTPUT)

        return df_table_rows_map

    def _write_detailed_sheets(
        self, writer, workbook, summary_data: dict, df_output_sheets: pd.DataFrame, date_ranges: dict, df: pd.DataFrame
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

        df_dict = {
            df_output_sheets.iloc[1, 0]: summary_data["photography_summary"],
            df_output_sheets.iloc[2, 0]: summary_data["photography_summary_project_wise"],
            df_output_sheets.iloc[3, 0]: summary_data["photographers_project_wise"],
            df_output_sheets.iloc[4, 0]: summary_data["photostackers_project_wise"],
            df_output_sheets.iloc[5, 0]: summary_data["retouchers_project_wise"],
        }

        for sheetname, temp_df in df_dict.items():
            # Remove start and end date columns
            cols = [col for col in temp_df.columns if col not in ["start_date", "end_date"]]
            temp_df = temp_df[cols]

            # Handle unnamed sheets
            if isinstance(sheetname, numbers.Number):
                sheetname = f"Unnamed sheet {unnamed_sheets + 1}"
                print(f"Unnamed sheet : {sheetname}")
                unnamed_sheets += 1

            worksheet2 = workbook.add_worksheet(sheetname)
            writer.sheets[sheetname] = worksheet2

            # Write sheet headers
            if sheetname in [df_output_sheets.iloc[1, 0], df_output_sheets.iloc[2, 0]]:
                index = False
                worksheet2.write(0, 0, "Overall Photoshoots from start to present")
                worksheet2.write(1, 0, "Start Date:")
                worksheet2.write(1, 1, overall_kpi_start_date_str)
            else:
                index = True
                worksheet2.write(0, 0, "Start Date")
                worksheet2.write(1, 0, "End Date")
                worksheet2.write(2, 0, "Working Days")
                worksheet2.write(0, 1, utils.convert_date_obj_to_str(date_ranges["start_date"]))
                worksheet2.write(1, 1, utils.convert_date_obj_to_str(date_ranges["end_date"]))
                worksheet2.write(2, 1, date_ranges["working_days"])

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
        df_output_sheets: pd.DataFrame,
        df_yearly_performance: pd.DataFrame,
        df_points_chart: pd.DataFrame,
        date_ranges: dict,
    ):
        """Write the yearly performance sheet"""
        import numbers
        from yearly_performance_points import write_yearly_performance_sheet

        sheetname = df_output_sheets.iloc[6, 0]
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
            date_ranges["yp_start_date"],
            date_ranges["yp_end_date"],
        )

    def process_kpi_data(self):
        """Main processing pipeline with complete workflow"""
        import time

        try:
            # Setup
            self.cleanup_existing_logs()

            # Load configuration
            df_staff_exp_kpi, df_catg_ppj, df_output_sheets, user_input_df = self.load_configuration_data()

            # Check for initial errors
            if self.python_errors_list:
                self.handle_errors()
                raise SystemExit(1)

            # Get processing options
            include_archives, include_overall = self.get_processing_options(user_input_df)

            # Load main data
            df, df_lens, df_src_filenames = self.load_main_data_files()

            # Load archive data if needed
            df_archive = self.load_archive_data(include_archives)
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
            active_staff, staff_and_their_categories = self.prepare_staff_data(df_staff_exp_kpi)
            sub_catg_list, ppj_dict = self.prepare_ppj_data(df_catg_ppj)
            date_ranges = self.extract_date_ranges(user_input_df)

            # Generate summary reports
            print("Generating summary reports...")
            start_time = time.time()
            summary_data = self.generate_summary_reports(df, include_overall, date_ranges)

            # Calculate KPIs
            summary_data = self.calculate_kpis(summary_data, ppj_dict, sub_catg_list)

            # Add breakdown columns
            summary_data = self.add_breakdown_columns(summary_data)

            # Filter for active staff
            summary_data = self.filter_for_active_staff(summary_data, active_staff)

            # Calculate yearly performance
            df_yearly_performance, df_points_chart = self.calculate_yearly_performance(
                df, df_staff_exp_kpi, date_ranges, staff_and_their_categories, active_staff, ppj_dict, sub_catg_list
            )

            # Create file tag and job sheet version analysis
            # df_file_tag_and_jobsheet_version = self.file_tag_and_jobsheet_version_analysis(df)

            # Create Excel output
            excel_filepath = self.create_excel_output(
                df,
                summary_data,
                df_output_sheets,
                date_ranges,
                include_overall,
                df_yearly_performance,
                df_points_chart,
                # df_file_tag_and_jobsheet_version,
            )

            end_time = round((time.time() - start_time))
            print(f"Success ... Time taken: {end_time} seconds")

            return {
                "dataframe": df,
                "summary_data": summary_data,
                "excel_filepath": excel_filepath,
                "processing_time": end_time,
            }

        except SystemExit:
            raise
        except Exception as e:
            error_msg = f"Unexpected error during processing: {str(e)}"
            print(error_msg)
            self.python_errors_list.append(error_msg)
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
