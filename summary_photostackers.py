from pandas import DataFrame
import pandas as pd

import config
import utils


def get_all_photostackers_names(df):

    p1_sign_names = utils.df_column_to_uniques_list(df, config.COL_PHOTOSTACKER_SIGN_1)
    p2_sign_names = utils.df_column_to_uniques_list(df, config.COL_PHOTOSTACKER_SIGN_2)
    # p2_sign_names = []

    all_photostacker_names = p1_sign_names + p2_sign_names

    # Normalize: strip leading/trailing whitespace
    all_photostacker_names = [name.strip().lower() for name in all_photostacker_names if isinstance(name, str)]

    all_photostacker_names = list(dict.fromkeys(all_photostacker_names))

    # photostacker_names = utils.df_column_to_uniques_list(df, config.COL_PHOTOSTACKER_SIGN)
    all_photostacker_names = utils.remove_empty_str_values(all_photostacker_names)
    return all_photostacker_names


def summary_of_photostackers_all_projects(df: DataFrame, include_overall, start_date, end_date):

    if include_overall == "n":
        # don't include the overall dates, rather use the start and end date provided by
        # user in the KP_invoivce cells
        temp_df_date1_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_1)
        temp_df_date2_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_2)
        # temp_df_date2_filtered = pd.DataFrame()

        df = utils.concat_dfs([temp_df_date1_filtered, temp_df_date2_filtered])
        df.reset_index(drop=True, inplace=True)

    # make an empty dataframe with these columns
    cols = ["Photostacker", "Rename", "Adjust", "Photostack", "#_projects_worked"]
    df_photostacker = utils.get_empty_df(cols)

    all_photostacker_names = get_all_photostackers_names(df)

    for p_name in sorted(all_photostacker_names):

        if (p_name == "") or (p_name == config.REDUNDANT_VALUE.lower()) or (p_name in config.UNMERGE_START_CONST_VALUES):
            continue

        p1_df = utils.filter_df_on_column_value(df, config.COL_PHOTOSTACKER_SIGN_1, p_name)
        p2_df = utils.filter_df_on_column_value(df, config.COL_PHOTOSTACKER_SIGN_2, p_name)
        # p2_df = pd.DataFrame()

        project_names_p1 = utils.df_column_to_uniques_list(p1_df, config.COL_PROJECT_NAME)
        project_names_p2 = utils.df_column_to_uniques_list(p2_df, config.COL_PROJECT_NAME)
        # project_names_p2 = []

        all_projects_worked = len(set(project_names_p1 + project_names_p2))

        rename, adjust, photostack = 0, 0, 0

        for temp_df in [p1_df, p2_df]:
            rename += utils.sum_df_on_a_column(temp_df, config.COL_RENAME)
            adjust += utils.sum_df_on_a_column(temp_df, config.COL_ADJUST)
            photostack += utils.sum_df_on_a_column(temp_df, config.COL_PHOTOSTACK)

        df_photostacker.loc[len(df_photostacker)] = [p_name, rename, adjust, photostack, all_projects_worked]

    df_photostacker.set_index("Photostacker", inplace=True)

    return df_photostacker


# Summary of "Photostackers" - Project Wise


def summary_of_photostackers_project_wise(df, start_date, end_date):

    # df_date_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOSTACKER_DATE)
    # grouped_project_wise_df = df_date_filtered.groupby([config.COL_PHOTOSTACKER_SIGN, config.COL_PROJECT_NAME])

    cols = ["start_date", "end_date", "Photostacker", "Project_name", "Rename", "Adjust", "Photostack", "Review", "extracted_project_date"]
    df_photostacker_project_wise = utils.get_empty_df(cols)

    all_photostacker_names = get_all_photostackers_names(df)

    for p_name in all_photostacker_names:

        # for photostacker_name_proj, group in grouped_project_wise_df:
        # photostacker_name, proj = photostacker_name_proj

        if (p_name == "") or (p_name == config.REDUNDANT_VALUE.lower()) or (p_name in config.UNMERGE_START_CONST_VALUES):
            continue

        p1_df = utils.filter_df_on_column_value(df, config.COL_PHOTOSTACKER_SIGN_1, p_name)
        p2_df = utils.filter_df_on_column_value(df, config.COL_PHOTOSTACKER_SIGN_2, p_name)

        p1_df_filtered = utils.filter_df_on_dates(p1_df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_1)
        p2_df_filtered = utils.filter_df_on_dates(p2_df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_2)

        concat_df = utils.concat_dfs([p1_df_filtered, p2_df_filtered])

        photostacker_unique_projects = utils.df_column_to_uniques_list(concat_df, config.COL_PROJECT_NAME)

        for project_name in photostacker_unique_projects:

            p1_project_filtered_df = utils.filter_df_on_column_value(p1_df_filtered, config.COL_PROJECT_NAME, project_name)
            p2_project_filtered_df = utils.filter_df_on_column_value(p2_df_filtered, config.COL_PROJECT_NAME, project_name)

            rename, adjust, photostack = 0, 0, 0

            for temp_df in [p1_project_filtered_df, p2_project_filtered_df]:
                rename += utils.sum_df_on_a_column(temp_df, config.COL_RENAME)
                adjust += utils.sum_df_on_a_column(temp_df, config.COL_ADJUST)
                photostack += utils.sum_df_on_a_column(temp_df, config.COL_PHOTOSTACK)

            temp_concat_df: DataFrame = utils.concat_dfs([p1_project_filtered_df, p2_project_filtered_df])
            review = temp_concat_df[config.COL_WARNINGS].str.contains(config.REVIEW_PHOTOSTACKER).any()
            review = "Investigate" if (review == True) else ""

            df_photostacker_project_wise.loc[len(df_photostacker_project_wise)] = [
                start_date,
                end_date,
                p_name,
                project_name,
                rename,
                adjust,
                photostack,
                review,
                temp_concat_df.loc[temp_concat_df[config.COL_PROJECT_NAME] == project_name, "extracted_project_date"].iloc[0]
            ]

    df_photostacker_project_wise.sort_values(by=["Photostacker", "extracted_project_date"], ascending=[True, False], inplace=True)
    df_photostacker_project_wise.set_index("Photostacker", inplace=True)

    return df_photostacker_project_wise


def summary_of_photostackers_by_month(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    print("Generating month-wise summary for photostackers...")

    # Convert dates to datetime and prepare month col (using DATE_1 only)
    # df_filtered[config.COL_PHOTOSTACKER_DATE_1] = pd.to_datetime(df_filtered[config.COL_PHOTOSTACKER_DATE_1], errors="coerce")
    df["month"] = df[config.COL_PHOTOSTACKER_DATE_1].dt.strftime("%b-%Y")

    all_months = sorted(df["month"].dropna().unique(), key=lambda x: pd.to_datetime("01-" + x))

    raw_photostackers = get_all_photostackers_names(df)
    all_photostackers = [
        name for name in raw_photostackers if name not in config.UNMERGE_START_CONST_VALUES and name != config.REDUNDANT_VALUE
    ]

    # Prepare result tables
    df_rename = pd.DataFrame(0, index=all_photostackers, columns=all_months)
    df_adjust = pd.DataFrame(0, index=all_photostackers, columns=all_months)
    df_photostack = pd.DataFrame(0, index=all_photostackers, columns=all_months)

    for p_name in all_photostackers:
        for month in all_months:
            # Filter only rows for the given month
            month_df = df[df["month"] == month]

            # Combine both signature columns for that name
            p1_df = utils.filter_df_on_column_value(month_df, config.COL_PHOTOSTACKER_SIGN_1, p_name)
            p2_df = utils.filter_df_on_column_value(month_df, config.COL_PHOTOSTACKER_SIGN_2, p_name)
            combined_df = pd.concat([p1_df, p2_df], ignore_index=True)

            df_rename.loc[p_name, month] = utils.sum_df_on_a_column(combined_df, config.COL_RENAME)
            df_adjust.loc[p_name, month] = utils.sum_df_on_a_column(combined_df, config.COL_ADJUST)
            df_photostack.loc[p_name, month] = utils.sum_df_on_a_column(combined_df, config.COL_PHOTOSTACK)

    df_rename.index.name = "Rename"
    df_adjust.index.name = "Adjust"
    df_photostack.index.name = "Photostack"

    return df_rename, df_adjust, df_photostack
