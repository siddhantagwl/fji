from pandas import DataFrame

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

    # making an initial copy, if no dates have to be included, this df_filtered
    # can be used to maintain code consistency
    df_date_filtered: DataFrame = df.copy()

    if include_overall == "n":
        # don't include the overall dates, rather use the start and end date provided by
        # user in the KP_invoivce cells
        temp_df_date1_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_1)
        temp_df_date2_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_2)
        # temp_df_date2_filtered = pd.DataFrame()

        df_date_filtered = utils.concat_dfs([temp_df_date1_filtered, temp_df_date2_filtered])
        df_date_filtered.reset_index(drop=True, inplace=True)

    # make an empty dataframe with these columns
    cols = ["Photostacker", "Rename", "Adjust", "Photostack", "#_projects_worked"]
    df_photostacker = utils.get_empty_df(cols)

    all_photostacker_names = get_all_photostackers_names(df_date_filtered)

    for p_name in sorted(all_photostacker_names):

        if (p_name == "") or (p_name == config.REDUNDANT_VALUE.lower()) or (p_name in config.UNMERGE_START_CONST_VALUES):
            continue

        p1_df = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOSTACKER_SIGN_1, p_name)
        p2_df = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOSTACKER_SIGN_2, p_name)
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

    cols = ["start_date", "end_date", "Photostacker", "Project_name", "Rename", "Adjust", "Photostack", "Review"]
    df_photostacker_project_wise = utils.get_empty_df(cols)

    all_photostacker_names = get_all_photostackers_names(df)

    for p_name in all_photostacker_names:

        # for photostacker_name_proj, group in grouped_project_wise_df:
        # photostacker_name, proj = photostacker_name_proj

        if (p_name == "") or (p_name == config.REDUNDANT_VALUE.lower()) or (p_name in config.UNMERGE_START_CONST_VALUES):
            continue

        # ------------------------------
        # phototstacker_sign_date_dict = {
        #     config.COL_PHOTOSTACKER_SIGN_1: config.COL_PHOTOSTACKER_DATE_1,
        #     config.COL_PHOTOSTACKER_SIGN_2: config.COL_PHOTOSTACKER_DATE_2,
        # }

        # concat_df = pd.DataFrame()
        # p_df_filtered_output_list = []

        # for sign_col, date_col in phototstacker_sign_date_dict.items():
        #     if sign_col not in df.columns:
        #         continue
        #     p_df = utils.filter_df_on_column_value(df, sign_col, p_name)
        #     p_df_filtered = utils.filter_df_on_dates(p_df, start_date, end_date, date_col)
        #     p_df_filtered_output_list.append(p_df_filtered)
        #     concat_df = utils.concat_dfs([concat_df, p_df])
        # ------------------------------

        p1_df = utils.filter_df_on_column_value(df, config.COL_PHOTOSTACKER_SIGN_1, p_name)
        p2_df = utils.filter_df_on_column_value(df, config.COL_PHOTOSTACKER_SIGN_2, p_name)

        p1_df_filtered = utils.filter_df_on_dates(p1_df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_1)
        p2_df_filtered = utils.filter_df_on_dates(p2_df, start_date, end_date, config.COL_PHOTOSTACKER_DATE_2)

        concat_df = utils.concat_dfs([p1_df_filtered, p2_df_filtered])

        photostacker_unique_projects = utils.df_column_to_uniques_list(concat_df, config.COL_PROJECT_NAME)

        for project_name in photostacker_unique_projects:

            # filter on the project name
            # project_filtered_df_concat = pd.DataFrame()
            # for temp_df in p_df_filtered_output_list:
            #     p_project_filtered_df = utils.filter_df_on_column_value(temp_df, config.COL_PROJECT_NAME, project_name)
            #     project_filtered_df_concat = utils.concat_dfs([project_filtered_df_concat, p_project_filtered_df])

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
            ]

    df_photostacker_project_wise.set_index("Photostacker", inplace=True)

    return df_photostacker_project_wise
