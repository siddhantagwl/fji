import config
import utils


# Summary of Retouchers - All projects combined

def calc_retouches(df):

    transfer = 0
    retouches = 0
    variance = 0

    for indx, row in df.iterrows():

        if row[config.COL_REJECT_RETOUCHERS_PAY] == "y":
            continue

        transfer += df.loc[indx, config.COL_TRANSFER]

        cols = [config.COL_PHOTOGRAPHY, config.COL_BESPOKE,
                config.COL_SUPERIMPOSE, config.COL_SAMPLES_RESTAKE]
        for col in cols:
            retouches += df.loc[indx, col]

        cols = [config.COL_CAPPED, config.COL_PHOTOGRAPHY_TO_VARIANCE,
                config.COL_VARIANCE, config.COL_COMBINE]
        for col in cols:
            variance += df.loc[indx, col]

    return transfer, retouches, variance


def get_all_retoucher_names(df):

    r1_sign_names = utils.df_column_to_uniques_list(df, config.COL_RETOUCHERS_SIGN_1)
    r2_sign_names = utils.df_column_to_uniques_list(df, config.COL_RETOUCHERS_SIGN_2)
    r3_sign_names = utils.df_column_to_uniques_list(df, config.COL_RETOUCHERS_SIGN_3)
    r4_sign_names = utils.df_column_to_uniques_list(df, config.COL_RETOUCHERS_SIGN_4)
    r5_sign_names = utils.df_column_to_uniques_list(df, config.COL_RETOUCHERS_SIGN_5)

    all_retoucher_names = r1_sign_names + r2_sign_names + r3_sign_names + r4_sign_names + r5_sign_names
    all_retoucher_names = list(dict.fromkeys(all_retoucher_names))
    all_retoucher_names = utils.remove_empty_str_values(all_retoucher_names)
    return all_retoucher_names


def summary_of_retouchers_all_projects(df, include_overall, start_date, end_date):

    df_date_filtered = df.copy()

    if include_overall == 'n':
        # don't include the overall dates, rather use the start and end date provided by
        # user in the KP_invoivce cells
        temp_df_date1_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_1)
        temp_df_date2_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_2)
        temp_df_date3_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_3)
        temp_df_date4_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_4)
        temp_df_date5_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_5)

        df_date_filtered = utils.concat_dfs([temp_df_date1_filtered, temp_df_date2_filtered, temp_df_date3_filtered,
                                             temp_df_date4_filtered, temp_df_date5_filtered])
        df_date_filtered.reset_index(drop=True, inplace=True)

    cols = ['Retoucher', 'Transfer', 'Retouches', 'Variance', '#_projects_worked']
    df_retouchers_signed = utils.get_empty_df(cols)

    all_retoucher_names = get_all_retoucher_names(df_date_filtered)

    for r_name in sorted(all_retoucher_names):

        if (r_name == "") or (r_name == config.TO_DUPLICATE_VAL) or (r_name == config.REDUNDANT_VALUE):
            continue

        r1_df = utils.filter_df_on_retoucher_name(df_date_filtered, r_name, config.COL_RETOUCHERS_SIGN_1)
        r2_df = utils.filter_df_on_retoucher_name(df_date_filtered, r_name, config.COL_RETOUCHERS_SIGN_2)
        r3_df = utils.filter_df_on_retoucher_name(df_date_filtered, r_name, config.COL_RETOUCHERS_SIGN_3)
        r4_df = utils.filter_df_on_retoucher_name(df_date_filtered, r_name, config.COL_RETOUCHERS_SIGN_4)
        r5_df = utils.filter_df_on_retoucher_name(df_date_filtered, r_name, config.COL_RETOUCHERS_SIGN_5)

        project_names_r1 = utils.df_column_to_uniques_list(r1_df, config.COL_PROJECT_NAME)
        project_names_r2 = utils.df_column_to_uniques_list(r2_df, config.COL_PROJECT_NAME)
        project_names_r3 = utils.df_column_to_uniques_list(r3_df, config.COL_PROJECT_NAME)
        project_names_r4 = utils.df_column_to_uniques_list(r3_df, config.COL_PROJECT_NAME)
        project_names_r5 = utils.df_column_to_uniques_list(r3_df, config.COL_PROJECT_NAME)

        all_projects_worked = len(set(project_names_r1 + project_names_r2 + project_names_r3 + project_names_r4 + project_names_r5))

        transfer, retouches, variance = 0, 0, 0

        for temp_df in [r1_df, r2_df, r3_df, r4_df, r5_df]:
            transfer1, retouches1, variance1 = calc_retouches(temp_df)
            transfer += transfer1
            retouches += retouches1
            variance += variance1

        df_retouchers_signed.loc[len(df_retouchers_signed)] = [r_name, transfer, retouches, variance, all_projects_worked]

    df_retouchers_signed.set_index('Retoucher', inplace=True)

    return df_retouchers_signed


#--------------------------------------------------------------------------------------------------

# Summary of Retouchers - Project wise

def summary_of_retouchers_project_wise(df, start_date, end_date):

    cols = ['start_date', 'end_date', 'Retoucher', 'Project_name', 'Transfer', 'Retouches', 'Variance', 'Review']
    df_retouchers_signed_project_wise = utils.get_empty_df(cols)

    all_retoucher_names = get_all_retoucher_names(df)

    for r_name in all_retoucher_names:

        if (r_name == "") or (r_name == config.TO_DUPLICATE_VAL):
            continue

        r1_df = utils.filter_df_on_retoucher_name(df, r_name, config.COL_RETOUCHERS_SIGN_1)
        r2_df = utils.filter_df_on_retoucher_name(df, r_name, config.COL_RETOUCHERS_SIGN_2)
        r3_df = utils.filter_df_on_retoucher_name(df, r_name, config.COL_RETOUCHERS_SIGN_3)
        r4_df = utils.filter_df_on_retoucher_name(df, r_name, config.COL_RETOUCHERS_SIGN_4)
        r5_df = utils.filter_df_on_retoucher_name(df, r_name, config.COL_RETOUCHERS_SIGN_5)

        r1_df_filtered = utils.filter_df_on_dates(r1_df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_1)
        r2_df_filtered = utils.filter_df_on_dates(r2_df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_2)
        r3_df_filtered = utils.filter_df_on_dates(r3_df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_3)
        r4_df_filtered = utils.filter_df_on_dates(r4_df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_4)
        r5_df_filtered = utils.filter_df_on_dates(r5_df, start_date, end_date, config.COL_DATE_DONE_RETOUCHERS_SIGN_5)

        concat_df = utils.concat_dfs([r1_df_filtered, r2_df_filtered, r3_df_filtered, r4_df_filtered, r5_df_filtered])

        retoucher_unique_projects = utils.df_column_to_uniques_list(concat_df, config.COL_PROJECT_NAME)

        for project_name in retoucher_unique_projects:

            # filter on the project name
            r1_project_filtered_df = utils.filter_df_on_column_value(r1_df_filtered, config.COL_PROJECT_NAME, project_name)
            r2_project_filtered_df = utils.filter_df_on_column_value(r2_df_filtered, config.COL_PROJECT_NAME, project_name)
            r3_project_filtered_df = utils.filter_df_on_column_value(r3_df_filtered, config.COL_PROJECT_NAME, project_name)
            r4_project_filtered_df = utils.filter_df_on_column_value(r4_df_filtered, config.COL_PROJECT_NAME, project_name)
            r5_project_filtered_df = utils.filter_df_on_column_value(r5_df_filtered, config.COL_PROJECT_NAME, project_name)

            transfer, retouches, variance = 0, 0, 0

            for temp_df in [r1_project_filtered_df, r2_project_filtered_df, r3_project_filtered_df, r4_project_filtered_df, r5_project_filtered_df]:
                transfer1, retouches1, variance1 = calc_retouches(temp_df)
                transfer += transfer1
                retouches += retouches1
                variance += variance1

            temp_concat_df = utils.concat_dfs([r1_project_filtered_df, r2_project_filtered_df, r3_project_filtered_df, r4_project_filtered_df, r5_project_filtered_df])
            review = temp_concat_df[config.COL_WARNINGS].str.contains(config.REVIEW_RETOUCHER).any()
            review = 'Investigate' if (review == True) else ''

            df_retouchers_signed_project_wise.loc[len(df_retouchers_signed_project_wise)] = [start_date, end_date, r_name,
                                                                                             project_name, transfer, retouches,
                                                                                             variance, review]

    df_retouchers_signed_project_wise.set_index('Retoucher', inplace=True)

    return df_retouchers_signed_project_wise
