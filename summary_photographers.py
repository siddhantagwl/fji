# Summary of Photographers - All projects

# import os
# import sys

# curr_path = os.getcwd()
# print(curr_path)
# sys.path.append(os.path.join(curr_path, 'python_program'))

import config
import utils
from pandas import DataFrame


def calc_photographers(df: DataFrame):

    items, images = 0, 0

    cols_to_sum = [config.COL_PHOTOGRAPHY, config.COL_BESPOKE,
                   config.COL_PHOTOGRAPHY_TO_VARIANCE, config.COL_SAMPLES_RESTAKE]

    for indx, row in df.iterrows():

        # find the total based on the below columns and then add to items
        total = 0

        for col in cols_to_sum:
            total += df.loc[indx, col]
        items += total

        # increment the images count if total is > 0
        if total > 0:
            images += 1

    return items, images


def get_all_photographer_names(df: DataFrame):

    p1_sign_names = utils.df_column_to_uniques_list(df, config.COL_PHOTOGRAPHER_1)
    p2_sign_names = utils.df_column_to_uniques_list(df, config.COL_PHOTOGRAPHER_2)
    p3_sign_names = utils.df_column_to_uniques_list(df, config.COL_PHOTOGRAPHER_3)

    all_photographer_names = p1_sign_names + p2_sign_names + p3_sign_names
    all_photographer_names = list(dict.fromkeys(all_photographer_names))
    all_photographer_names = utils.remove_empty_str_values(all_photographer_names)

    return all_photographer_names



def summary_of_photographers_all_projects(df: DataFrame, include_overall, start_date, end_date):

    cols = ['Photographer', 'Items', 'Images', '-', '#_projects_worked']
    df_photographers = utils.get_empty_df(cols)

    # making an initial copy, if no dates have to be included, this df_filtered
    # can be used to maintain code consistency
    df_date_filtered = df.copy()

    if include_overall == 'n':
        # don't include the overall dates, rather use the start and end date provided by
        # user in the KP_invoivce cells
        df_date_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOGRAPHER_DATE)

    all_photographer_names = get_all_photographer_names(df_date_filtered)

    for p_name in all_photographer_names:

        if (p_name == "") or (p_name in config.UNMERGE_START_CONST_VALUES):
            continue

        p1_df: DataFrame = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOGRAPHER_1, p_name)
        p2_df: DataFrame = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOGRAPHER_2, p_name)
        p3_df: DataFrame = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOGRAPHER_3, p_name)

        project_names_p1 = p1_df[config.COL_PROJECT_NAME].unique().tolist()
        project_names_p2 = p2_df[config.COL_PROJECT_NAME].unique().tolist()
        project_names_p3 = p3_df[config.COL_PROJECT_NAME].unique().tolist()

        all_projects_worked = len(set(project_names_p1 + project_names_p2 + project_names_p3))
        #all_projects_worked = str(len(all_projects_worked)) + '-> ' + ' || '.join(all_projects_worked)

        items1, images1 = calc_photographers(p1_df)
        items2, images2 = calc_photographers(p2_df)
        items3, images3 = calc_photographers(p3_df)

        items = items1 + items2 + items3
        images = images1 + images2 + images3

        df_photographers.loc[len(df_photographers)] = [p_name, items, images, '' , all_projects_worked]

    df_photographers.set_index('Photographer', inplace=True)

    return df_photographers

#--------------------------------------------------------------------------------------------------

# Summary of Photographers - Project Wise

def summary_of_photographers_project_wise(df: DataFrame, start_date, end_date):

    # make an empty output Dataframe
    cols = ['start_date', 'end_date', 'Photographer', 'Project_name', 'Items', 'Images']
    df_photographers_project_wise = utils.get_empty_df(cols)

    df_date_filtered = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOGRAPHER_DATE)

    all_photographer_names = get_all_photographer_names(df_date_filtered)

    for p_name in all_photographer_names:

        if (p_name == "") or (p_name in config.UNMERGE_START_CONST_VALUES):
            continue

        p1_df = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOGRAPHER_1, p_name)
        p2_df = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOGRAPHER_2, p_name)
        p3_df = utils.filter_df_on_column_value(df_date_filtered, config.COL_PHOTOGRAPHER_3, p_name)


        concat_df = utils.concat_dfs([p1_df, p2_df, p3_df])

        photographer_unique_projects = utils.df_column_to_uniques_list(concat_df, config.COL_PROJECT_NAME)

        # iterate over uniq project names
        for project_name in photographer_unique_projects:

            # filter on the project name
            p1_project_filtered_df = utils.filter_df_on_column_value(p1_df, config.COL_PROJECT_NAME, project_name)
            p2_project_filtered_df = utils.filter_df_on_column_value(p2_df, config.COL_PROJECT_NAME, project_name)
            p3_project_filtered_df = utils.filter_df_on_column_value(p3_df, config.COL_PROJECT_NAME, project_name)

            # calculate the items and images for the filered DF
            photographer_items_1, photographer_images_1 = calc_photographers(p1_project_filtered_df)
            photographer_items_2, photographer_images_2 = calc_photographers(p2_project_filtered_df)
            photographer_items_3, photographer_images_3 = calc_photographers(p3_project_filtered_df)

            photographer_items = photographer_items_1 + photographer_items_2 + photographer_items_3
            photographer_images = photographer_images_1 + photographer_images_2 + photographer_images_3

            # add the result to the output dataframe
            df_photographers_project_wise.loc[len(df_photographers_project_wise)] = [start_date, end_date,
                                                                                     p_name, project_name,
                                                                                     photographer_items, photographer_images]

    df_photographers_project_wise.set_index('Photographer', inplace=True)

    return df_photographers_project_wise
