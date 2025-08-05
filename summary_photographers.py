# Summary of Photographers - All projects
from pandas import DataFrame
import pandas as pd

import config
import utils


def calc_photographers(df: DataFrame) -> tuple[int, int]:
    """
    Calculate the total items and images for photographers.
    sum the values in the columns COL_PHOTOGRAPHY, COL_BESPOKE, COL_PHOTOGRAPHY_TO_VARIANCE, COL_SAMPLES_RESTAKE
    and return the total items and images.
    increment the images count if the total is greater than 0.
    """
    items, images = 0, 0

    cols_to_sum = [config.COL_PHOTOGRAPHY, config.COL_BESPOKE, config.COL_PHOTOGRAPHY_TO_VARIANCE, config.COL_SAMPLES_RESTAKE]

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

    # Normalize: strip leading/trailing whitespace
    all_photographer_names = [name.strip().lower() for name in all_photographer_names if isinstance(name, str)]

    # dropping duplicates while maintaining order
    all_photographer_names = list(dict.fromkeys(all_photographer_names))
    all_photographer_names = utils.remove_empty_str_values(all_photographer_names)

    return all_photographer_names


def summary_of_photographers_all_projects(df: DataFrame, include_overall: bool, start_date, end_date):

    cols = ["Photographer", "Items", "Images", "-", "#_projects_worked"]
    df_photographers = utils.get_empty_df(cols)

    if not include_overall:
        # don't include the overall dates, rather use the start and end date provided by
        # user in the KP_invoivce cells
        df = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOGRAPHER_DATE)

    all_photographer_names = get_all_photographer_names(df)

    for p_name in all_photographer_names:

        if (p_name == "") or (p_name in config.UNMERGE_START_CONST_VALUES):
            continue

        p1_df: DataFrame = utils.filter_df_on_column_value(df, config.COL_PHOTOGRAPHER_1, p_name)
        p2_df: DataFrame = utils.filter_df_on_column_value(df, config.COL_PHOTOGRAPHER_2, p_name)
        p3_df: DataFrame = utils.filter_df_on_column_value(df, config.COL_PHOTOGRAPHER_3, p_name)

        project_names_p1 = p1_df[config.COL_PROJECT_NAME].unique().tolist()
        project_names_p2 = p2_df[config.COL_PROJECT_NAME].unique().tolist()
        project_names_p3 = p3_df[config.COL_PROJECT_NAME].unique().tolist()

        all_projects_worked = len(set(project_names_p1 + project_names_p2 + project_names_p3))

        items1, images1 = calc_photographers(p1_df)
        items2, images2 = calc_photographers(p2_df)
        items3, images3 = calc_photographers(p3_df)

        items = items1 + items2 + items3
        images = images1 + images2 + images3

        df_photographers.loc[len(df_photographers)] = [p_name, items, images, "", all_projects_worked]

    df_photographers.set_index("Photographer", inplace=True)

    return df_photographers


# --------------------------------------------------------------------------------------------------

# Summary of Photographers - Project Wise


def summary_of_photographers_project_wise(df: DataFrame, start_date, end_date):

    # make an empty output Dataframe
    cols = ["start_date", "end_date", "Photographer", "Project_name", "Items", "Images", "extracted_project_date"]
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
            df_photographers_project_wise.loc[len(df_photographers_project_wise)] = [
                start_date,
                end_date,
                p_name,
                project_name,
                photographer_items,
                photographer_images,
                concat_df.loc[concat_df[config.COL_PROJECT_NAME] == project_name, "extracted_project_date"].iloc[0]
            ]

    df_photographers_project_wise.sort_values(by=["Photographer", "extracted_project_date"], ascending=[True, False], inplace=True)
    df_photographers_project_wise.set_index("Photographer", inplace=True)

    return df_photographers_project_wise


def summary_of_photographers_by_month(df: pd.DataFrame):
    # Filter by Photographer Date
    print("Generating month-wise summary for photographers...")

    # Convert date to datetime if not already
    # df_filtered[config.COL_PHOTOGRAPHER_DATE] = pd.to_datetime(df_filtered[config.COL_PHOTOGRAPHER_DATE], errors='coerce')
    df["month"] = df[config.COL_PHOTOGRAPHER_DATE].dt.strftime("%b-%Y")

    # Get all valid photographer names
    raw_photographers = get_all_photographer_names(df)
    all_photographers = [name for name in raw_photographers if name and name.lower() not in config.UNMERGE_START_CONST_VALUES]

    all_months = sorted(df["month"].dropna().unique(), key=lambda x: pd.to_datetime("01-" + x))

    # Build items count by month
    result = pd.DataFrame(0, index=all_photographers, columns=all_months)

    for p_name in all_photographers:
        for month in all_months:
            # Filter for that month only
            month_df = df[df["month"] == month]

            # Filter for this photographer in all 3 columns
            p1_df = utils.filter_df_on_column_value(month_df, config.COL_PHOTOGRAPHER_1, p_name)
            p2_df = utils.filter_df_on_column_value(month_df, config.COL_PHOTOGRAPHER_2, p_name)
            p3_df = utils.filter_df_on_column_value(month_df, config.COL_PHOTOGRAPHER_3, p_name)

            # Use existing logic to calculate items
            items1, _ = calc_photographers(p1_df)
            items2, _ = calc_photographers(p2_df)
            items3, _ = calc_photographers(p3_df)

            total_items = items1 + items2 + items3
            result.loc[p_name, month] = total_items

    result.index.name = "Items"
    return result
