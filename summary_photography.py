from pandas import DataFrame

import config
import utils
from summary_photographers import get_all_photographer_names
from summary_photostackers import get_all_photostackers_names
from summary_retouchers import get_all_retoucher_names


def summary_of_photography(df: DataFrame):

    cols = ["Photography_date", "Items", "Images", "#_projects_done"]
    df_photography_summary = utils.get_empty_df(cols)

    for photography_date, group in df.groupby(config.COL_PHOTOGRAPHER_DATE):
        # print(photography_date)
        if not photography_date:
            continue

        all_projects_worked = len(set(group[config.COL_PROJECT_NAME].values))

        total_items = 0
        total_images = 0

        for indx, row in group.iterrows():
            unmerge_start_val = group.loc[indx, config.COL_UNMERGE_START]
            if unmerge_start_val == config.TRANSFER_VALUE:
                continue

            photographer_values = set(
                group.loc[indx, [config.COL_PHOTOGRAPHER_1, config.COL_PHOTOGRAPHER_2, config.COL_PHOTOGRAPHER_3]].values
            )

            # if all same values of NA, CV, FJI in p1,p2,p3 then skip
            if len(photographer_values) == 1 and list(photographer_values)[0] in config.PHOTOGRAPHER_SIGN_CONST_VALUES:
                continue

            # Now get the actual values of photopgraphy, bespoke etc
            val_photography = group.loc[indx, config.COL_PHOTOGRAPHY]
            val_bespoke = group.loc[indx, config.COL_BESPOKE]
            val_capped = group.loc[indx, config.COL_CAPPED]
            val_photovar = group.loc[indx, config.COL_PHOTOGRAPHY_TO_VARIANCE]
            val_samples_reshoot = group.loc[indx, config.COL_SAMPLES_RESTAKE]

            row_sum = sum([val_photography, val_bespoke, val_capped, val_photovar, val_samples_reshoot])

            total_items += row_sum

            if row_sum > 0:
                total_images += 1

        df_photography_summary.loc[len(df_photography_summary)] = [
            photography_date,
            total_items,
            total_images,
            all_projects_worked,
        ]

    df_photography_summary["Photography_date"] = utils.convert_df_col_to_date(df_photography_summary["Photography_date"])

    return df_photography_summary


def summary_of_photography_project_wise(df: DataFrame, include_overall: str, start_date, end_date) -> DataFrame:

    cols = ["Photography_date", "Project_name", "Items", "Images", "Photographers", "Photostackers", "Retouchers"]
    df_photography_summary_project_wise = utils.get_empty_df(cols)

    if include_overall == "n":
        df = utils.filter_df_on_dates(df, start_date, end_date, config.COL_PHOTOGRAPHER_DATE)
        df.reset_index(drop=True, inplace=True)

    for (photography_date, project_name), group in df.groupby([config.COL_PHOTOGRAPHER_DATE, config.COL_PROJECT_NAME]):

        if not photography_date:
            continue

        # total_items = 0
        # total_images = 0

        # unmerge_start_val = group.loc[indx, config.COL_UNMERGE_START]
        # if unmerge_start_val == config.TRANSFER_VALUE:
        #     continue
        # photographer_values = set(
        #     group.loc[indx, [config.COL_PHOTOGRAPHER_1, config.COL_PHOTOGRAPHER_2, config.COL_PHOTOGRAPHER_3]].values
        # )
        # # if all same values of NA, CV, FJI in p1,p2,p3 then skip
        # if len(photographer_values) == 1 and list(photographer_values)[0] in config.PHOTOGRAPHER_SIGN_CONST_VALUES:
        #     continue

        # Filter out rows to skip in one go for efficiency (efficiency logic to above)
        skip_mask = (
            (group[config.COL_UNMERGE_START] == config.TRANSFER_VALUE)
            | (
                group[[config.COL_PHOTOGRAPHER_1, config.COL_PHOTOGRAPHER_2, config.COL_PHOTOGRAPHER_3]]
                .apply(lambda x: len(set(x)) == 1 and list(set(x))[0] in config.PHOTOGRAPHER_SIGN_CONST_VALUES, axis=1)
            )
        )
        filtered_group = group[~skip_mask]

        # Calculate totals efficiently
        item_cols = [
            config.COL_PHOTOGRAPHY,
            config.COL_BESPOKE,
            config.COL_CAPPED,
            config.COL_PHOTOGRAPHY_TO_VARIANCE,
            config.COL_SAMPLES_RESTAKE,
        ]
        # Calculate row sums for the item columns
        row_sums = filtered_group[item_cols].sum(axis=1)
        total_items = row_sums.sum()
        total_images = (row_sums > 0).sum()

        all_photographer_names = get_all_photographer_names(group)
        all_photostackers_names = get_all_photostackers_names(group)
        all_retouchers_names = get_all_retoucher_names(group)

        df_photography_summary_project_wise.loc[len(df_photography_summary_project_wise)] = [
            photography_date,
            project_name,
            total_items,
            total_images,
            ", ".join(all_photographer_names),
            ", ".join(all_photostackers_names),
            ", ".join(all_retouchers_names),
        ]

    df_photography_summary_project_wise["Photography_date"] = utils.convert_df_col_to_date(
        df_photography_summary_project_wise["Photography_date"]
    )
    df_photography_summary_project_wise["Items"] = utils.convert_df_col_to_numeric(df_photography_summary_project_wise["Items"])
    df_photography_summary_project_wise["Images"] = utils.convert_df_col_to_numeric(
        df_photography_summary_project_wise["Images"]
    )

    return df_photography_summary_project_wise
