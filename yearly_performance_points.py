import utils

from summary_photographers import summary_of_photographers_project_wise
from summary_photostackers import summary_of_photostackers_project_wise
from summary_retouchers import summary_of_retouchers_project_wise



def modify_date_cols_to_month_year(df_staff_exp_KPI):

    date_modf_mapping = {}

    for col in df_staff_exp_KPI.columns:

        if col not in ['Names', 'Active', 'Category 1', 'Category 2', 'Category 3', 'Category 4']:

            col_date_modf = utils.convert_date_obj_to_str(col,  date_format='%b-%Y')
            date_modf_mapping[col] = col_date_modf

    df_staff_exp_KPI_modf = df_staff_exp_KPI.rename(columns=date_modf_mapping)

    return df_staff_exp_KPI_modf


def calc_month_end_date(end_date_obj):

    year, month = end_date_obj.year, end_date_obj.month

    # at index 1 is the number of days of this month
    day = utils.get_month_days(year, month)[1]

    user_end_date_yp_obj = utils.construct_pd_datetime_obj(year, month, day)

    return user_end_date_yp_obj


def date_list(start_date_obj, end_date_obj):

    date_range = utils.construct_list_of_dates(start_date_obj, end_date_obj, freq='M')

    user_date_list = []

    for end_date in date_range:
        start_date = utils.construct_pd_datetime_obj(end_date.year, end_date.month, 1)
        user_date_list.append([start_date, end_date])

    return user_date_list


def calculate_yearly_summary_tables(df, user_date_list, ppj_dict, sub_catg_list):
    # needs to be calulated again as dates are diffrent here in Yearly performance points table

    data = {}

    for user_start_date, user_end_date in user_date_list:

        month_year_str = utils.convert_date_obj_to_str(user_start_date, date_format='%b-%Y')

        df_photographers_yearly_summ = summary_of_photographers_project_wise(df, user_start_date, user_end_date)
        df_photographers_yearly_summ = utils.calc_KPI_from_PPJ(df_photographers_yearly_summ, ppj_dict, sub_catg_list)

        df_photostackers_yearly_summ = summary_of_photostackers_project_wise(df, user_start_date, user_end_date)
        df_photostackers_yearly_summ = utils.calc_KPI_from_PPJ(df_photostackers_yearly_summ, ppj_dict, sub_catg_list)

        df_retouchers_yearly_summ = summary_of_retouchers_project_wise(df, user_start_date, user_end_date)
        df_retouchers_yearly_summ = utils.calc_KPI_from_PPJ(df_retouchers_yearly_summ, ppj_dict, sub_catg_list)

        data[month_year_str] = [df_photographers_yearly_summ, df_photostackers_yearly_summ, df_retouchers_yearly_summ]

    return data


def calculate_actual_KPI_val(all_catg, df_dict_yearly_summ, staff_name):

    actual_kpi = 0
    # iterate over categories ['Photostacker', 'Retoucher' ...]
    for catg in all_catg:
        temp_df = df_dict_yearly_summ.get(catg)
        if temp_df is None:
            continue
        # filter this staff_name in the df
        kpi_df = utils.filter_column_values_for_active_staff(temp_df, [staff_name])
        actual_kpi += sum(kpi_df['KPI Points'])

    return int(actual_kpi)


def calculate_yearly_performance_table(yearly_summary_data, staff_and_their_categories, df_staff_exp_KPI_modf, active_staff):

    month_year_data = {}
    expected_kpi_set = set()

    for month_year_str, df_list in yearly_summary_data.items():

        df_dict_yearly_summ = {temp_df.index.name: temp_df for temp_df in df_list}

        print(month_year_str)

        month_year_data[month_year_str] = {}

        for staff_name, all_catg in staff_and_their_categories.items():

            cond = (df_staff_exp_KPI_modf['Names'] == staff_name)
            filtered = df_staff_exp_KPI_modf.loc[cond, month_year_str]
            # expected_kpi = df_staff_exp_KPI_modf.loc[cond, month_year_str].iloc[0]

            if filtered.empty:
                expected_kpi = '-'
                continue

            expected_kpi = filtered.iloc[0]
            if not expected_kpi:
                expected_kpi = '-'
                continue

            expected_kpi = int(expected_kpi)
            expected_kpi_set.add(expected_kpi)

            actual_kpi = calculate_actual_KPI_val(all_catg, df_dict_yearly_summ, staff_name)
            performance_points = calc_performance_points(actual_kpi, expected_kpi)

            month_year_data[month_year_str][staff_name] = f'{modify_string_for_printing(actual_kpi)}  [{expected_kpi}]  [{performance_points}]'

    df_yearly_performance = utils.get_df_from_dict(month_year_data)
    df_yearly_performance.sort_index(inplace = True)

    df_yearly_performance = utils.filter_index_for_active_staff(df_yearly_performance, active_staff)

    return df_yearly_performance, expected_kpi_set


def modify_string_for_printing(actual_kpi):
    l = len(str(actual_kpi))
    rem_spaces = 0

    if l < 3:
        rem_spaces = 3 - l

    spaces = ' ' * rem_spaces
    return f'{spaces}{actual_kpi}'


def get_performance_points_chart(target_kpi):

    bucket_size = target_kpi // 4

    number_list = list(range(0, target_kpi + 1))
    grouped_numbers = [number_list[i:i+bucket_size] for i in range(0, len(number_list), bucket_size)]

    # add a point for 0
    point_chart = {(0, 0):0}

    # start with point value 1
    points = 1

    for i, group in enumerate(grouped_numbers):

        start_rng, end_rng = group[0], group[-1]
        if i == 0:
            # change the point that needs to be start with 1 rather than 0
            # coz if actual kpi = 0 then points = 0
            start_rng += 1

        point_chart[(start_rng, end_rng)] = points
        # increment point value for next group
        points += 1

    return point_chart


def calc_performance_points(actual_kpi, target_kpi):

    point_chart = get_performance_points_chart(target_kpi)

    if actual_kpi == 0:
        return 0

    if actual_kpi >= target_kpi:
        return 5

    for (start_rng, end_rng), point in point_chart.items():
        if start_rng <= actual_kpi <= end_rng:
            return point
    return


def make_points_reference_chart(expected_kpi_set):
    expected_kpi_points_table = {k: get_performance_points_chart(k) for k in expected_kpi_set}

    cols = ['target_kpi', 0, 1, 2, 3, 4, 5]
    df_points_chart = utils.get_empty_df(cols)

    for exp_kpi, points_chart in expected_kpi_points_table.items():
        df_points_chart.loc[len(df_points_chart)] = [exp_kpi] + list(points_chart.keys())

    df_points_chart = df_points_chart.set_index('target_kpi').sort_index()
    df_points_chart.index.name = "target_kpi ↓ \ Points →"

    return df_points_chart


def write_yearly_performance_sheet(writer, wb, yearly_performance_sheet, df, df_points_chart, start_date, end_date):

    ws = wb.add_worksheet(yearly_performance_sheet)
    writer.sheets[yearly_performance_sheet] = ws

    ws.write(0, 0, 'Yearly Performance Rating')
    ws.write(1, 0, 'Start Date')
    ws.write(2, 0, 'End Date')
    ws.write(3, 0, 'Note:')
    ws.write(4, 0, 'Legend:')

    ws.write(1, 1, utils.convert_date_obj_to_str(start_date, date_format='%b-%Y'))
    ws.write(2, 1, utils.convert_date_obj_to_str(end_date, date_format='%b-%Y'))
    ws.write(3, 1, 'This is the yearly performance result, including KPI results')
    ws.write(4, 1, '300 [300] [1] - Actual Result [Expected Result] [Performance Rating]')

    startrow = 6
    startcol = 0

    df.to_excel(writer,
               sheet_name=yearly_performance_sheet,
               startrow=startrow,
               startcol=startcol, index=True)

    points_table_row = startrow + len(df) + 4
    ws.write(points_table_row, 0, 'Points table reference:')

    points_table_row += 2
    df_points_chart.to_excel(writer,
                             sheet_name=yearly_performance_sheet,
                             startrow=points_table_row,
                             startcol=startcol, index=True)
    return
