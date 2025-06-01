COL_FJI = 'FJI Item No.'
COL_PRIORITY = 'Priority'
COL_NOTES_FOR_FJI = 'NOTES for FJI'
COL_WARNINGS = 'Warnings'

COL_PHOTOGRAPHER_1 =  'Photographer 1'
COL_PHOTOGRAPHER_2 =  'Photographer 2'
COL_PHOTOGRAPHER_3 =  'Photographer 3'
COL_PHOTOGRAPHER_DATE = 'Photographer Date'

COL_UNMERGE_START = 'Unmerge'
COL_UNMERGE_END = 'Unmerge (additional)'
COL_NOTES_FOR_PHOTOSTACKERS = 'Notes for Photostackers'
COL_RENAME = 'Rename'
COL_ADJUST = 'Adjust'
COL_PHOTOSTACK = 'Photostack'

COL_PHOTOSTACKER_SIGN_1 = 'Photostacker Sign 1'
COL_PHOTOSTACKER_DATE_1 = 'Photostacker Date 1'
COL_PHOTOSTACKER_SIGN_2 = 'Photostacker Sign 2'
COL_PHOTOSTACKER_DATE_2 = 'Photostacker Date 2'

COL_PAGE_NO = 'Page No.'
COL_CLIENT_SKU_NO = 'Client SKU No.'
COL_JEWELLERY_TYPE = 'Jewellery Type'
COL_DIAMOND_COLOUR = 'Diamond Colour'
COL_METAL_COLOUR = 'Metal Colour'
COL_ANGLE = 'Angle'
COL_OTHER = 'Other'
COL_IMAGES = "Images"

COL_FILENAME =  'Filename'
COL_NOTES_FOR_RETOUCHERS = 'Notes for Retouchers'
COL_BASE_FILE = 'BASE file'

COL_PHOTOGRAPHY =  'Photography'
COL_BESPOKE =  'Bespoke'
COL_GROUPSHOT =  'Groupshot'
COL_CAPPED =  'Capped'
COL_PHOTOGRAPHY_TO_VARIANCE =  'Photography to Variance'
COL_VARIANCE =  'Variance'
COL_SUPERIMPOSE =  'Superimpose'
COL_COMBINE =  'Combine'
COL_SAMPLES_RESTAKE =  'Samples & Retake'
COL_DUPLICATE_FILES =  'Duplicate Files'
COL_TRANSFER = 'Transfer'

COL_REJECT_FILE_COUNT =  'Reject File Count'
COL_REJECT_CLIENT_PAY =  'Reject Client Pay'
COL_REJECT_RETOUCHERS_PAY =  'Reject Retouchers Pay'

COL_RETOUCHERS_SIGN_1 = 'Retouchers Sign 1'
COL_DATE_DONE_RETOUCHERS_SIGN_1 =  'Date Done Retouchers Sign 1'
COL_RETOUCHERS_SIGN_2 =  'Retouchers Sign 2'
COL_DATE_DONE_RETOUCHERS_SIGN_2 =  'Date Done Retouchers Sign 2'
COL_RETOUCHERS_SIGN_3 =  'Retouchers Sign 3'
COL_DATE_DONE_RETOUCHERS_SIGN_3 =  'Date Done Retouchers Sign 3'
COL_RETOUCHERS_SIGN_4 =  'Retouchers Sign 4'
COL_DATE_DONE_RETOUCHERS_SIGN_4 =  'Date Done Retouchers Sign 4'
COL_RETOUCHERS_SIGN_5 =  'Retouchers Sign 5'
COL_DATE_DONE_RETOUCHERS_SIGN_5 =  'Date Done Retouchers Sign 5'

COL_PROJECT_NAME = 'Project Name'
COL_FILETAG = 'Filetag'
COL_JOBSHEET_FILEVERSION = "jobsheet_fileversion"

UNMERGE_START_CONST_VALUES = ["cv", "na", "n/a", "fji", "transfer"]
PHOTOGRAPHER_SIGN_CONST_VALUES =["cv", "na", "n/a", "fji"]
TO_DUPLICATE_VAL = "to duplicate"
TRANSFER_VALUE = 'transfer'
REDUNDANT_VALUE = "REDUNDANT"
REVIEW_RETOUCHER = "REVIEW RETOUCHER:"
REVIEW_PHOTOSTACKER = "REVIEW PHOTOSTACK:"

COLS_TO_EXPECT_IN_CSV = [COL_WARNINGS, COL_PHOTOGRAPHER_1, COL_PHOTOGRAPHER_2,
                         COL_PHOTOGRAPHER_3, COL_PHOTOGRAPHER_DATE,
                         COL_UNMERGE_START, COL_UNMERGE_END, COL_RENAME,
                         COL_ADJUST, COL_PHOTOSTACK,
                         COL_PHOTOSTACKER_SIGN_1, COL_PHOTOSTACKER_DATE_1,
                         COL_PHOTOSTACKER_SIGN_2, COL_PHOTOSTACKER_DATE_2,
                         COL_FILENAME, COL_PHOTOGRAPHY,
                         COL_BESPOKE, COL_GROUPSHOT, COL_CAPPED, COL_PHOTOGRAPHY_TO_VARIANCE,
                         COL_VARIANCE, COL_SUPERIMPOSE, COL_COMBINE, COL_SAMPLES_RESTAKE,
                         COL_DUPLICATE_FILES, COL_TRANSFER, COL_REJECT_FILE_COUNT,
                         COL_REJECT_CLIENT_PAY, COL_REJECT_RETOUCHERS_PAY,
                         COL_RETOUCHERS_SIGN_1, COL_DATE_DONE_RETOUCHERS_SIGN_1,
                         COL_RETOUCHERS_SIGN_2, COL_DATE_DONE_RETOUCHERS_SIGN_2,
                         COL_RETOUCHERS_SIGN_3, COL_DATE_DONE_RETOUCHERS_SIGN_3,
                         COL_RETOUCHERS_SIGN_4, COL_DATE_DONE_RETOUCHERS_SIGN_4,
                         COL_RETOUCHERS_SIGN_5, COL_DATE_DONE_RETOUCHERS_SIGN_5,
                         COL_PROJECT_NAME, COL_FILETAG, COL_JOBSHEET_FILEVERSION
                        ]


COLS_TO_LOWER_CASE = [COL_PHOTOGRAPHER_1, COL_PHOTOGRAPHER_2,
                      COL_PHOTOGRAPHER_3, COL_UNMERGE_START,
                      COL_PHOTOSTACKER_SIGN_1,
                       COL_PHOTOSTACKER_SIGN_2,
                      COL_RETOUCHERS_SIGN_1, COL_RETOUCHERS_SIGN_2,
                      COL_RETOUCHERS_SIGN_3,
                      COL_RETOUCHERS_SIGN_4, COL_RETOUCHERS_SIGN_5,
                       COL_REJECT_FILE_COUNT,
                      COL_REJECT_CLIENT_PAY, COL_REJECT_RETOUCHERS_PAY]

STRING_COLS = [COL_WARNINGS, COL_UNMERGE_START, COL_UNMERGE_END, COL_FILETAG]

INTEGER_COLS = [COL_RENAME, COL_ADJUST, COL_PHOTOSTACK, COL_PHOTOGRAPHY,
                COL_BESPOKE, COL_GROUPSHOT, COL_CAPPED, COL_PHOTOGRAPHY_TO_VARIANCE, COL_VARIANCE,
                COL_SUPERIMPOSE, COL_COMBINE, COL_SAMPLES_RESTAKE, COL_DUPLICATE_FILES,
                COL_TRANSFER]

DATE_COLS = [COL_PHOTOGRAPHER_DATE, COL_PHOTOSTACKER_DATE_1,
             COL_PHOTOSTACKER_DATE_2,
             COL_DATE_DONE_RETOUCHERS_SIGN_1, COL_DATE_DONE_RETOUCHERS_SIGN_2,
             COL_DATE_DONE_RETOUCHERS_SIGN_3,
             COL_DATE_DONE_RETOUCHERS_SIGN_4, COL_DATE_DONE_RETOUCHERS_SIGN_5
        ]

# old column in older jobsheets and their new names
BACKWARD_COLUMN_COMPATIBILITY = {
    "Unmerge (Start)": COL_UNMERGE_START,
    "Unmerge (End)": COL_UNMERGE_END,
    "Samples & Reshoot": COL_SAMPLES_RESTAKE,
}

INPUT_UI_FILENAME = 'KPI_Calculator.xlsm'
AGGREGATED_DATASHEET_OUTPUT_FILENAME = 'aggregated_datasheet_files.xlsx'
INTERMEDIATE_OUTPUT_FILENAME = 'kpi_output_python_intermediate.xlsx'

STAFF_EXPECTED_KPI_SHEETNAME = 'Staff_Expected_KPI'
CATEGORY_PPJ_SHEETNAME = "Define_Category_PPJ"
DATE_INPUT_SHEETNAME = "date_input_hidden_py"
OUTPUT_SHEETNAMES = "sheet_names_hidden_py"

PYTHON_CODES_FOLDER_NAME = 'python_program'
DATESHEETS_FOLDER_NAME = 'Datasheet'
ARCHIVE_FOLDER_NAME = '_Archive'
INTERMEDIATE_FOLDER_NAME = 'intermediate'

PYTHON_ERRORS_FILENAME = "python_errors.txt"
PYTHON_WARNINGS_FILENAME = "python_warnings.txt"

START_ROW_EXCEL_OUTPUT = 5
START_COL_EXCEL_OUTPUT = 0

