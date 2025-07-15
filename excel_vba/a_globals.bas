Attribute VB_Name = "a_globals"
Global CURR_PATH As String, PATH_SEP As String, OS As String
Global GLOBALS_INITIALIZED As Boolean
Global WS_DATE_INPUT As Worksheet
Global PYTHON_OUTPUT_FILEPATH As String, PYTHON_SCRIPT_PATH  As String
Global KPI_FOLDER_PATH  As String, PYTHON_ERRORS_TXT_FILEPATH As String, PYTHON_WARNINGS_TXT_FILEPATH As String
Global OVERALL_SUMMARY_SHEETNAME As String, PHOTOGRAPHY_SUMMARY_SHEETNAME As String
Global PHOTOGRAPHY_SUMMARY_BRKD_SHEETNAME As String, PHOTOGRAPHER_SHEETNAME As String
Global PHOTOSTACKER_SHEETNAME As String, RETOUCHER_SHEETNAME As String
Global YP_RATING_SHEETNAME As String
Global START_ROW_DATA As Integer
Global SHEETS_TO_DELETE As Variant, SHEETS_TO_OVERWRITE As Variant
Global RANGE_TO_CLEAR As String
Global PYTHON_ERRORS_RANGE As Range
Global PYTHON_WARNINGS_RANGE As Range

Function init()

    CURR_PATH = ThisWorkbook.Path
    PATH_SEP = Application.PathSeparator
    OS = UTILS.get_OS()
    
    'Check for AppleScriptTask script file
    If UTILS.test_applescript_file = False Then
        init = False
        GLOBALS_INITIALIZED = False
        Exit Function
    End If
    
    Set WS_DATE_INPUT = ThisWorkbook.Sheets("Date_input")
    
    'Original
    KPI_FOLDER_PATH = "FJI Dropbox\" & Split(CURR_PATH, "FJI Dropbox")(1)
    'KPI_FOLDER_PATH = "FJI Dropbox\_Programming\_KPI"
    
    PYTHON_OUTPUT_FILEPATH = CURR_PATH & "\python_program\intermediate\kpi_output_python_intermediate.xlsx"
    PYTHON_SCRIPT_PATH = KPI_FOLDER_PATH & "\python_program\KPI_aggregation.py"
    PYTHON_ERRORS_TXT_FILEPATH = CURR_PATH & "/python_program/python_errors.txt"
    PYTHON_WARNINGS_TXT_FILEPATH = CURR_PATH & "/python_program/python_warnings.txt"

    OVERALL_SUMMARY_SHEETNAME = Trim(WS_DATE_INPUT.Range("overall").Value)
    PHOTOGRAPHY_SUMMARY_SHEETNAME = Trim(WS_DATE_INPUT.Range("photg_summ").Value)
    PHOTOGRAPHY_SUMMARY_BRKD_SHEETNAME = Trim(WS_DATE_INPUT.Range("photg_summ_brk").Value)
    PHOTOGRAPHER_SHEETNAME = Trim(WS_DATE_INPUT.Range("photographer").Value)
    PHOTOSTACKER_SHEETNAME = Trim(WS_DATE_INPUT.Range("photostacker").Value)
    RETOUCHER_SHEETNAME = Trim(WS_DATE_INPUT.Range("retoucher").Value)
    YP_RATING_SHEETNAME = Trim(WS_DATE_INPUT.Range("yearly_performce_rating").Value)
    
    Set PYTHON_ERRORS_RANGE = WS_DATE_INPUT.Range("python_error_list_range")
    Set PYTHON_WARNINGS_RANGE = WS_DATE_INPUT.Range("python_warning_list_range")
    
    SHEETS_TO_DELETE = Array(PHOTOGRAPHER_SHEETNAME, PHOTOSTACKER_SHEETNAME, _
                                                RETOUCHER_SHEETNAME, PHOTOGRAPHY_SUMMARY_BRKD_SHEETNAME, _
                                                YP_RATING_SHEETNAME)
                                    
    SHEETS_TO_OVERWRITE = Array(OVERALL_SUMMARY_SHEETNAME, PHOTOGRAPHY_SUMMARY_SHEETNAME)
    
    RANGE_TO_CLEAR = "A1:AA2000"
    
    START_ROW_DATA = 6
    
    init = True
    GLOBALS_INITIALIZED = True
    
End Function
