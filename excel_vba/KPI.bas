Attribute VB_Name = "KPI"
Sub main_run_kpi()
    
    ThisWorkbook.Save
    
    If GLOBALS_INITIALIZED = False Then
        If a_globals.init = False Then Exit Sub
    End If
    
    '---------------------------------------------------------------------
    
    If UTILS.validate_date(WS_DATE_INPUT.Range("start_date")) = False Then
        msg = "You have entered incorrect start date!"
        MsgBox msg, vbOKOnly + vbCritical
        Exit Sub
    End If
    
    If UTILS.validate_date(WS_DATE_INPUT.Range("end_date")) = False Then
        msg = "You have entered incorrect end date!"
        MsgBox msg, vbOKOnly + vbCritical
        Exit Sub
    End If
    
    '---------------------------------------------------------------------
    
    Call Reset.reset_excel_ui
    
    '---------------------------------------------------------------------
    
    KPI_FOLDER_PATH = UTILS.get_os_path(KPI_FOLDER_PATH)
    dropbox_root = UTILS.get_dropbox_parent_folder(CURR_PATH, KPI_FOLDER_PATH, PATH_SEP)
    
    pythonScriptFullPath = dropbox_root & PATH_SEP & PYTHON_SCRIPT_PATH
    pythonScriptFullPath = UTILS.get_os_path(pythonScriptFullPath)
    
    returnCode = UTILS.RunPythonScript(CURR_PATH, pythonScriptFullPath)
    
    '---------------------------------------------------------------------
    
    'Call LoadPythonErrors(PYTHON_WARNINGS_TXT_FILEPATH, "B36")
    
    
    ' check for any errors in python program and notify it to the user
        
    If returnCode <> 0 Then
        'Call display_python_errors
        'LoadPythonErrors PYTHON_ERRORS_TXT_FILEPATH, "B18"
        'LoadPythonErrors PYTHON_WARNINGS_TXT_FILEPATH, "B36"

        
        Call LoadPythonErrors(PYTHON_ERRORS_TXT_FILEPATH, "B18")

        
        MsgBox "Found some errors in python program. Check the errors and try again.", vbCritical + vbOKOnly
        Exit Sub
    End If
    
    '---------------------------------------------------------------------
    
    PYTHON_OUTPUT_FILEPATH = UTILS.get_os_path(PYTHON_OUTPUT_FILEPATH)
    
    'Application.ScreenUpdating = False
    
    Workbooks.Open PYTHON_OUTPUT_FILEPATH
    
    Set wb = ActiveWorkbook
    'Application.ScreenUpdating = True
    For i = 1 To wb.Worksheets.Count
        
        Set ws = wb.Worksheets(i)
        ws_name = ws.Name
        ws.Tab.Color = 5296274
        
        If (ws_name = OVERALL_SUMMARY_SHEETNAME) Or (ws_name = PHOTOGRAPHY_SUMMARY_SHEETNAME) Then
            
            move_sheet = False
        
        Else
            
            move_sheet = True
            If UTILS.WorksheetExists(ws_name, ThisWorkbook) Then
                Call UTILS.delete_worksheet(ThisWorkbook, ws_name)
            End If
        
        End If
        
        ' apply autofilter by default - this will auto fit columns properly,
        ' and take the arrow in account while fitting
        If (i > 1) And (i < wb.Worksheets.Count) Then
            On Error Resume Next
            ws.Range("A" & START_ROW_DATA & ":G" & START_ROW_DATA).AutoFilter
            On Error GoTo 0
        End If
        
        Call formatting(ws)
        
        ' copy the sheet from intemediate file to the main Excel
        If move_sheet Then
        
            ws.Copy after:=ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count)
            
        Else
            ' here we are just copying the contents of the sheet rather than moving the sheet
            
            ThisWorkbook.Sheets(ws_name).Range(RANGE_TO_CLEAR).Clear
            
            lRow = UTILS.get_last_row_in_col(ws, 1)
            ws.Range("A1:G" & lRow).Copy ThisWorkbook.Sheets(ws_name).Range("A1")
            
            ThisWorkbook.Sheets(ws_name).Tab.Color = 5296274
            
            If (i > 1) And (i < wb.Worksheets.Count) Then
                On Error Resume Next
                ws.Range("A" & START_ROW_DATA & ":G" & START_ROW_DATA).AutoFilter
                On Error GoTo 0
            End If
        
        End If
        
    Next i
    
    wb.Close False
    
    ' hyperlink the breakdown column values to individual sheets
    Set ws_tables_rows = ThisWorkbook.Sheets("table_rows_map")
    Set ws_overall_kpi = ThisWorkbook.Sheets(OVERALL_SUMMARY_SHEETNAME)
    Set ws_project_summ = ThisWorkbook.Sheets(PHOTOGRAPHY_SUMMARY_SHEETNAME)
    
    ws_overall_kpi.Activate
    
    For i = 2 To 4
    
        table_name = ws_tables_rows.Cells(i, 1).Value
        start_row = ws_tables_rows.Cells(i, 2).Value
        end_row = ws_tables_rows.Cells(i, 3).Value
        
        For j = start_row + 1 To end_row
            ws_overall_kpi.Hyperlinks.Add Range("F" & j), Address:="", SubAddress:="'" & table_name & "'!A1"
        Next j
        
    Next i
    
    ' apply hyperlinks to summary sheet
    ws_project_summ.Activate
    
    lRow = UTILS.get_last_row_in_col(ws_project_summ, 1)
    
    For i = START_ROW_DATA + 1 To lRow
        ws_project_summ.Hyperlinks.Add Range("E" & i), Address:="", SubAddress:="'Photography_summary_breakdown'!A1"
        
        If (i Mod 2) = 1 Then
            ws_project_summ.Range("A" & i & ":E" & i).Interior.Color = RGB(242, 242, 242)
        End If
        
    Next i
    
    Call UTILS.delete_worksheet(ThisWorkbook, "table_rows_map")
    
    If WS_DATE_INPUT.Range("include_overall").Value = "Y" Then
        ws_overall_kpi.Range("A1:AA4").Interior.Color = RGB(244, 176, 132)
    End If
    
    ws_overall_kpi.Activate
    
    Application.ScreenUpdating = True
    
End Sub

Function formatting(ws)
    
    If ws Is Nothing Then
        Exit Function
    End If
    
    ws.Activate
    
    ActiveWindow.DisplayGridlines = False
    
    If ws.Name = YP_RATING_SHEETNAME Then
    
        With ws.Range("A1:B5").Font
            .Bold = True
            .Size = 16
        End With
        
        ws.Columns("B:Z").ColumnWidth = 16.2
        
        lRow = UTILS.get_last_row_in_col(ws, 1)
        
        ' center and vertical align
         With ws.Range("B8:Z" & lRow)
            .HorizontalAlignment = xlCenter
            .VerticalAlignment = xlCenter
        End With
        
        ' string to find - only highligh who have achieved the rating of 5
        fnd = "[5]"
        Call HighlightFindValues(ws, fnd)
        
    Else
    
        With ws.Columns("B:G")
                .HorizontalAlignment = xlCenter
                .VerticalAlignment = xlCenter
        End With
        
        With ws.Range("A1:B4").Font
            .Bold = True
            .Size = 16
        End With
        
        ws.Cells.EntireColumn.AutoFit
        
    End If
    
    ws.Columns("A").ColumnWidth = 22
    
End Function

Function HighlightFindValues(ws, findStrng)

    Dim FirstFound As String
    Dim FoundCell As Range, rng As Range
    Dim myRange As Range, LastCell As Range
    
    Set myRange = ws.UsedRange
    Set LastCell = myRange.Cells(myRange.Cells.Count)
    
    Set FoundCell = myRange.Find(what:=findStrng, after:=LastCell)
    
    'Test to see if anything was found
    If Not FoundCell Is Nothing Then
        FirstFound = FoundCell.Address
    Else
        GoTo NothingFound
    End If
    
    Set rng = FoundCell
    
    'Loop until cycled through all unique finds
    Do Until FoundCell Is Nothing
        'Find next cell with fnd value
        Set FoundCell = myRange.FindNext(after:=FoundCell)
        
        'Add found cell to rng range variable
        Set rng = Union(rng, FoundCell)
        
        'Test to see if cycled through to first found cell
        If FoundCell.Address = FirstFound Then Exit Do
          
    Loop
    
    'Highlight Found cells yellow
    rng.Interior.Color = RGB(216, 228, 188)
    
    Exit Function
    
    'Error Handler
NothingFound:
      Debug.Print "No values were found in this worksheet"

End Function

Private Function display_python_errors()

    Dim wbText As Workbook
    
    If Dir(PYTHON_ERRORS_TXT_FILEPATH) = "" Then Exit Function
    
    Set wbText = Workbooks.Open(PYTHON_ERRORS_TXT_FILEPATH)
    Set wsText = wbText.Sheets(1)
    
    lRow = UTILS.get_last_row_in_col(wsText, 1)
    
    With wsText.Range("A1:A" & lRow)
        .Font.Color = vbRed
        .Font.Bold = True
        .VerticalAlignment = xlCenter
        .Interior.Color = RGB(255, 255, 0)
        
        .Copy WS_DATE_INPUT.Cells(PYTHON_ERRORS_RANGE.Row, PYTHON_ERRORS_RANGE.Column)
    End With
    
    Application.CutCopyMode = False
    
    wbText.Close SaveChanges:=False

End Function

'***********************************
'**
'**   Function Added by Mohamed Kamel on 22/03/2024
'**   Requested by My on 15/032024, see https://www.dropbox.com/t/LuNVkMaSmvxKotDI
'**   This function reads errors stored in the file "filePath" and put them in the range "sheet_range"
'**
'***********************************

Function LoadPythonErrorssss(filePath, sheet_range)

    Dim myFile As String, Errors(15, 10) As String, Pos As Integer
    
    Pos = 0

    If Dir(filePath) <> "" Then

    Open filePath For Input As #1
        Do Until EOF(1)
            Line Input #1, textline
            Dim Row As Variant
            Row = Split(textline, "|||")
            
            For i = LBound(Row) To UBound(Row)
                Errors(Pos, i) = Row(i)
            Next i
            
            Pos = Pos + 1
        Loop
        Close #1
    End If
    
    Range(sheet_range).Resize(UBound(Errors, 1) + 1, UBound(Errors, 2) + 1) = Errors
    
End Function

Sub LoadPythonErrors(filePath As String, sheet_range As String)

    Dim textline As String
    Dim Row As Variant
    Dim Errors() As String
    Dim Pos As Long, colCount As Long
    Pos = 0

    If Dir(filePath) = "" Then
        MsgBox "File not found: " & filePath, vbExclamation
        Exit Sub
    End If

    ' Initial array with room for first row
    ReDim Errors(0 To 0, 0 To 0)

    Open filePath For Input As #1
        Do Until EOF(1)
            Line Input #1, textline
            Row = Split(textline, "|||")
            colCount = UBound(Row)

            ' Resize rows if needed
            If Pos > UBound(Errors, 1) Then
                ReDim Preserve Errors(0 To Pos, 0 To colCount)
            End If

            ' Resize columns if this row has more than before
            If colCount > UBound(Errors, 2) Then
                ReDim Preserve Errors(0 To Pos, 0 To colCount)
            End If

            ' Store row data
            Dim i As Long
            For i = 0 To colCount
                Errors(Pos, i) = Row(i)
            Next i

            Pos = Pos + 1
        Loop
    Close #1

    ' Write to worksheet
    Range(sheet_range).Resize(UBound(Errors, 1) + 1, UBound(Errors, 2) + 1).Value = Errors

End Sub

