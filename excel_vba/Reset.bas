Attribute VB_Name = "Reset"
Sub reset_excel_ui()

    If GLOBALS_INITIALIZED = False Then
        If a_globals.init = False Then Exit Sub
    End If
    
    PYTHON_ERRORS_RANGE.Clear
    PYTHON_WARNINGS_RANGE.Clear
    
    For Each sheetName In SHEETS_TO_DELETE
        
        If WorksheetExists(sheetName, ThisWorkbook) Then
            Call UTILS.delete_worksheet(ThisWorkbook, sheetName)
        End If
        
    Next sheetName
    
    '----------------------------------------------------------------------------------------
    
    For Each sheetName In SHEETS_TO_OVERWRITE
        
        With ThisWorkbook.Sheets(sheetName)
            .Range(RANGE_TO_CLEAR).Clear
            .Tab.ColorIndex = xlColorIndexNone
        End With
        
    Next sheetName

End Sub

