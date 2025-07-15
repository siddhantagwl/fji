VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Sheet6"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = True
Private Sub Worksheet_FollowHyperlink(ByVal Target As Hyperlink)

    Dim clickedCell As Range
    Dim clickedRow As Long
    Dim destinationSheetName As String
    Dim detailsSheet As Worksheet

    ThisWorkbook.Save
    
    If GLOBALS_INITIALIZED = False Then
        If a_globals.init = False Then Exit Sub
    End If
    
    ' Get the cell where the hyperlink was clicked
    Set clickedCell = Target.Parent
    clickedRow = clickedCell.Row

    ' Get the value to filter (e.g., Photographer name)
    personToFilter = Me.Cells(clickedRow, 1).Value

    ' Determine the sheet name from SubAddress
    If InStr(1, Target.SubAddress, "Photographer", vbTextCompare) > 0 Then
        destinationSheetName = "Photographer"
        
    ElseIf InStr(1, Target.SubAddress, "Photostacker", vbTextCompare) > 0 Then
        destinationSheetName = "Photostacker"
        
    ElseIf InStr(1, Target.SubAddress, "Retoucher", vbTextCompare) > 0 Then
        destinationSheetName = "Retoucher"
        
    Else
        MsgBox "Unknown target sheet in hyperlink: " & Target.SubAddress, vbExclamation
        Exit Sub
    End If

    ' Set the target sheet
    Set detailsSheet = ThisWorkbook.Sheets(destinationSheetName)
    lRow = UTILS.get_last_row_in_col(detailsSheet, 1)
    Set tableRange = detailsSheet.Range("$A$" & START_ROW_DATA & ":$G$" & lRow)
    
    Call UTILS.show_all_data(detailsSheet)
    
    Call UTILS.FilterDetails(detailsSheet, tableRange, personToFilter)
    
End Sub
