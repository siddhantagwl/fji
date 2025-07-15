VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Sheet7"
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

    ' Get the value to filter
    dateToFilter = Me.Cells(clickedRow, 1).Value

    ' Force US-style date format (important even on Mac!)
    formattedDate = Format(dateToFilter, "m/d/yyyy")

    destinationSheetName = "Photography_summary_breakdown"

    ' Set the target sheet
    Set detailsSheet = ThisWorkbook.Sheets(destinationSheetName)
    lRow = UTILS.get_last_row_in_col(detailsSheet, 1)
    Set tableRange = detailsSheet.Range("$A$" & START_ROW_DATA & ":$G$" & lRow)
    
    Call UTILS.show_all_data(detailsSheet)
    
    detailsSheet.Activate
    
    'Debug.Print Format(detailsSheet.Range("A7").Value, "yyyy-mm-dd"), IsDate(detailsSheet.Range("A7").Value)

    On Error Resume Next
    tableRange.AutoFilter Field:=1, Operator:=xlFilterValues, Criteria2:=Array(2, dateToFilter)
    On Error GoTo 0
    
End Sub

