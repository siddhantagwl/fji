Attribute VB_Name = "UTILS"
Function get_OS()

    MY_OS = Application.OperatingSystem
    If InStr(1, MY_OS, "Windows") > 0 Then
        get_OS = "win"
    Else
        get_OS = "mac"
    End If
    
End Function

Function get_os_path(path_name) As String

    win_sep = "\"
    mac_sep = "/"
    
    If OS = "win" Then
        get_os_path = Replace(path_name, mac_sep, win_sep)
    Else
        get_os_path = Replace(path_name, win_sep, mac_sep)
    End If
    
End Function

Function get_dropbox_parent_folder(CURR_PATH, dest_sub_path, PATH_SEP)
    
    ' this returns an array of folders when splitted on "/"
    split_array_of_dest_folder = Split(dest_sub_path, PATH_SEP)
    
    ' now split Excel workbook path on 0th index string from above array (ex: FJi Dropbox)
    dropbox_parent_folder = Split(CURR_PATH, PATH_SEP & split_array_of_dest_folder(0))
    
    ' now return the 0th index from the array which will be the actual parent folder of dropbox
    get_dropbox_parent_folder = dropbox_parent_folder(0)

End Function

Function test_applescript_file()
    
    test_applescript_file = True
    
    pathForScript = "/Users/yourUserName/Library/Application Scripts/com.microsoft.Excel/"
    ScriptFileName = "macOSUtils.scpt"
    
    If OS = "mac" Then
        If UTILS.CheckAppleScriptTaskExcelScriptFile(ScriptFileName) = False Then
            msg = "Sorry " & ScriptFileName & " is not at the correct location." & vbNewLine & vbNewLine & _
                  "Please copy " & Chr(34) & ScriptFileName & Chr(34) & " to this directory: " & _
                  vbNewLine & vbNewLine & Chr(34) & pathForScript & Chr(34)
            MsgBox msg, vbCritical, "File Not Found"
            test_applescript_file = False
        End If
    End If

End Function

Function CheckAppleScriptTaskExcelScriptFile(ScriptFileName) As Boolean

    'Function to Check if the AppleScriptTask script file exists
    'Ron de Bruin : 6-March-2016
    Dim AppleScriptTaskFolder As String
    Dim TestStr As String

    AppleScriptTaskFolder = MacScript("return POSIX path of (path to desktop folder) as string")
    AppleScriptTaskFolder = Replace(AppleScriptTaskFolder, "/Desktop", "") & _
        "Library/Application Scripts/com.microsoft.Excel/"

    On Error Resume Next
    TestStr = Dir(AppleScriptTaskFolder & ScriptFileName, vbDirectory)
    On Error GoTo 0
    If TestStr = vbNullString Then
        CheckAppleScriptTaskExcelScriptFile = False
    Else
        CheckAppleScriptTaskExcelScriptFile = True
    End If
    
End Function

Function WorksheetExists(shtName, Optional wb As Workbook) As Boolean
    
    Dim sht As Worksheet

    If wb Is Nothing Then Set wb = ThisWorkbook
    
    On Error Resume Next
    Set sht = wb.Sheets(shtName)
    On Error GoTo 0
    
    WorksheetExists = Not sht Is Nothing
    
End Function

Function delete_worksheet(wb, sheetName)

    Application.DisplayAlerts = False
    wb.Sheets(sheetName).Delete
    Application.DisplayAlerts = True

End Function

Function get_last_row_in_col(ws, colNum)

    get_last_row_in_col = ws.Cells(ws.Rows.Count, colNum).End(xlUp).Row

End Function

Function RunPythonScript(runningDirectoryForPython, scriptFullPath)
    
    If OS = "mac" Then
        RunPythonScript = mac_os_run_python(runningDirectoryForPython, scriptFullPath)
    Else
        RunPythonScript = windows_run_python(runningDirectoryForPython, scriptFullPath)
    End If

End Function

'***********************************
'**
'**   Function Modified by Mohamed Kamel on 22/03/2024
'**   Requested by My on 15/032024, see https://www.dropbox.com/t/LuNVkMaSmvxKotDI
'**   Modified to properly manage produced when running Apple script task
'**
'***********************************

Function mac_os_run_python(runningDirectoryForPython, scriptFullPath)
    
On Error GoTo Oops
    
    If Right(runningDirectoryForPython, 1) <> PATH_SEP Then runningDirectoryForPython = runningDirectoryForPython & PATH_SEP

    python_interpreter_path = WS_DATE_INPUT.Range("python_interpreter_mac").Value
    
    argumentTopass = python_interpreter_path & "###" & runningDirectoryForPython & "###" & scriptFullPath
    Debug.Print argumentTopass
    ' argumentTopass = "test" for testing the calling
    returnVal = AppleScriptTask("macOSUtils.scpt", "mac_run_python", argumentTopass)
    
    Debug.Print "Returned from AppleScript: " & returnVal

    If InStr(returnVal, "Success ...") Then
        mac_os_run_python = 0
    Else
        'MsgBox "Failed to run python MAC OS"
        mac_os_run_python = 1
    End If
    
    Exit Function
    
Oops:
    If Dir(runningDirectoryForPython & "python_program/" & "python_errors.txt") <> "" Then
        mac_os_run_python = 1
    Else
        mac_os_run_python = 0
    End If
    
      
End Function


Function windows_run_python(runningDirectoryForPython, scriptFullPath)

    Dim WshShell As Object
    Set WshShell = VBA.CreateObject("WScript.Shell")
    
    Dim waitOnReturn As Boolean: waitOnReturn = True
    Dim windowStyle As Integer: windowStyle = 1
    
    Dim command As String
    Dim returnCode As Integer
    
    'runningDirectoryForPython = Chr(34) & runningDirectoryForPython & Chr(34)
    scriptFullPath = Chr(34) & scriptFullPath & Chr(34)
    
    python_exe = Chr(34) & WS_DATE_INPUT.Range("python_exe").Value & Chr(34)
    
    ' waits for the cmd or not ?
    is_wait_for_cmd = UCase(WS_DATE_INPUT.Range("show_python_cmd").Value)
    
    If is_wait_for_cmd = "Y" Then
        cmd_after_run = "/K"
    Else
        cmd_after_run = "/C"
    End If
    
    command = "cmd.exe " & cmd_after_run & " " & Chr(34) & python_exe & " " & scriptFullPath & Chr(34)
    Debug.Print command
    
    ChDir runningDirectoryForPython
    
    returnCode = WshShell.Run(command, windowStyle, waitOnReturn)
    
    windows_run_python = returnCode
    
End Function

Function validate_date(date_var)

    validate_date = IsDate(date_var)

End Function

Function FilterDetails(ws, rng, strToFilter)

    ws.Activate
    
    ' Apply filter
    On Error Resume Next
    rng.AutoFilter Field:=1, Criteria1:=strToFilter
    On Error GoTo 0

End Function

Function show_all_data(ws)

    On Error Resume Next
    If ws.AutoFilterMode Then ws.ShowAllData
    On Error GoTo 0
    
End Function
