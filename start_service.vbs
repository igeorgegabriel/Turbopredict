' TURBOPREDICT Background Service Starter
' Silent VBScript launcher - no console window
' Place shortcut to this file in Windows Startup folder for auto-start

Set WshShell = CreateObject("WScript.Shell")
Set FSO = CreateObject("Scripting.FileSystemObject")

' Get script directory
ScriptDir = FSO.GetParentFolderName(WScript.ScriptFullName)

' PowerShell command to run the service
PSScript = ScriptDir & "\scripts\scheduled_analysis.ps1"
Command = "powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & PSScript & """"

' Run hidden (0 = hidden, False = don't wait)
WshShell.Run Command, 0, False
