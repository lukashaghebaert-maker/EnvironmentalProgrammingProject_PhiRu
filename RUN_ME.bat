@echo off
title Environmental Project GUI Launcher
echo Starting the Python environment...

:: 1. Initialize Anaconda
call %USERPROFILE%\anaconda3\Scripts\activate.bat

:: 2. Activate your specific project environment
call conda activate PhiRuProject

:: 3. Navigate to the root folder (where this .bat is located)
:: %~dp0 automatically finds the path to the current folder , /d strips the file and keeps the directory
cd /d "%~dp0"

:: 4. Run the GUI using its relative path
python "Python_script\GUI.py"

echo.
echo Application closed.
pause