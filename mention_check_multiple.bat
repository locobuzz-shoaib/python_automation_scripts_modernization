@echo off
setlocal EnableDelayedExpansion

rem Set the working directory
cd /d "C:\Users\MSI\Desktop\workspace\python_automation_script"

rem Activate the virtual environment
call ".venv\Scripts\activate.bat"

rem Initialize default values
set mode=offline
set date_check=
set python_args=
set arglist=

rem Loop through all arguments to extract mode and date_check
:ProcessArgs
if "%1"=="" goto ProcessSocialIDs

if "%1"=="--mode" (
    set mode=%2
    shift
    shift
    goto ProcessArgs
)

if "%1"=="--date_check" (
    set date_check=%2
    shift
    shift
    goto ProcessArgs
)

rem Collect all remaining arguments as social_id
set arglist=%arglist% %1
shift
goto ProcessArgs

:ProcessSocialIDs
rem Prepare the arguments to be passed based on optional parameters
set python_args=--mode %mode%
if not "%date_check%"=="" (
    set python_args=%python_args% --date_check %date_check%
)

rem Loop through all remaining social_id arguments and process each
for %%i in (%arglist%) do (
    echo Processing social_id: %%i
    python "mention_created_based_on_social_id.py" %%i %python_args%
)

rem Deactivate the virtual environment
call deactivate
endlocal
