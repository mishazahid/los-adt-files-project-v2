@echo off
REM Batch script to set up virtual environment for the project

echo ============================================================
echo Setting up Virtual Environment
echo ============================================================
echo.

REM Check if venv already exists
if exist "venv" (
    echo ⚠️  Virtual environment already exists!
    set /p recreate="Do you want to recreate it? (y/n): "
    if /i "%recreate%"=="y" (
        echo Removing existing virtual environment...
        rmdir /s /q venv
    ) else (
        echo Using existing virtual environment.
        echo.
        echo To activate it, run:
        echo   venv\Scripts\activate.bat
        exit /b
    )
)

echo Creating virtual environment...
python -m venv venv

echo.
echo ✅ Virtual environment created!
echo.
echo Next steps:
echo 1. Activate the virtual environment:
echo    venv\Scripts\activate.bat
echo.
echo 2. Install dependencies:
echo    pip install -r requirements.txt
echo.
echo 3. Start the server:
echo    python start_server.py
echo.

pause

