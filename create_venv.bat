@echo off

IF EXIST venv (
echo venv already exists. If you want to re-load packages and venv, please delete venv.
call venv\Scripts\activate.bat
pause
) ELSE (
python -m venv
call venv\Scripts\activate.bat
python -m pip install -r requirements.txt --default-timeout=120
)