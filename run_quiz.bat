@echo off

call create_venv.bat

call venv/Scripts.activate.bat

python -m main

pause