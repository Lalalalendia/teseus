@echo off
cd /d "%~dp0"
python apply_e00_fixture_scope_fix.py
if errorlevel 1 pause
