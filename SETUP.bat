@echo off
REM Sourcing Assistant — one-shot setup script
REM Run this on your PC: double-click or `cmd /c SETUP.bat`

setlocal
cd /d "%~dp0"

echo === [1/4] Installing Python deps ===
pip install -r app\requirements.txt

echo === [2/4] Building joined data (sales + buy sheets) ===
python data\build_joined.py
if errorlevel 1 goto err

echo === [3/4] Reordering ASINs by priority ===
python data\reorder_asins.py
if errorlevel 1 goto err

echo === [4/4] Fetching Keepa for all ASINs (long-running, ~hours) ===
echo This is rate-limited by Keepa tokens. The fetcher is RESUMABLE — kill it any time
echo with Ctrl+C and rerun this script to continue from where it left off.
python data\fetch_keepa.py

echo.
echo === Building winner profile ===
python profile\build_profile.py
if errorlevel 1 goto err

echo.
echo === All done ===
echo To launch the Streamlit app:    LAUNCH.bat
goto end

:err
echo Setup failed. Check the error above.
exit /b 1

:end
endlocal
