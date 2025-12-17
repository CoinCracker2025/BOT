@echo off
rem --- run_dashboard.bat (improved) ---
rem Place ce fichier dans le même dossier que app.py

cd /d %~dp0

rem Try to activate a local venv if present
if exist "%~dp0venv\Scripts\activate.bat" (
  call "%~dp0venv\Scripts\activate.bat"
)

rem Optional: set port here if you need a different one
set STREAMLIT_PORT=8501

rem Ensure unbuffered output for logs
set PYTHONUNBUFFERED=1

rem Run Streamlit and log output to streamlit.log
echo Starting Streamlit (port %STREAMLIT_PORT%)...
python -m streamlit run app.py --server.port %STREAMLIT_PORT% --logger.level=debug > streamlit.log 2>&1

rem Open browser to the local app (optional)
start http://localhost:%STREAMLIT_PORT%

pause