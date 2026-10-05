@echo off
title "Big Data Pipeline - Glassmorphism Dashboard and API"
echo ================================================================
echo    Launching Big Data Glassmorphism Dashboard on Python 3.12
echo ================================================================
echo.
echo Opening Dashboard at: http://127.0.0.1:8000
echo Swagger UI available at: http://127.0.0.1:8000/docs
echo.
start http://127.0.0.1:8000
"C:\Users\USER\AppData\Local\Programs\Python\Python312\python.exe" -m uvicorn src.api:app --host 127.0.0.1 --port 8000 --reload
pause
