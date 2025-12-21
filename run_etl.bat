@echo off

chcp 65001 > nul


set LOG=C:\sideProject\house_data_ETL\airflow_trigger.log

rem 用 powershell 取系統時間，避免 cmd 自己亂搞編碼

for /f %%i in ('powershell -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set NOW=%%i



echo ====== ETL START %NOW% ====== >> %LOG%



"C:\Users\hnchen\AppData\Local\Programs\Python\Python310\python.exe" "C:\sideProject\house_data_ETL\main.py" >> %LOG% 2>&1



for /f %%i in ('powershell -Command "Get-Date -Format yyyy-MM-dd_HH:mm:ss"') do set NOW=%%i



echo ====== ETL END %NOW% ====== >> %LOG%

echo. >> %LOG%



exit /b 0