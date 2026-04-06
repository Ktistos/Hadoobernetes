@echo off
setlocal

:: ------------------------------------------------------------------ ::
:: Must run as Administrator                                           ::
:: ------------------------------------------------------------------ ::
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This script must be run as Administrator.
    echo Right-click the file and select "Run as administrator".
    pause
    exit /b 1
)

:: ------------------------------------------------------------------ ::
:: Configuration                                                       ::
:: ------------------------------------------------------------------ ::
set MINIKUBE_IP=192.168.49.2
set HOSTS_FILE=C:\Windows\System32\drivers\etc\hosts
set HOSTS_ENTRY=%MINIKUBE_IP% kc.minikube.local minio.minikube.local minio-console.minikube.local
set WSL_CONF=/etc/wsl.conf

:: ------------------------------------------------------------------ ::
:: Add host entries if not already present                             ::
:: ------------------------------------------------------------------ ::
echo.
echo =^> Configuring Windows hosts file...

findstr /c:"minio.minikube.local" "%HOSTS_FILE%" >nul 2>&1
if %errorlevel% equ 0 (
    echo    Entries already present, skipping.
) else (
    echo %HOSTS_ENTRY% >> "%HOSTS_FILE%"
    echo    Added: %HOSTS_ENTRY%
)

:: ------------------------------------------------------------------ ::
:: Configure WSL to stop overwriting /etc/hosts on restart            ::
:: ------------------------------------------------------------------ ::
echo.
echo =^> Configuring WSL to preserve /etc/hosts...

wsl -e bash -c "grep -q 'generateHosts' /etc/wsl.conf 2>/dev/null && echo skip || (echo -e '\n[network]\ngenerateHosts=false' | sudo tee -a /etc/wsl.conf > /dev/null && echo done)"

echo.
echo =^> Done. Restart WSL for the wsl.conf change to take effect:
echo    wsl --shutdown
echo.
pause
endlocal
