@echo off
for /l %%i in (1,1,%1) do call :body %%i
exit /b

:body
setlocal
set PEERADDRBASE=7055
set FILEPATHBASE=%appdata%\hyperledger01

set /a ADDRPORT=%PEERADDRBASE%+%1*100
set CORE_PEER_LISTENADDRESS=127.0.0.1:%ADDRPORT%
set CORE_PEER_ADDRESS=%CORE_PEER_LISTENADDRESS%
set CORE_PEER_ID=billgates_%1
set CORE_PEER_FILESYSTEMPATH=%FILEPATHBASE%\txnet%1
timeout 1
start cmd /k txnetwork
rem start echo .
endlocal