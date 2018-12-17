@echo off

for /l %%i in (1,1,%1) do call :invokebody %%i
timeout 1
peer network status
peer chaincode query -n txnetwork -c "{\"Function\": \"count\", \"Args\": []}"
peer chaincode query -n txnetwork -c "{\"Function\": \"status\", \"Args\": []}"

for /l %%i in (1,1,%2) do call :testbody %%i
goto :eof

:invokebody
peer chaincode invoke -n txnetwork -c "{\"Function\": \"invoke\", \"Args\": [\"aa\",\"%1\"]}"
goto :eof

:testbody
setlocal
set PEERLOCALADDRBASE=7051
set /a LOCADDRPORT=%PEERLOCALADDRBASE%+%1*100
set CORE_PEER_LOCALADDR=127.0.0.1:%LOCADDRPORT%
peer network status
peer chaincode query -n txnetwork -c "{\"Function\": \"count\", \"Args\": []}"
peer chaincode query -n txnetwork -c "{\"Function\": \"status\", \"Args\": []}"
endlocal

