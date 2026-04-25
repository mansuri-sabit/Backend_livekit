@echo off
echo Starting LiveKit Server (self-hosted, native)...
echo.
echo   URL:    ws://localhost:8280
echo   Key:    devkey
echo   Secret: secret
echo.
cd /d "%~dp0"
livekit-server.exe --dev --bind 0.0.0.0
