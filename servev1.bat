@echo off
rem servev1 启动脚本：需先激活 dog_voice 环境
cd /d %~dp0
python -u servev1.py
pause
