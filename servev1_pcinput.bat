@echo off
rem PC 语音输入模式（松手即发）一键启动：显式调用 dog_voice 环境 python 并开启 --pc-input
rem 板子（ESP32）按键说话 -> 松手 -> 识别完成立即 Ctrl+V 粘贴到 PC 焦点窗口
rem （确认模式 --pc-input-confirm 需 ESP32 发送键配合，焊好固件后再用）
cd /d %~dp0
"F:/Users/lenovo/miniconda3/envs/dog_voice/python.exe" -u servev1.py --pc-input
pause
