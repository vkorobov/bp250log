@echo off
path=c:\program files\python;%path%
c:
cd \bp250log
:start
python bp250log.py
echo "�訡��!!!"
echo %date% %time% error >> bp250log.log
ping myhost
goto start