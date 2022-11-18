@echo off
CALL ..\Scripts\activate
python twdhcli.py --config twdhcli.ini
deactivate
