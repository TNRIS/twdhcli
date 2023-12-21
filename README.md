# twdhcli
         __               ____         ___ 
        / /__      ______/ / /_  _____/ (_)
       / __/ | /| / / __  / __ \/ ___/ / / 
      / /_ | |/ |/ / /_/ / / / / /__/ / /  
      \__/ |__/|__/\__,_/_/ /_/\___/_/_/   

Python script for running TWDH-specific maintenance commands

Requires: Python 3.8

Installation:
=============

Linux, Mac, Windows Powershell:
```
cd /usr/lib/ckan
python3 -m venv twdhclienv
. twdhclienv/bin/activate
cd twdhclienv
git clone git@github.com:twdbben/twdhcli.git
cd twdhcli
pip install -r requirements.txt
```

Usage:
======

Linux, Mac, Windows Powershell:
```
cd /usr/lib/ckan
. twdhclienv/bin/activate
cd twdhclienv/twdhcli
python twdhcli.py --config ckan.ini
```

Command line parameters:
------------------------

```
python twdhcli.py --help
Usage: twdhcli.py [OPTIONS]

Run TWDH-specific maintenance commands

Options:
  --host TEXT          TWDH CKAN host.  [required]
  --apikey TEXT        TWDH CKAN api key to use.  [required]
  --test-run           Show what would change but don't make any changes.
  --verbose            Show more information while processing.
  --debug              Show debugging messages.
  --logfile PATH       The full path of the main log file.  [default: ./twdhcli.log]
  --config FILE        Read configuration from FILE.
  --version            Show the version and exit.
  --help               Show this message and exit.

Commands:
  update-dates  Update dates on datasets where update_type == 'automatic'
```

Note that parameters are parsed and processed in priority order - through environment variables, a config file, or through the command line interface.

Environment variables should be all caps and prefixed with `TWDHCLI_`, for example:

```
export TWDHCLI_APIKEY="MYCKANAPIKEY"
export TWDHCLI_HOST="https://ckan.example.com"
```
