import logging
from colorama import Fore, Back, Style
import click
import click_config_file
from ckanapi import RemoteCKAN
from ckanapi import errors
import json
import sys
import os
import glob
from datetime import datetime
from time import perf_counter
import shutil
import dateparser
from jsonschema import validate
import re
from collections import Counter

version = '0.1'
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
counter = {}

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


@click.group()
@click.option('--host',
              required=True,
              help='TWDH CKAN host.')
@click.option('--apikey',
              required=True,
              help='TWDH CKAN api key to use.')
@click.option('--test-run',
              is_flag=True,
              help='Show what would change but don\'t make any changes.')
@click.option('--verbose',
              is_flag=True,
              help='Show more information while processing.')
@click.option('--debug',
              is_flag=True,
              help='Show debugging messages.')
@click.option('--logfile',
              type=click.Path(),
              default='./twdhcli.log',
              show_default=True,
              help='The full path of the main log file.')
@click_config_file.configuration_option(config_file_name='twdhcli.ini')
@click.version_option(version)
@click.pass_context
def twdhcli(ctx, host, apikey, test_run, verbose, debug, logfile):
    """\b
       __               ____         ___
      / /__      ______/ / /_  _____/ (_)
     / __/ | /| / / __  / __ \/ ___/ / /
    / /_ | |/ |/ / /_/ / / / / /__/ / /
    \__/ |__/|__/\__,_/_/ /_/\___/_/_/

    TWDH-specific CKAN maintenance commands 
    """

    logger = setup_logger('mainlogger', logfile,
                          logging.DEBUG if debug else logging.INFO)

    def logecho(message, level='info'):
        """helper for logging to file and console"""
        if level == 'error':
            logger.error(message)
            click.echo(Fore.RED + level.upper() + ': ' + Fore.WHITE +
                       message, err=True) if verbose else False
        elif level == 'warning':
            logger.warning(message)
            click.echo(Fore.YELLOW + level.upper() + ': ' +
                       Fore.WHITE + message) if verbose else False
        elif level == 'debug':
            logger.debug(message)
            click.echo(Fore.GREEN + level.upper() + ': ' +
                       Fore.WHITE + message) if debug else False
        else:
            logger.info(message)
            click.echo(Fore.GREEN + message)

    # twdhcli func main
    logecho('Starting twdhcli/%s ...' % version)

    # log into CKAN
    try:
        portal = RemoteCKAN(host, apikey=apikey,
                            user_agent='twdhcli/' + version)
    except:
        logecho('Cannot connect to host %s' %
                host, level='error')
        sys.exit()
    else:
        logecho('Connected to host %s' % host)

    ctx.obj['logecho'] = logecho
    ctx.obj['test_run'] = test_run
    ctx.obj['portal'] = portal

    #logecho('Ending twdhcli...')


@twdhcli.command()
@click.pass_context
def update_dates(ctx):
    """
    Update dates on datasets where update_type == 'automatic'
    """

    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']
    portal = ctx.obj['portal']

    logecho( 'Running update-dates command ... ' )

    if test_run:
        logecho( '!!! --test-run enabled: no data will be updated' )

    datasets = portal.action.package_search(q='update_type:automatic')

    dom = datetime.now().day

    # types of updates
    update_daily = ['real-time','15 minutes or less','hourly','monthly']
    update_after_second = ['daily']
    update_after_seventh = ['weekly']
    update_quarterly = ['quarterly']
    update_semi_annually = ['every 6 months']
    update_annually = ['annually']

    counter['datasets'] = 0
    counter['updates'] = 0
    counter['skips'] = 0
    counter['failures'] = 0
    for dataset in datasets['results']:
        counter['datasets'] += 1

        logecho( '>>> {}: {}: {}'.format( dataset['title'], 
            dataset['update_type'], 
            dataset['update_frequency'] ) )

        if not 'date_range' in dataset or dataset['date_range'] == '':
            logecho( '      > no date range, skipping' )
            counter['skips'] += 1

        elif ( dataset['update_frequency'] in update_daily ) \
            or ( dataset['update_frequency'] in update_quarterly ) \
            or ( dataset['update_frequency'] in update_semi_annually ) \
            or ( dataset['update_frequency'] in update_annually ) \
            or ( dataset['update_frequency'] in update_after_second and dom > 2 ) \
            or ( dataset['update_frequency'] in update_after_seventh and dom > 7 ):

            from_date = datetime.strptime( dataset['from_date'], '%Y-%m-%d %H:%M:%S')
            now = datetime.now().replace(hour=0, minute=0, second=0, microsecond = 0)

            # if update_frequency is monthly, we want to set the new_to_date to _last_ month instead of _now_
            if dataset['update_frequency'] == 'monthly':
                # If we're in January, decrement year by one, else use current year
                new_to_year = now.year if now.month > 1 else now.year - 1
                # If we're in January, set the month to december, else set to now.month-1
                new_to_month =  now.month - 1 if now.month > 1 else 12
                new_to_date = datetime( new_to_year, new_to_month, 1, 0, 0 )

            elif dataset['update_frequency'] == 'quarterly':
                # If we're before April, decrement year by one, else use current year
                new_to_year = now.year if now.month > 3 else now.year - 1
                # If we're before April, set the month to now.month+12-3, else set to now.month-3
                new_to_month =  now.month - 3 if now.month > 3 else now.month + 12 - 3
                new_to_date = datetime( new_to_year, new_to_month, 1, 0, 0 )

            elif dataset['update_frequency'] == 'every 6 months':
                # If we're before July, decrement year by one, else use current year
                new_to_year = now.year if now.month > 3 else now.year - 1
                # If we're before July, set the month to now.month+12-6, else set to now.month-6
                new_to_month =  now.month - 6 if now.month > 6 else now.month + 12 - 6
                new_to_date = datetime( new_to_year, new_to_month, 1, 0, 0 )

            elif dataset['update_frequency'] == 'annually':
                new_to_year = now.year - 1
                new_to_date = datetime( new_to_year, now.month, 1, 0, 0 )

            else:
                new_to_date = now
                
            new_drange = '{} - {}'.format( from_date.strftime('%m/%d/%Y'), new_to_date.strftime('%m/%d/%Y'))
            logecho( '    > {} ==> {}'.format( dataset['date_range'], new_drange ), level='info' )

            if new_drange == dataset['date_range']: 
                logecho( '      > date already correct, skipping update' )
                counter['skips'] += 1
            elif from_date > new_to_date: 
                logecho( '      > WARNING: from_date would be greater than to_date, skipping update' )
                counter['skips'] += 1
            else:
                if test_run:
                    logecho( '      > --test-run enabled, updates not applied!' )
                else:
                    try:
                        results = portal.action.package_revise(
                            match = {'id': dataset['id']},
                            update = {'date_range': new_drange}
                        )
                        logecho( '      > Update complete' )
                        counter['updates'] += 1

                    except NotAuthorized:
                        counter['failures'] += 1
                        logecho( '      > Authorization failure: update not completed' )
        else:
            logecho( '      > Not in need of update' )
            counter['skips'] += 1


    logecho('=== Dataset date updates complete')
    if test_run:
        logecho( '!!! --test-run enabled: no data was updated' )
    click.echo('    {} dataset{} updated'.format( counter['updates'], 's' if counter['updates'] != 1 else '' ))
    click.echo('    {} dataset{} skipped'.format( counter['skips'], 's' if counter['skips'] != 1 else '' ))
    if counter['failures'] > 0:
        click.echo('    {} dataset{} unauthorized'.format( counter['failures'], 's' if counter['failures'] != 1 else '' ))
    click.echo('    {} dataset{} checked'.format( counter['datasets'], 's' if counter['datasets'] != 1 else '' ))


@twdhcli.command()
@click.argument('dataset_id')
@click.pass_context
def dataset_undelete(ctx, dataset_id):
    """
    Changes a deleted datasets status from 'deleted' to 'active'

    """

    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']
    portal = ctx.obj['portal']

    logecho( 'Running dataset-undelete command for dataset with id "{}"... '.format( dataset_id) )

    if test_run:
        logecho( '!!! --test-run enabled: no data will be updated' )

    try:
        dataset = portal.action.package_show(id=dataset_id)

    #except Exception as err:
    except:
        logecho( 'ERROR: Dataset with id "{}" not found'.format( dataset_id ) )
        #print(f"Unexpected {err=}, {type(err)=}")
        return False

    #except:
    #    logecho( 'ERROR: Dataset with id "{}" not found'.format( dataset_id ) )

    # click.echo( dataset )


    if test_run:
        logecho( '> --test-run enabled, updates not applied!' )
    else:
        try:                
            results = portal.action.package_revise(
                match = {'id': dataset['id']},
                update = {'state': 'active'}
            )
            logecho( 'Dataset with id "{}" set to state "active"'.format( dataset_id ) )

        except Exception as err:
            logecho( 'ERROR: Dataset update not completed' )
            logecho(f"Unexpected {err=}, {type(err)=}")


    
    dataset = portal.action.package_show(id=dataset_id)
    click.echo( dataset['state'] )

if __name__ == '__main__':
    twdhcli(obj={},auto_envvar_prefix='TWDHCLI')
