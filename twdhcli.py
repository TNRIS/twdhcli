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
from datetime import datetime, date
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
              required=False,
              help='TWDH CKAN api key to use if authentication is required.')
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
    ctx.obj['host'] = host

    #logecho('Ending twdhcli...')


@twdhcli.command()
@click.option('--filename',
              required=True,
              type=click.Path(),
              default='./data-dictionary-list-{}.json'.format( date.today().strftime('%Y-%m-%d') ),
              help='Filename to save data dictionary JSON into.')

@click.pass_context
def fetch_data_dictionaries(ctx, filename):
    """
    Fetch data dictionaries
    """

    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']
    portal = ctx.obj['portal']
    host = ctx.obj['host']

    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')
    click.echo( 'Fetching data dictionaries from {} '.format( host ) )
    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')

    query = portal.action.package_search(q='type:dataset', rows=1000)

    counter['datasets'] = 0
    counter['data-dictionaries'] = 0

    dds = []

    for dataset in query['results']:
       

        resources = dataset.get( 'resources' )

        # If the first resource has a DP+ created DD, use it
        if len( resources ) > 0 and resources[0].get( 'datastore_active', False ):
            logecho( '> {}'.format( dataset['title'], dataset['name'] ) )
            logecho( '  {}/dataset/{}'.format( host, dataset['name'] ) )
            logecho( '   {}'.format( resources[0].get('id') ) )

            resource = portal.action.datastore_search(resource_id=resources[0].get('id'))

            dd = {}
            dd['dataset'] = dataset['name'];
            dd['dataset_url'] = '{host}/dataset/{name}'.format( host=host, name=dataset['name'] )
            dd['resource_id'] = resource['resource_id'];
            dd['data_dictionary_edit_url'] = '{host}/dataset/{name}/dictionary/{id}'.format( host=host, name=dataset['name'], id=resource['resource_id'] )

            dd['fields'] = resource['fields'];
            dds.append( dd )

            counter['data-dictionaries'] += 1

        dict = {}
        dict['data-dictionaries'] = dds
        try:
            file = open(filename,mode='w')
            json.dump(dict, file, indent = 4)
        finally:
            file.close()

        counter['datasets'] += 1

    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')
    click.echo('{} dataset{} checked for DP+ data dictionaries'.format( counter['data-dictionaries'], 's' if counter['data-dictionaries'] != 1 else '' ))
    click.echo('DP+ Data Dictionaries fetched for {} resource{} and output to file {}'.format( counter['data-dictionaries'], 's' if counter['data-dictionaries'] != 1 else '', filename ))
    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')


@twdhcli.command()
@click.option('--filename',
              required=True,
              type=click.Path(),
              default='./taglist.json',
              help='Filename to save tag JSON into.')

@click.pass_context
def fetch_tags(ctx, filename):
    """
    Fetch dataset tag values
    """

    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']
    portal = ctx.obj['portal']
    host = ctx.obj['host']

    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')
    click.echo( 'Fetching tag values from {} '.format( host ) )
    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')

    query = portal.action.package_search(q='type:dataset', rows=1000)

    counter['datasets'] = 0
    tags = []

    for dataset in query['results']:
       
        logecho( '> {}'.format( dataset['title'], dataset['name'] ) )
        logecho( '  {}dataset/{}'.format( host, dataset['name'] ) )

        if 'primary_tags' not in dataset:
            dataset['primary_tags'] = []


        if 'secondary_tags' not in dataset:
            dataset['secondary_tags'] = []


        if 'tags' not in dataset:
            dataset['tags'] = []

        if( len( dataset['primary_tags'] ) == 0 ): click.echo( '   No primary tags set!' )
        if( len( dataset['secondary_tags'] ) == 0 ): click.echo( '   No secondary tags set!' )
        if( len( dataset['tags'] ) == 0 ): click.echo( '   No native CKAN tags set!' )

        #logecho( '  > {}'.format( dataset['primary_tags'] ) )
        #logecho( '  > {}'.format( dataset['secondary_tags'] ) )
        #logecho( '  > {}'.format( dataset['tags'] ) )

        taginfo = {}
        taginfo['name'] = dataset['name']
        taginfo['primary_tags'] = dataset['primary_tags']
        taginfo['secondary_tags'] = dataset['secondary_tags']

        taginfo['tags'] = []
        for tag in dataset['tags']:
            taginfo['tags'].append( tag['name'] )
       
        tags.append( taginfo )

        counter['datasets'] += 1

    dict = {}
    dict['datasets'] = tags

    with open(filename, 'w') as fp:
        json.dump(dict, fp, indent = 4)

    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')
    click.echo('Tags fetched for {} dataset{} and output to file taglist.json'.format( counter['datasets'], 's' if counter['datasets'] != 1 else '' ))
    click.echo('-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-')

@twdhcli.command()
@click.argument('tags_json')
@click.pass_context
def update_tags(ctx, tags_json):
    """
    Update tags using data in tags_json 
    """

    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']
    portal = ctx.obj['portal']

    logecho( 'Running update-tags command ... ' )

    if test_run:
        logecho( '!!! --test-run enabled: no data will be updated' )

    f = open( tags_json )
    tags = json.load(f)

    counter['datasets'] = 0
    counter['updates'] = 0
    counter['skips'] = 0
    counter['not-found'] = 0
    counter['failures'] = 0

    for dataset_tags in tags['datasets']:

        try:
            dataset = portal.action.package_show(id='{}'.format( dataset_tags['name'] ) )

        except Exception as e:
            counter['not-found'] += 1
            logecho( '      > Dataset not found: {}'.format( dataset_tags['name'] ) )
            continue

        counter['datasets'] += 1

        logecho( '>>> {} ( {} )'.format( dataset['title'], dataset['name'] ) )
        if 'primary_tags' in dataset_tags:
            logecho( '  > primary_tags: {}'.format( dataset_tags['primary_tags'] ) )
        else:
            dataset_tags['primary_tags'] = []
            logecho( '  > NO primary_tags FIELD PRESENT!' )
        if 'secondary_tags' in dataset_tags:
            logecho( '  > secondary_tags: {}'.format( dataset_tags['secondary_tags'] ) )
        else:
            dataset_tags['secondary_tags'] = []
            logecho( '  > NO primary_tags FIELD PRESENT!' )

        dataset_tags['tags'] = dataset_tags['primary_tags'] + dataset_tags['secondary_tags'] 
        logecho( '  > tags: {}'.format( dataset_tags['tags'] ) )

        if test_run:
            logecho( '      > --test-run enabled, updates not applied!' )
        else:
            tdict = {
                'primary_tags': dataset_tags['primary_tags'],
                'secondary_tags': dataset_tags['secondary_tags'],
            }

            try:

                results = portal.action.package_revise(
                    match = {'id': dataset['id']},
                    update = {
                        'primary_tags': dataset_tags['primary_tags'],
                        'secondary_tags': dataset_tags['secondary_tags']
                    }
                )

                logecho( '      > Update complete' )
                counter['updates'] += 1

            except Exception as e:
                counter['failures'] += 1
                logecho( '      > Authorization failure: update not completed' )
                #logecho( e )

    logecho('=== Dataset tag updates complete')
    if test_run:
        logecho( '!!! --test-run enabled: no data was updated' )
    click.echo('    {} dataset{} updated'.format( counter['updates'], 's' if counter['updates'] != 1 else '' ))
    click.echo('    {} dataset{} skipped'.format( counter['skips'], 's' if counter['skips'] != 1 else '' ))
    if counter['failures'] > 0:
        click.echo('    {} dataset{} unauthorized'.format( counter['failures'], 's' if counter['failures'] != 1 else '' ))
    if counter['not-found'] > 0:
        click.echo('    {} dataset{} not found'.format( counter['failures'], 's' if counter['failures'] != 1 else '' ))

@twdhcli.command()
@click.pass_context
def update_dates(ctx):
    """
    Update dates on datasets 'update_type' of 'automatic'
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
    Changes a dataset status from 'deleted' to 'active'

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


@twdhcli.command()
@click.argument('json_file')
@click.pass_context
def update_rolling_dates(ctx, json_file):
    """
    Update rolling dates using data in json_file 

    This still needs a lot ow polishing
    """

    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']
    portal = ctx.obj['portal']

    logecho( 'Running update-rolling-dates command ... ' )

    if test_run:
        logecho( '!!! --test-run enabled: no data will be updated' )

    f = open( json_file )
    dates = json.load(f)

    counter['datasets'] = 0
    counter['updates'] = 0
    counter['skips'] = 0
    counter['not-found'] = 0
    counter['failures'] = 0

    for updates in dates['datasets']:

        try:
            dataset = portal.action.package_show(id='{}'.format( updates['name'] ) )

        except Exception as e:
            counter['not-found'] += 1
            logecho( e )
            logecho( '      > Dataset not found: {}'.format( updates['name'] ) )
            continue

        counter['datasets'] += 1

        logecho( '>>> {} - {}'.format( dataset['title'], dataset['name'] ) )

        if test_run:
            logecho( '      > --test-run enabled, updates not applied!' )
        else:

            try:
                from_date = datetime.strptime( updates['from_date'], '%Y-%m-%d')
                to_date = datetime.strptime( updates['to_date'], '%Y-%m-%d')
                new_drange = '{} - {}'.format( from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

                results = portal.action.package_revise(
                    match = {'id': dataset['id']},
                    update = {
                        'date_range': new_drange,
                        'from_date': updates['from_date'],
                        'to_date': updates['to_date'],
                        'end_date': ''
                    }
                )

                logecho( '      > Update complete' )
                counter['updates'] += 1

            except Exception as e:
                counter['failures'] += 1
                logecho( '      > Authorization failure: update not completed' )
                logecho( e )

    logecho('=== Dataset rolling date updates complete')
    if test_run:
        logecho( '!!! --test-run enabled: no data was updated' )
    click.echo('    {} dataset{} updated'.format( counter['updates'], 's' if counter['updates'] != 1 else '' ))
    click.echo('    {} dataset{} skipped'.format( counter['skips'], 's' if counter['skips'] != 1 else '' ))
    if counter['failures'] > 0:
        click.echo('    {} dataset{} unauthorized'.format( counter['failures'], 's' if counter['failures'] != 1 else '' ))
    if counter['not-found'] > 0:
        click.echo('    {} dataset{} not found'.format( counter['failures'], 's' if counter['failures'] != 1 else '' ))




if __name__ == '__main__':
    twdhcli(obj={},auto_envvar_prefix='TWDHCLI')
