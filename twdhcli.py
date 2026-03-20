from __future__ import annotations
from colorama import init, Fore, Back, Style
import click
import click_config_file
import ckanapi
import requests
import os
import sys
import json
from dotenv import dotenv_values
from datetime import datetime, date
from time import perf_counter
import logging
import csv
from pathlib import Path
from urllib.parse import urlparse
import subprocess

import helpers as h

version = '0.11.0'

# Initialize Colorama with autoreset enabled
init(autoreset=True)

log = logging.getLogger(__name__)
FORMAT = '%(message)s'
#logging.basicConfig(format=FORMAT, level=logging.INFO)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def get_patch_functions():
    return  {
        'example': patch_fn_example,
        'clear_data_dictionary': patch_fn_clear_data_dictionary,
        'set_title': patch_fn_set_title,
        'set_app_email': patch_fn_set_app_email,
        'clear_spatial_data': patch_fn_clear_spatial_data,
        'clear_spatial_data_full': patch_fn_clear_spatial_data_full,
        'set_spatial_data': patch_fn_set_spatial_data,
        'fix_empty_date_ranges': patch_fn_fix_empty_date_ranges,
        'validate_datasets': patch_fn_validate_datasets,

    }

@click.group()
@click.option('--host',
              required=False,
              help='TWDH CKAN host, usually https://txwaterdatahub.org')
@click.option('--apikey',
              required=False,
              help='TWDH CKAN API key to use if authentication is required.')
@click.option('--test-run',
              is_flag=True,
              default=False,
              help='Show less information.')
@click.option('--quiet',
              is_flag=True,
              help='Show less information.')
@click.option('--debug',
              is_flag=True,
              help='Show debugging messages.')
@click.option('--logfile',
              type=click.Path(),
              default='./twdhcli.log',
              show_default=True,
              help='The full path of the main log file.')
@click.version_option(version)
@click.pass_context
def twdhcli(ctx, host, apikey, test_run, quiet, debug, logfile):
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
            click.echo(Fore.RED + '🔴 ' + 
                message, err=True) if not quiet else True
        elif level == 'warning':
            logger.warning(message)
            click.echo(Fore.YELLOW + '🟡 ' +
                Fore.WHITE + message) if not quiet else True
        elif level == 'debug':
            logger.debug(message)
            click.echo('🟢🟢 ' +
                Fore.WHITE + message) if debug else False
        elif level == 'note':
            logger.debug(message)
            click.echo('🟢 ' +
                Fore.GREEN + message) if not quiet else True
        elif level == 'detail':
            logger.debug(message)
            click.echo('🔵 ' +
                Fore.BLUE + message) if not quiet else True
        elif level == 'info':
            logger.debug(message)
            click.echo('⚪️ ' + Fore.WHITE + message) if not quiet else True
        elif level == 'exit':
            logger.debug(message)
            click.echo('⚫️ ' + Fore.WHITE + message) if not quiet else True
        elif level == 'celebration':
            logger.debug(message)
            click.echo('🎉 ' + Fore.MAGENTA + message) if not quiet else True
        elif level == 'divider':
            logger.debug(message)
            click.echo(Fore.MAGENTA + message) if not quiet else True
        else:
            logger.info(message)
            click.echo(Fore.GREEN + message) if not quiet else True

    logecho('Starting twdhcli/%s ...' % version, 'detail')

    if not os.path.exists("./.env"):
        logecho('.env file not found', level='warning')

    config = dotenv_values( ".env" )

    if apikey == None:
        # apikey not passed as a parameter, check config
        apikey = config.get("apikey",None) 
        if apikey == None:
            logecho("Cannot continue: --apikey parameter not set and APIKEY not found in .env.secrets","error")
            exit(1)
    logecho("apikey set", "detail")

    if host == None:
        # host not passed as a parameter, check config
        host = config.get("host",None) 
        if host == None:
            logecho("Cannot continue: --host parameter not set and TWDH_HOST not found in .env","error")
            exit(1)

    # log into CKAN
    try:
        twdh = ckanapi.RemoteCKAN(host, apikey=apikey,
                            user_agent='twdhcli/' + version)
    except Exception as e:
        logecho('Cannot connect to host %s' % host, level='error')
        sys.exit()
    else:
        logecho('Connected to host %s' % host, "detail")

    ctx.obj['twdh'] = twdh
    ctx.obj['logecho'] = logecho
    ctx.obj['test_run'] = test_run

@twdhcli.command()
@click.option('--dest',
              type=click.Path(),
              default='./twdh-snapshots',
              show_default=True,
              help='The full path of the CSV output file.')
@click.pass_context
def snapshot(ctx,dest):
    """
    Create JSON snapshot files for datasets, applications and organizations
    """
    h.snapshot(ctx,dest)


@twdhcli.command()
@click.option('--patch-fn',
              required=True,
              default=None,
              help='patch function to apply')
@click.option('--ids',
              required=False,
              default=None,
              help='list of dataset ids to patch')
@click.option('--patch-data',
              required=False,
              default=None,
              help='JSON blob containing patch values')
@click.option('--dataset-type',
              required=False,
              default="dataset",
              help='dataset or application')
@click.option('--confirm-each',
              default=False,
              is_flag=True,
              help='Confirm each patch operation instead of just once at the start')
@click.option('--skip-snapshot',
              default=False,
              is_flag=True,
              help='Don\'t prompt for snapshot, and don\'t create a snapshot')
@click.pass_context
def patch_datasets(ctx, patch_fn, ids, patch_data, dataset_type, confirm_each, skip_snapshot):
    """
    Patch datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    patch_fn_dict = get_patch_functions()

    if patch_fn not in patch_fn_dict:
        logecho( "Patch function does not exist: {}".format(patch_fn), "info" )
        return

    if not skip_snapshot and click.confirm('🟢 Take a snapshot before running patches?', default=True):
        h.snapshot( ctx, './twdh-snapshots' )
    else:
        logecho( "Skipped snapshot!", "warning" )

    datasets = h.fetch_datasets(ctx, ids, dataset_type)

    # Confirm patch operation
    if ids:
        logecho( "Prepared to patch the following datasets", 'warning')
        for dataset in datasets:
            logecho( "- {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
        if not confirm_each:
            if click.confirm('🟢 Proceed with all patches?'):
                logecho( "Proceeding with patches ...", "info" )
            else: 
                logecho( "Operation cancelled", "exit" )
                sys.exit(0)
    else:
        if not confirm_each:
            if click.confirm('🟢 Proceed with patching {} {}s?'.format(len(datasets), dataset_type)):
                logecho( "Proceeding with patches ...", "info" )
            else: 
                logecho( "Operation cancelled", "exit" )
                return

    if patch_data:
        try:
            data_dict = json.loads(patch_data)
        except json.JSONDecodeError:
            logecho("Error: Could not decode JSON '{}'".format(patch_data), 'error')
            return False
        except Exception as e:
            logecho("An unexpected error occurred: {}".format(e), 'error')
            return False
    else:
        data_dict = {}
        logecho( "Patch data is an empty dict", "warning" )

    for dataset in datasets:
        logecho( "About to patch {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
        if confirm_each:
            if click.confirm('🟢 Proceed with patch?'):
                logecho( "Proceeding with patch ...", "info" )
            else: 
                logecho( "Patch cancelled", "warning" )
                continue
        try:
            # Run patch function
            if patch_fn_dict[patch_fn](ctx,dataset,data_dict):
                logecho( "... patched", 'info')
            elif test_run:
                logecho( "... patched skipped by test_run", 'info')
            else:
                logecho( "... patched failed", 'info')

        except Exception as e:
            logecho( e, 'error' )

def patch_fn_example(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        logecho('This is an example patch function', 'info')    
        
        if test_run:
            return False

        # Call action here

    except Exception as e:
        if str(e) == 'Not found':
            logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
            return False
        else:
            logecho("Error: {}".format(e), 'error')
            return False

    return True


def patch_fn_validate_datasets(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        if test_run:
            return False

        remote.action.package_patch( id=dataset.get("id") )

    except Exception as e:
        logecho( "Bailing out: Dataset {} does not validate: {}".format( dataset['name'], e ), 'error' )
        sys.exit(1)

    return True


def patch_fn_fix_empty_date_ranges(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    if 'date_range' in dataset:
        logecho( dataset['date_range'], 'info' )
        logecho( 'Date range exists, skipping ...', 'info' )
    else:
        logecho( 'No date range!', 'info' )

        try:
            if test_run:
                return False

            remote.action.package_patch( id=dataset.get("id"), date_range="no date range" )

        except Exception as e:
            if str(e) == 'Not found':
                logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
                return False
            else:
                logecho("Error: {}".format(e), 'error')
                return False
    return True


def patch_fn_clear_spatial_data(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        if test_run:
            return False

        remote.action.package_patch( id=dataset.get("id"), gazetteer="" )

    except Exception as e:
        if str(e) == 'Not found':
            logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
            return False
        else:
            logecho("Error: {}".format(e), 'error')
            return False

    return True
    
def patch_fn_clear_spatial_data_full(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        if test_run:
            return False

        remote.action.package_patch( id=dataset.get("id"), gazetteer="" )

    except Exception as e:
        if str(e) == 'Not found':
            logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
            return False
        else:
            logecho("Error: {}".format(e), 'error')
            return False

    return True


def patch_fn_set_spatial_data(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        spatial_simp = data.get('spatial_simp', '{}')
        parsed_spatial_simp = json.loads(spatial_simp)

    except json.JSONDecodeError as e:
        logecho(f"JSON parsing error on spatial_simp: {e}, value: {spatial_simp}", 'error')

    try:
        spatial_full = data.get('spatial_full', '{}')
        parsed_spatial_simp = json.loads(spatial_full)

    except json.JSONDecodeError as e:
        logecho(f"JSON parsing error on spatial_full: {e}, value: {spatial_full}",'error')

    try:
        if test_run:
            return False

        remote.action.package_patch( id=dataset.get("id"), spatial_simp=spatial_simp, spatial_full=spatial_full )

    except Exception as e:
        if str(e) == 'Not found':
            logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
            return False
        else:
            logecho("Error: {}".format(e), 'error')
            return False

    return True

def patch_fn_clear_data_dictionary(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        if test_run:
            return False

        remote.action.package_patch( id=dataset.get("id"), data_dictionary="" )

    except Exception as e:
        if str(e) == 'Not found':
            logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
            return False
        else:
            logecho("Error: {}".format(e), 'error')
            return False

    return True


def patch_fn_set_title(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        if test_run:
            return False

        remote.action.package_patch( id=dataset.get("id"), title=data['title'] )

    except Exception as e:
        if str(e) == 'Not found':
            logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
            return False
        else:
            logecho("Error: {}".format(e), 'error')
            return False

    return True

def patch_fn_set_app_email(ctx,dataset,data):

    remote = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        if test_run:
            return False

        remote.action.package_patch( id=dataset.get("id"), data_contact_email=data['email'] )

    except Exception as e:
        if str(e) == 'Not found':
            logecho( "Error: dataset {} not found".format(dataset.get("id")), 'error')
            return False
        else:
            logecho("Error: {}".format(e), 'error')
            return False

    return True


@twdhcli.command()
@click.option('--patch-file',
              required=True,
              default=None,
              help='JSON file containing patch data')
@click.option('--confirm-each',
              default=False,
              is_flag=True,
              help='Confirm each patch operation instead of just once at the start')
@click.pass_context
def restore_spatial(ctx, patch_file, confirm_each):
    """
    Restore spatial data to datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    try:
        with open(patch_file, "r") as file:
            patch_data = json.load(file)
    except FileNotFoundError:
        logecho("Error: The file was not found.", 'error')
        sys.exit(1)
    except json.JSONDecodeError as e:
        logecho(f"Error: Could not decode JSON from '{patch_file}'. Check if the file contains valid JSON.", 'error')
        logecho( f"{e}", 'error' )
        sys.exit(1)
    except Exception as e:
        logecho(f"An unexpected error occurred: {e}", 'error')
        sys.exit(1)
    logecho( "Restoring spatial data from {} ...".format(patch_file), "info" )

    if not confirm_each:
        logecho( "Hint: Use --confirm-each if you want to confirm one at a time", "note" )
        if click.confirm('🟢 Proceed with all patches from {}? '.format(patch_file)):
            logecho( "Proceeding with patches ...", "info" )
        else: 
            logecho( "Operation cancelled", "warning" )
            sys.exit(0)
        confirm_all = False
    else:
        confirm_all = True

    for dataset in patch_data['results']:

        logecho( "🟣", "divider" )

        run_patch = True

        if 'gazetteer' in dataset:

            spatial_full = dataset['gazetteer'].get('spatial_full', None)
            spatial_simp = dataset['gazetteer'].get('spatial_simp', None)

            if spatial_full != None or spatial_simp != None:

                logecho( "Spatial data found for dataset \"{}\"".format(dataset['name']), "info" )

                if confirm_all:
                    if click.confirm("🟢 Proceed to patch dataset \"{}\"? ".format(dataset['name']), abort=False, default=True):
                        run_patch = True
                    else: 
                        logecho( "Patch cancelled for dataset \"{}\"".format(dataset['name']), "warning" )
                        run_patch = False

                if run_patch:
                    if patch_fn_set_spatial_data( ctx, dataset, dataset.get('gazetteer', None)):
                        logecho( "Patched dataset \"{}\"".format(dataset['name']), "info" )
                    else:
                        logecho( "Error patching dataset \"{}\"".format(dataset['name']), "info" )

            else:
                logecho( "No spatial data found for \"{}\"".format(dataset['name']), "info" )

        else:
            logecho( "No gazetteer attribute found for \"{}\"".format(dataset['name']), "info" )

@twdhcli.command()
@click.option('--new-size',
              required=True,
              default=32000,
              help='Maximum size for spatial_simp')
@click.option('--ids',
              required=False,
              default=None,
              help='list of dataset ids to patch')
@click.option('--confirm-each',
              default=False,
              is_flag=True,
              help='Confirm each patch operation instead of just once at the start')
@click.option('--allow-enlarge',
              default=False,
              is_flag=True,
              help='Do not resize spatial_simp to be larger than it already is. This should be set to false in the case that for instance you resized to 4K and you want to resize back to 32K and not have the previously shrunk extents stay at their shrunken size.')
@click.option('--skip-snapshot',
              default=False,
              is_flag=True,
              help='Don\'t prompt for snapshot, and don\'t create a snapshot')

@click.pass_context
def update_spatial_simp(ctx, new_size, ids, confirm_each, allow_enlarge, skip_snapshot):
    """
    Update spatial_simp to new_size
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    if not skip_snapshot and click.confirm('🟢 Take a snapshot before running patches?', default=True):
        h.snapshot( ctx, './twdh-snapshots' )
    else:
        logecho( "Skipped snapshot!", "warning" )

    datasets = h.fetch_datasets(ctx, ids, "dataset")

    # Confirm patch operation
    if ids:
        logecho( "Prepared to update spatial_simp in the following datasets", 'warning')
        for dataset in datasets:
            logecho( "- {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
        if not confirm_each:
            if click.confirm('🟢 Proceed with all updating spatial_simp?'):
                logecho( "Proceeding with updating spatial_simp ...", "info" )
            else: 
                logecho( "Operation cancelled", "exit" )
                sys.exit(0)
    else:
        if not confirm_each:
            if click.confirm('🟢 Proceed with updating spatial_simp on {} {}s?'.format(len(datasets), "dataset")):
                logecho( "Proceeding with updating spatial_simp ...", "info" )
            else: 
                logecho( "Operation cancelled", "exit" )
                return

    for dataset in datasets:
        gazetteer = dataset.get("gazetteer", {})
        if 'spatial_full' in gazetteer and gazetteer['spatial_full'] != None:
            if not allow_enlarge and len(dataset["gazetteer"]["spatial_simp"].encode('utf-8')) < new_size:
                logecho( "+ {} ({}) spatial_simp = {} already less than {}".format(dataset.get("title"),dataset.get("id"),len(dataset["gazetteer"]["spatial_simp"].encode('utf-8')),new_size), 'info')
            else:

                logecho( "About to patch {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
                if confirm_each:
                    if click.confirm('🟢 Proceed with update?'):
                        logecho( "Proceeding with update ...", "info" )
                    else: 
                        logecho( "Update cancelled", "warning" )
                        continue
                try:
                    if len(dataset["gazetteer"]["spatial_full"].encode('utf-8')) < new_size:
                        logecho( " {} ({}) spatial_full = {} already less than {}, setting spatial_simp = spatial_full".format(dataset.get("title"),dataset.get("id"),len(dataset["gazetteer"]["spatial_simp"].encode('utf-8')),new_size), 'info')
                        gazetteer['spatial_simp'] = gazetteer['spatial_full']
                    else:
                        #logecho( " updating {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
                        gazetteer['spatial_simp'] = h.simplify_geojson_by_size(ctx,gazetteer['spatial_full'],new_size)

                    if patch_fn_set_spatial_data(ctx,dataset,gazetteer):
                        logecho( "Updated spatial_simp on dataset \"{}\"".format(dataset['name']), "info" )
                    else:
                        logecho( "Error updating spatial_simp on dataset \"{}\"".format(dataset['name']), "info" )
                    

                except Exception as e:
                    logecho( e )

@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='list of dataset ids to show')
@click.pass_context
def show_datasets(ctx,ids):
    """
    Show datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    datasets = h.fetch_datasets(ctx, ids)

    for dataset in datasets:
        logecho("{}: {}".format(dataset["name"], str(dataset)), 'info')


@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='dataset state report')
@click.pass_context
def dataset_state_report(ctx,ids):
    """
    Print a report of dataset states
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    data_admin_approved = ['approved','unapproved']
    state = ['active','draft']
    private = ['true','false']

    results = []
    for d in data_admin_approved:
        for s in state:
            for p in private:
                result = twdh.action.package_search( 
                        fq_list=[ 
                            'type:dataset',
                            'data_admin_approved:{}'.format(d), 
                            'state:{}'.format(s), 
                            'private:{}'.format(p) 
                        ],
                        include_private=True,
                        include_drafts=True
                    ) 
                results.append( result)
                logecho('data_admin_approved={}/state={}/private={}: {}'.format( 
                    d, 
                    s, 
                    p, 
                    result['count'] 
                ) )

    c = 0
    for r in results:
        c+= r['count']
    logecho('{} datasets'.format(c))

@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='list of unapproved public active datasets')
@click.pass_context
def get_unapproved_public_active_datasets(ctx,ids):
    """
    Show unapproved public active datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    results = twdh.action.package_search(
        fq_list=[
            'data_admin_approved:unapproved',
            'state:active',
            'private:false'
        ],
        rows=10000
    )
    if results['count'] > 0:
        for result in results['results']:
            logecho(result['id'], 'info')
    else:
        logecho( 'No unapproved, public, active datasets found. That\'s a good thing!', 'info' )

@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='list of approved private draft datasets')
@click.pass_context
def get_approved_private_draft_datasets(ctx,ids):
    """
    Show approved private draft datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']


    results = twdh.action.package_search(
        fq_list=[
            'data_admin_approved:approved',
            'state:draft',
            'private:true'
        ]
    )
    if results['count'] > 0:
        for result in results['results']:
            logecho(result['id'], 'info')
    else:
        logecho( 'No approved, private, draft datasets found. That\'s a good thing!', 'info' )


@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='list of dataset ids to show')
@click.pass_context
def show_applications(ctx,ids):
    """
    Show applications
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    datasets = h.fetch_datasets(ctx, ids, 'application')

    for dataset in datasets:
        logecho("{}: {}".format(dataset["name"], str(dataset)), 'info')


@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='list of dataset ids to show')
@click.pass_context
def list_datasets(ctx,ids):
    """
    List datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    datasets = h.fetch_datasets(ctx, ids)

    for dataset in datasets:
        logecho(dataset["name"], 'info')


@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='list of dataset ids to show')
@click.pass_context
def list_applications(ctx,ids):
    """
    List applications
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    datasets = h.fetch_datasets(ctx, ids, 'application')

    for dataset in datasets:
        logecho(dataset["name"], 'info')


@twdhcli.command()
@click.option('--ids',
              required=False,
              default=None,
              help='list of dataset spatial stats to show')
@click.option('--csvout',
              type=click.Path(),
              default='./spatial-stats.csv',
              show_default=True,
              help='The full path of the CSV output file.')
@click.option('--quiet',
              default=False,
              is_flag=True,
              help='Don\t write per-dataset details to stdout')
@click.pass_context
def spatial_stats(ctx,ids,csvout,quiet):
    """
    Get spatial stats of datasets and export them to a CSV
    """

    h.spatial_stats( ctx, ids, csvout, quiet )


if __name__ == '__main__':
    twdhcli(obj={},auto_envvar_prefix='TWDHCLI')
