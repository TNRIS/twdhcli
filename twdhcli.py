from __future__ import annotations

from colorama import init, Fore, Back, Style
import click
import click_config_file
import ckanapi
import requests
import os
import sys
import json
from shapely import from_geojson, to_geojson
from shapely.geometry import shape
from dotenv import dotenv_values
from datetime import datetime, date
from time import perf_counter
import logging
import csv
from pathlib import Path
from urllib.parse import urlparse
import subprocess

init(autoreset=True)

log = logging.getLogger(__name__)
FORMAT = '%(message)s'
#logging.basicConfig(format=FORMAT, level=logging.INFO)

version = '0.1'
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def get_patch_functions():
    return  {
        'example': patch_fn_example,
        'clear_data_dictionary': patch_fn_clear_data_dictionary,
        'set_title': patch_fn_set_title,
        'set_app_email': patch_fn_set_app_email,
        'clear_spatial_data': patch_fn_clear_spatial_data,
        'set_spatial_data': patch_fn_set_spatial_data,

    }

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
              required=False,
              help='TWDH CKAN host.')
@click.option('--apikey',
              required=False,
              help='TWDH CKAN api key to use if authentication is required.')
@click.option('--test-run',
              is_flag=True,
              help='Show what would change but don\'t make any changes.')
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
                       Fore.GREEN + message)
        elif level == 'detail':
            logger.debug(message)
            click.echo('🔵 ' +
                       Fore.BLUE + message)
        elif level == 'info':
            logger.debug(message)
            click.echo('⚪️ ' + Fore.WHITE + message)
        elif level == 'exit':
            logger.debug(message)
            click.echo('⚫️⚫️ ' + Fore.WHITE + message)
        elif level == 'celebration':
            logger.debug(message)
            click.echo('🎉 ' + Fore.MAGENTA + message)
        elif level == 'divider':
            logger.debug(message)
            click.echo(Fore.MAGENTA + message)
        else:
            logger.info(message)
            click.echo(Fore.GREEN + message)

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

    _snapshot(ctx,dest)

def _snapshot(ctx,dest):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    if not os.path.exists(dest):
        logecho('Destination directory {} not found'.format(dest), level='error')
        sys.exit()

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        parsed_address = urlparse(twdh.address)
        logecho( parsed_address.netloc )
        snap_dest = "{}/{}_{}".format( dest, parsed_address.netloc, timestamp )
        Path(snap_dest).mkdir(parents=True)

    except Exception as e:
        logecho('An error occurred: {}'.e, level='error')
        sys.exit(1)

    _spatial_stats( ctx, [], '{}/spatial-stats.csv'.format( snap_dest ), True )

    dataset_types = [ 'dataset', 'application' ]

    # Create human readable dataset and application backups
    for dataset_type in dataset_types:
        dataset_file = '{}/{}.json'.format(snap_dest, dataset_type) 
        results = twdh.action.package_search(
            rows=100000,
            fq="type:{}".format(dataset_type),
            include_deleted=True,
            include_drafts=True,
            include_private=True
        )
        try:
            with open(dataset_file, 'w') as json_file:
                json.dump(results, json_file, indent=4) #
            logecho( 'Created snapshot file: {}'.format(dataset_file), 'info' )

        except FileNotFoundError:
            logecho( "Unable to write JSON / Destination not found error", 'error' )
            sys.exit(1)
        except Exception as e:
            logecho( "An unexpected error occurred, unable to write JSON: {}".format(e), 'error' )
            sys.exit(1)

    # Create JSONL backups of datasets, applications, organizations, users. Datasets include type 'dataset' and 'application' all in the same file.
    obj_types = [ 'datasets', 'groups', 'organizations', 'users']
    for obj_type in obj_types:
        obj_file = '{}/{}.jsonl'.format(snap_dest, obj_type)
        try:

            """
            # I would like to use the 'dump' functionality of ckanapi here but I can't figure out how to make it work, so I'll call the command instead for now
            with open(obj_file, 'w') as jsonl_file:
                for item in twdh.dump_things(obj_type):
                    jsonl_file.write(json.dumps(item) + '\n')
            """

            command = "ckanapi dump {obj_type} --apikey={apikey} --all -O {obj_file} -r {url}".format( \
                obj_type=obj_type, \
                apikey=twdh.apikey, \
                obj_file=obj_file, \
                url=twdh.address \
            )

            #logecho( command, 'info' )
            logecho( 'Dumping {}...\n'.format(obj_type), 'info' )
            output = subprocess.getoutput(command)
            logecho( output, 'info' )
            logecho( 'Created snapshot file: {}'.format(obj_file), 'info' )


        except FileNotFoundError:
            logecho( "Unable to write JSONL / Destination not found error", 'error' )
            sys.exit(1)
        except Exception as e:
            logecho( "An error occurred: {}".format(e), 'error' )
            sys.exit(1)


        logecho("Successfully dumped datasets to {}".format(obj_file), 'info')

    logecho("Snapshot complete!", 'celebration')

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

    datasets = fetch_datasets(ctx, ids)

    for dataset in datasets:
        logecho("{}: {}".format(dataset["name"], str(dataset)), 'info')

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
        ]
    )
    print(results)


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

    datasets = fetch_datasets(ctx, ids, 'application')

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

    datasets = fetch_datasets(ctx, ids)

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

    datasets = fetch_datasets(ctx, ids, 'application')

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

    _spatial_stats( ctx, ids, csvout, quiet )

def _spatial_stats(ctx, ids, csvout, quiet):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    datasets = fetch_datasets(ctx, ids)

    dataset_count = 0
    spatial_dataset_count = 0
    nonspatial_dataset_count = 0
    spatial_full_total = 0
    spatial_simp_total = 0

    csvdata = [['id','name','spatial_full_size','spatial_simp_size','spatial_simp_reduiction']]
    for dataset in datasets:
        if not quiet:
            logecho("{}".format(dataset["name"] ), "info")
        dataset_count += 1
        spatial_full_size = 0
        spatial_simp_size = 0
        spatial_simp_reduction = 0
        if "gazetteer" in dataset:
            if dataset["gazetteer"]["spatial_full"] is not None:
                spatial_full_size = len(dataset["gazetteer"]["spatial_full"].encode('utf-8'))
                if not quiet:
                    logecho("  spatial_full = {} bytes".format(spatial_full_size), "info")
                spatial_full_total += spatial_full_size
            if dataset["gazetteer"]["spatial_simp"] is not None:
                spatial_simp_size = len(dataset["gazetteer"]["spatial_simp"].encode('utf-8'))
                if not quiet:
                    logecho("  spatial_simp = {} bytes".format(spatial_simp_size), "info")
                spatial_simp_total += spatial_simp_size
            if dataset["gazetteer"]["spatial_full"] is not None and dataset["gazetteer"]["spatial_full"] is not None:
                spatial_dataset_count += 1
                spatial_simp_reduction = ( 100 - ( ( spatial_simp_size / spatial_full_size ) * 100 ) )
                if not quiet:
                    logecho("  simplification reduction = {}%".format( spatial_simp_reduction ), "info")
            else:
                if not quiet:
                    logecho("  no spatial data", "info")
                nonspatial_dataset_count += 1
        else:
            if not quiet:
                logecho("  no spatial data", "info")
            nonspatial_dataset_count += 1

        csvdata.append( [dataset['id'], dataset['name'], spatial_full_size, spatial_simp_size, spatial_simp_reduction] )

    if not quiet:
        logecho("-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-", "info")
    logecho("{} spatial datasets".format(spatial_dataset_count), "info")
    csvdata.insert(0,["# {} spatial datasets".format(spatial_dataset_count)])
    logecho("{} nonspatial datasets".format(nonspatial_dataset_count), "info")
    csvdata.insert(1,["# {} nonspatial datasets".format(nonspatial_dataset_count)])
    logecho("spatial_full_total = {} bytes".format(spatial_full_total), "info")
    csvdata.insert(2,["# spatial_full_total = {} bytes".format(spatial_full_total)])
    logecho("spatial_simp_total = {} bytes".format(spatial_simp_total), "info")
    csvdata.insert(3,["# spatial_simp_total = {} bytes".format(spatial_simp_total)])
    logecho("simplification reduction = {}%".format( 100 - ( ( spatial_simp_total / spatial_full_total ) * 100 ) ), "info")
    csvdata.insert(4,["# simplification reduction = {}%".format( 100 - ( ( spatial_simp_total / spatial_full_total ) * 100 ))])

    try:
        with open(csvout, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csvdata)
    
    except FileNotFoundError:
        print("Unable to write CSV / File not found error")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred, unable to write CSV: {e}")
        sys.exit(1)

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
@click.pass_context
def patch_datasets(ctx, patch_fn, ids, patch_data, dataset_type, confirm_each):
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

    if click.confirm('🟢 Take a snapshot before running patches?', default=True):
        _snapshot( ctx, './twdh-snapshots' )
    else:
        logecho( "Skipped snapshot!", "warning" )


    datasets = fetch_datasets(ctx, ids, dataset_type)

    # Confirm patch operation
    if ids:
        logecho( "+ Prepared to patch the following datasets", 'warning')
        for dataset in datasets:
            logecho( "  - {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
        if not confirm_each:
            if click.confirm('🟢 Proceed with all patches?', abort=True):
                logecho( "Proceeding with patches ...", "info" )
            else: 
                logecho( "Operation cancelled", "exit" )
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
            print("Error: Could not decode JSON '{}'".format(patch_data))
            return False
        except Exception as e:
            print("An unexpected error occurred: {}".format(e))
            return False
    else:
        data_dict = {}

    #print( data_dict )
    #return


    for dataset in datasets:
        logecho( "+ About to patch {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
        if confirm_each:
            if click.confirm('🟢 Proceed with patch?'):
                logecho( "    Proceeding with patch ...", "info" )
            else: 
                logecho( "    Patch cancelled", "warning" )
                continue
        try:
            logecho( "    ... patching {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')
            # Run patch function
            patch_fn_dict[patch_fn](twdh,dataset,data_dict)
            logecho( "    ...... patched {} ({})".format(dataset.get("title"),dataset.get("id")), 'info')

        except Exception as e:
            print( e )

    


def patch_fn_example():
    print('This is an example patch function')
    #patch = twdh.action.package_patch( id=dataset.get("id") )
    #patch = twdh.action.package_patch( id=dataset.get("id"), extras=[], data_admin_approved="approved" )
    #patch = twdh.action.package_patch( id=dataset.get("id"), data_admin_approved="approved", end_date="", group=["water-use"] )
    #patch = twdh.action.package_patch( id=dataset.get("id"), gazetteer= { "spatial_simp": "", "spatial_full": "", "place_keywords": "El Paso (City), Austin (City), Austin (County), ABCDEFGH" })


def patch_fn_clear_spatial_data(remote,dataset,data):

    patch = remote.action.package_patch( id=dataset.get("id"), gazetteer="" )

def patch_fn_set_spatial_data(remote,dataset,data):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    test_run = ctx.obj['test_run']

    try:
        spatial_simp = data.get('spatial_simp', '{}')
        parsed_spatial_simp = json.loads(spatial_simp)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error on spatial_simp: {e}, value: {spatial_simp}")

    try:
        spatial_full = data.get('spatial_full', '{}')
        parsed_spatial_simp = json.loads(spatial_full)
    except json.JSONDecodeError as e:
        print(f"JSON parsing error on spatial_full: {e}, value: {spatial_full}")


    patch = remote.action.package_patch( id=dataset.get("id"), spatial_simp=spatial_simp, spatial_full=spatial_full )

    # {"gazetteer": {"spatial_simp": null,"spatial_full": null,"place_keywords": null}}

def patch_fn_clear_data_dictionary(remote,dataset,data):
    patch = remote.action.package_patch( id=dataset.get("id"), data_dictionary="" )

def patch_fn_set_title(remote,dataset,data):
    patch = remote.action.package_patch( id=dataset.get("id"), title=data['title'] )

def patch_fn_set_app_email(remote,dataset,data):
    patch = remote.action.package_patch( id=dataset.get("id"), data_contact_email=data['email'] )

def fetch_datasets(ctx,ids=None,package_type='dataset'):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    if ids:
        logecho('Fetching {}s: {}'.format(package_type,ids) )
    else:
        logecho('Fetching all {}s'.format(package_type))
    datasets = []

    if ids:
        id_list = ids.split()
        for id in id_list:
            try:

                dataset = twdh.action.package_show( id=id )
                if dataset:
                    datasets.append( dataset )
            except Exception as e:
                logecho( "Exception loading dataset {}: {}".format( id, e ), 'error')
                exit(1)

    else:
            query = twdh.action.package_search(
                rows=100000,
                fq="type:{}".format(package_type),
                include_drafts=True,
                include_private=True
            )
            if query["count"] == 0:
                logecho( "No datasets found", 'error')
                exit(1)
            else:
                datasets=query["results"]

    return datasets


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
    Patch datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    try:
        with open(patch_file, "r") as file:
            patch_data = json.load(file)
    except FileNotFoundError:
        print("Error: The file was not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from '{patch_file}'. Check if the file contains valid JSON.")
        print( f"{e}" )
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    logecho( "Restoring spatial data from {} ...".format(patch_file), "info" )

    if not confirm_each:
        logecho( "Hint: Use --confirm-each if you want to confirm one at a time", "note" )
        if click.confirm('🟢 Proceed with all patches from {}? '.format(patch_file), abort=True):
            logecho( "Proceeding with patches ...", "info" )
        else: 
            logecho( "Operation cancelled", "warning" )
        confirm_all = False
    else:
        confirm_all = True

    for dataset in patch_data['result']['results']:

        logecho( "🟣", "divider" )

        if 'gazetteer' in dataset:

            spatial_full = dataset['gazetteer'].get('spatial_full', None)
            spatial_simp = dataset['gazetteer'].get('spatial_simp', None)

            if spatial_full != None or spatial_simp != None:

                logecho( "Spatial data found for dataset \"{}\"".format(dataset['name']), "info" )
                #logecho( "  spatial_full: {}".format(spatial_simp[:50]), "info" )
                #logecho( "  spatial_simp: {}".format(spatial_simp[:50]), "info" )

                if confirm_all:
                    if click.confirm("🟢 Proceed to patch dataset \"{}\"? ".format(dataset['name']), abort=False, default=True):
                        patch_fn_set_spatial_data( twdh, dataset, dataset.get('gazetteer', None))
                        logecho( "Patched dataset \"{}\"".format(dataset['name']), "info" )
                    else: 
                        logecho( "Patch cancelled for dataset \"{}\"".format(dataset['name']), "warning" )
                else:
                    patch_fn_set_spatial_data( twdh, dataset, dataset.get('gazetteer', None))
                    logecho( "Patched dataset \"{}\"".format(dataset['name']), "info" )


            else:
                logecho( "No spatial data found for \"{}\"".format(dataset['name']), "info" )
        else:
            logecho( "No spatial data found for \"{}\"".format(dataset['name']), "info" )


if __name__ == '__main__':
    twdhcli(obj={},auto_envvar_prefix='TWDHCLI')
