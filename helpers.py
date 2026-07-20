import os
import sys
import csv
import json
import subprocess
import traceback

from datetime import datetime

from pathlib import Path
from urllib.parse import urlparse

from shapely import to_geojson
from shapely.geometry import shape
from shapely.ops import unary_union
from shapely.validation import make_valid

from ckanapi.errors import NotFound

PROD_HOSTS = {
    "txwaterdatahub.org",
    "www.txwaterdatahub.org",
}

def apikey_validates(ctx,apikey):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    try:
        results = twdh.action.user_list()
        logecho('API Key test passed', level='info')
        return True
    except Exception as e:
        logecho('API Key is not valid ', level='error')
        #print(traceback.format_exc())
        sys.exit(1)

def snapshot(ctx,dest):
    """ Create json and jsonl snapshot files of 
        - users
        - organizations
        - datasets
            - data dictionaries
            - resource views
            - spatial extents
        - applications
        ... and run a 'spatial stats' report output to a CSV file

    Args:
        ctx (dict): context
        dest (str): destination directory
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    """
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
    """

    snap_dest = mk_report_dir(ctx,dest)

    snapshot_datasets( ctx, snap_dest )
    snapshot_datasets_jsonl( ctx, snap_dest )
    snapshot_data_dictionaries( ctx, snap_dest )
    snapshot_resource_views( ctx, snap_dest )
    snapshot_spatial( ctx, snap_dest )
    spatial_stats( ctx, [], '{}/spatial-stats.csv'.format( snap_dest ) )

    logecho("Snapshot complete!", 'celebration')
    logecho("Snapshot directory: {}".format(snap_dest), 'info')


def snapshot_datasets(ctx,snap_dest):
    """ Create human readable dataset and application backups

    Args:
        ctx (dict): context
        span_dest (str): destination directory
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    dataset_types = [ 'dataset', 'application' ]

    for dataset_type in dataset_types:
        dataset_file = '{}/{}s.json'.format(snap_dest, dataset_type) 
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


def snapshot_data_dictionaries(ctx,snap_dest):
    """ Create resource 'data dictionary' backup

    Args:
        ctx (dict): context
        span_dest (str): destination directory
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    dd_file = '{}/data-dicts.jsonl'.format(snap_dest) 
    results = twdh.action.package_search(
        rows=100000,
        fq="type:dataset",
        include_deleted=True,
        include_drafts=True,
        include_private=True
    )
    try:
        with open(dd_file, 'w') as json_file:
            for dataset in results['results']:
                if "resources" in dataset:
                    for resource in dataset['resources']:
                        dd = twdh.action.data_dictionary_show( id=resource['id'] )
                        if len(dd) > 0:
                            record = {
                                "package_id": dataset.get("id"),
                                "package_name": dataset.get("name"),
                                "resource_id": resource.get("id"),
                                "resource_name": resource.get("name"),
                                "data_dictionary": dd,
                            }

                            json_file.write(json.dumps(record) + '\n')

        logecho( 'Created snapshot file: {}'.format(dd_file), 'info' )

    except FileNotFoundError:
        logecho( "Unable to write JSON / Destination not found error", 'error' )
        sys.exit(1)
    except Exception as e:
        logecho( "An unexpected error occurred: {}".format(e), 'error' )
        sys.exit(1)


def snapshot_resource_views(ctx,snap_dest):
    """ Create resource 'views' backup

    Args:
        ctx (dict): context
        span_dest (str): destination directory
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    v_file = '{}/resource-views.jsonl'.format(snap_dest) 
    results = twdh.action.package_search(
        rows=100000,
        fq="type:dataset",
        include_deleted=True,
        include_drafts=True,
        include_private=True
    )
    try:
        with open(v_file, 'w') as json_file:
            for dataset in results['results']:
                if "resources" in dataset:
                    for resource in dataset['resources']:
                        views = twdh.action.resource_view_list( id=resource['id'] )
                        if len(views) > 0:
                            record = {
                                "package_id": dataset.get("id"),
                                "package_name": dataset.get("name"),
                                "package_type": dataset.get("type"),
                                "resource_id": resource.get("id"),
                                "resource_name": resource.get("name"),
                                "views": views,
                            }
                            json_file.write(json.dumps(record) + '\n')

        logecho( 'Created snapshot file: {}'.format(v_file), 'info' )

    except FileNotFoundError:
        logecho( "Unable to write JSON / Destination not found error", 'error' )
        sys.exit(1)
    except Exception as e:
        logecho( "An unexpected error occurred: {}".format(e), 'error' )
        #sys.exit(1)


def snapshot_datasets_jsonl(ctx,snap_dest):
    """ Create JSONL backups of datasets,  applications, organizations, and 
    users. Datasets include type 'dataset' and 'application' all in the same 
    file.

    Args:
        ctx (dict): context
        span_dest (str): destination directory
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    obj_types = [ 
        'datasets', 
        'groups', 
        'organizations', 
        'users'
    ]
    for obj_type in obj_types:
        obj_file = '{}/{}.jsonl'.format(snap_dest, obj_type)
        try:
            if obj_type == "organizations":
                logecho('Dumping organizations with ckanapi dump...\n', 'info')

                command = [
                    "ckanapi",
                    "dump", "organizations",
                    "--apikey={apikey}".format(apikey=twdh.apikey),
                    "--all",
                    "-O", "{obj_file}".format(obj_file=obj_file),
                    "-r", "{url}".format(url=twdh.address)
                ]

                subprocess.check_call(command)

                logecho(
                    "Appending dumped organizations with users...\n",
                    "info",
                )

                enriched_orgs = []

                with open(obj_file, "r") as json_file:
                    for line in json_file:
                        if not line.strip():
                            continue

                        org = json.loads(line)
                        org_id = org.get("id") or org.get("name")

                        if not org_id:
                            continue

                        try:
                            full_org = twdh.action.organization_show(
                                id=org_id,
                                include_users=True,
                                include_datasets=False,
                            )

                            enriched_orgs.append(full_org)

                        except Exception as e:
                            logecho(
                                "Could not enrich organization {} with users, keeping dumped org. Error: {}".format(
                                    org_id,
                                    e,
                                ),
                                "warning",
                            )
                            enriched_orgs.append(org)

                with open(obj_file, "w") as json_file:
                    for org in enriched_orgs:
                        json_file.write(json.dumps(org) + "\n")

                logecho('Created snapshot file: {}'.format(obj_file), 'info')
                logecho(
                    "Successfully dumped and enriched {} organizations to {}".format(
                        len(enriched_orgs),
                        obj_file,
                    ),
                    'info',
                )
                continue

            command = [
                "ckanapi",
                "dump", "{obj_type}".format(obj_type=obj_type),
                "--apikey={apikey}".format(apikey=twdh.apikey),
                "--all",
                "-O", "{obj_file}".format(obj_file=obj_file),
                "-r", "{url}".format(url=twdh.address)
            ]


            logecho( 'Dumping {}...\n'.format(obj_type), 'info' )
            subprocess.check_call(command)
            logecho( 'Created snapshot file: {}'.format(obj_file), 'info' )

        except Exception as e:
            logecho( "An error occurred: {}".format(e), 'error' )
            print(traceback.format_exc())
            sys.exit(1)


        logecho("Successfully dumped {} to {}".format(obj_type, obj_file), 'info')

def snapshot_spatial(ctx,snap_dest):
    """ Create backup of spatial data

    Args:
        ctx (dict): context
        span_dest (str): destination directory
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    try:

        dataset_file = f'{snap_dest}/spatial_data.jsonl' 
        results = twdh.action.package_search(
            rows=100000,
            fq=f"type:dataset",
            fl="id",
            include_deleted=True,
            include_drafts=True,
            include_private=True
        )

        with open(dataset_file, 'w') as json_file:
            for result in results["results"]:
                logecho( result["id"], 'info' )
                spatial_data = twdh.action.spatial_extents_show(
                    id=result["id"],
                    include_all=True
                )
                record = {
                        "package_id": result.get("id"),
                        "package_name": result.get("name"),
                        "spatial_data": spatial_data,
                    }

                json_file.write(json.dumps(record) + '\n')
        logecho( 'Created spatial data snapshot file: {}'.format(dataset_file), 'info' )

    except FileNotFoundError:
        logecho( "Unable to write JSON / Destination not found error", 'error' )
        sys.exit(1)
    except Exception as e:
        logecho( "An unexpected error occurred, unable to write JSON: {}".format(e), 'error' )
        sys.exit(1)


def spatial_stats(ctx, ids, csvout):
    """ Create spatial stats report

    Args:
        ctx (dict): context
        ids (str): space separated list of ids to include / all ids processed if left empty
        csvout (str): output destination file
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    datasets = fetch_datasets(ctx, ids)

    dataset_count = 0
    spatial_dataset_count = 0
    nonspatial_dataset_count = 0
    spatial_full_total = 0
    spatial_extent_total = 0

    logecho( "", "divider" )

    csvdata = [['id','name','spatial_full_size','spatial_simp_size','spatial_simp_reduction']]
    for dataset in datasets:
        dataset_count += 1
        spatial_full_size = 0
        spatial_extent_size = 0
        spatial_extent_reduction = 0
        if "spatial_extent" in dataset and len(dataset["spatial_extent"]) > 0:

            spatial_extents = twdh.action.spatial_extents_show(id=dataset["id"])

            if "spatial_extent_full" in spatial_extents:
                spatial_full_size = len(json.dumps(spatial_extents["spatial_extent_full"]))
                spatial_full_total += spatial_full_size
            else:
                spatial_full_size = 0

            if dataset["spatial_extent"] is not None:
                spatial_extent_size = len(json.dumps(dataset["spatial_extent"]))
                spatial_extent_total += spatial_extent_size
            else:
                spatial_extent_size = 0

            if dataset["spatial_extent"] is not None:
                spatial_dataset_count += 1
                if spatial_full_size > 0:
                  spatial_extent_reduction = '{}%'.format(round(( 100 - ( ( spatial_extent_size / spatial_full_size ) * 100 ) ), 2))
                else:
                  spatial_extent_reduction = 'n/a'
                logecho("{} / spatial_full: {} / spatial_extent: {} / reduction: {}".format(dataset["name"], spatial_full_size, spatial_extent_size, spatial_extent_reduction ), "info")

            else:
                nonspatial_dataset_count += 1
                spatial_extent_reduction = 0

        else:
            nonspatial_dataset_count += 1

    
        csvdata.append( [dataset['id'], dataset['name'], spatial_full_size, spatial_extent_size, spatial_extent_reduction] )

    logecho( "", "divider" )
    logecho("{} spatial datasets".format(spatial_dataset_count), "info")
    csvdata.insert(0,["# {} spatial datasets".format(spatial_dataset_count)])
    logecho("{} nonspatial datasets".format(nonspatial_dataset_count), "info")
    csvdata.insert(1,["# {} nonspatial datasets".format(nonspatial_dataset_count)])
    logecho("spatial_full_total = {} bytes".format(spatial_full_total), "info")
    csvdata.insert(2,["# spatial_full_total = {} bytes".format(spatial_full_total)])
    logecho("spatial_extent_total = {} bytes".format(spatial_extent_total), "info")
    csvdata.insert(3,["# spatial_extent_total = {} bytes".format(spatial_extent_total)])


    if spatial_full_total > 0:
        simplification_reduction = 100 - ( ( spatial_extent_total / spatial_full_total ) * 100 )
    else:
        simplification_reduction = 0
    logecho("simplification reduction = {}%".format( round( simplification_reduction, 2 ) ), "info")
    csvdata.insert(4,["# simplification reduction = {}%".format( round( simplification_reduction, 2 ) )])

    try:
        with open(csvout, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(csvdata)
    
    except FileNotFoundError:
        logecho("Unable to write CSV / File not found error", 'error')
        sys.exit(1)
    except Exception as e:
        logecho(f"An unexpected error occurred, unable to write CSV: {e}", 'error')
        sys.exit(1)

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


def simplify_geojson_by_size(ctx, json_data, max_bytes, tolerance_step=0.001):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    data = json_data
    tolerance = 0.001
    current_size = len(json.dumps(json_data))
    orig_size = current_size

    # retrieve polygons from geojson
    polygons = [make_valid(shape(feature['geometry'])) for feature in data['features']]

    # union polygons and create initial simplification
    merged = unary_union(polygons)
    #simplified = merged.simplify(tolerance, preserve_topology=True)
    current_size = len(to_geojson(merged))

    # repeat simplification with increasing tolerance until size is less than max_bytes
    while current_size > max_bytes and tolerance < 0.5:
        tolerance += tolerance_step
        simplified = merged.simplify(tolerance, preserve_topology=True)
        current_size = len(to_geojson(simplified))
        
    if current_size <= max_bytes:
        reduction = 100 - (( current_size / orig_size ) * 100)
        logecho(f"Original Size: {orig_size} bytes / Final size: {current_size} bytes / Reduction: {round(reduction, 2)}% / Tolerance: {round(tolerance, 4)}", 'info')
        return to_geojson(simplified)
    else:
        logecho("Could not reach target size without losing too much detail. Current size={}".format(current_size), 'info')
        #return json.dumps(json_data)
        return to_geojson(merged)


def assert_not_prod_destination(dest_host):
    parsed = urlparse(dest_host)
    hostname = (parsed.hostname or "").lower()

    if hostname in PROD_HOSTS:
        raise RuntimeError(
            f"Refusing to continue. PROD cannot be used as clone destination: {hostname}"
        )

    if "prod" in hostname and "dev" not in hostname and "localhost" not in hostname:
        raise RuntimeError(
            f"Refusing to continue. Destination looks like production: {hostname}"
        )

def validate_snapshot_dir(snapshot_dir):
    snapshot_path = Path(snapshot_dir)

    if not snapshot_path.exists():
        raise RuntimeError(f"Snapshot directory does not exist: {snapshot_dir}")

    if not snapshot_path.is_dir():
        raise RuntimeError(f"Snapshot path is not a directory: {snapshot_dir}")

    required_files = [
        "datasets.json",
        "applications.json",
        "datasets.jsonl",
        "groups.jsonl",
        "organizations.jsonl",
        "users.jsonl",
        "data-dicts.jsonl",
        # "resource-views.jsonl",
        "spatial_data.jsonl",
    ]

    missing_files = []

    for filename in required_files:
        filepath = snapshot_path / filename
        if not filepath.exists():
            missing_files.append(filename)

    if missing_files:
        raise RuntimeError(
            "Snapshot directory is missing required files: {}".format(
                ", ".join(missing_files)
            )
        )

    return snapshot_path


def dpp_report(ctx,dest):
    """
    Get datapusher status for all DP+ resources
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    host = ctx.obj['host']

    logecho( "Running Datapusher Plus status report")
    report_dest = mk_report_dir(ctx,dest)

    res_count = 0
    results = []
    errors = []
    error_urls = []
    error_messages = []
    error_detail = []
    error_summary = {
        'SolrHttp404': { 'count': 0, 'ids': [] },
        'SolrHttp500': { 'count': 0, 'ids': [] },
        'SolrHttp510': { 'count': 0, 'ids': [] },
        'SolrTimeoutError': { 'count': 0, 'ids': [] },
        'OtherSolrError': { 'count': 0, 'ids': [] },
        'InvalidCSV': { 'count': 0, 'ids': [] },
        'InvalidChunkLength': { 'count': 0, 'ids': [] },
        'InvalidOLESignature': { 'count': 0, 'ids': [] },
        'ReadTimedOut': { 'count': 0, 'ids': [] },
        'MaxRetriesExceeded': { 'count': 0, 'ids': [] },
        'UnsupportedFileFormat': { 'count': 0, 'ids': [] },
        'StatusCode403': { 'count': 0, 'ids': [] },
        'StatusCode404': { 'count': 0, 'ids': [] },
        'StatusCode504': { 'count': 0, 'ids': [] },
        'QSVValidationFailure': { 'count': 0, 'ids': [] },
        'NoTaskStatus': { 'count': 0, 'ids': [] },
        'Null': { 'count': 0, 'ids': [] },
        'Other': { 'count': 0, 'ids': [] }
    }
    error_summary_other = []

    datasets = fetch_datasets(ctx,None,'dataset')
    for dataset in datasets:
        for resource in dataset.get( 'resources', []):
            logecho( "{}({}) {}({}) {}".format( dataset.get('name'),dataset.get('id'),resource.get('name'),resource.get('id'), str(resource.get('datastore_active'))) )
            result = {
                'dataset': dataset.get('name'),
                'dataset_id': dataset.get('id'),
                'resource': resource.get('name'),
                'resource_id': resource.get('id'),
                'resource_format': resource.get('format', 'N/A'),
                'datastore_active': resource.get('datastore_active', 'N/A')
            }
            res_count+=1
            try:
                dp_status = twdh.action.datapusher_status( resource_id=resource.get('id') )
                if dp_status:

                    if "task_info" in dp_status:

                        task_info = dp_status.get("task_info")

                        if not 'status' in task_info:
                            error_summary['NoTaskStatus']['count'] += 1
                            error_summary['NoTaskStatus']['ids'].append(dataset.get('name'))
                        else:
                            # Remove logs if task status is complete
                            if( task_info["status"] == "complete"):
                                dp_status["task_info"].pop('logs')
                            else:
                                error_urls.append(task_info["metadata"].get("original_url", ""))
                                error_messages.append(task_info.get("error", ""))
                                url = task_info["metadata"].get("original_url", "")
                                parsed = urlparse(url)
                                error_detail.append(
                                    {
                                        'last_updated': dp_status.get('last_updated'),
                                        'dataset_name': dataset.get('name'),
                                        'dataset_id': dataset.get('id'),
                                        'resource_name': resource.get('name'),
                                        'resource_id': resource.get('id'),
                                        'domain': parsed.netloc,
                                        'url': url,
                                        'message': task_info.get("error", ""),
                                        'twdh_url': "{}/dataset/{}".format( host, dataset.get('id'))
                                    }
                                )

                                error = task_info.get("error", "")

                                if error is None:
                                    error_summary['Null']['count'] += 1
                                    error_summary['Null']['ids'].append(dataset.get('name'))
                                elif 'Solr returned an error' in error:
                                    if 'HTTP 404' in error:
                                        error_summary['SolrHttp404']['count'] += 1
                                        error_summary['SolrHttp404']['ids'].append(dataset.get('name'))
                                    elif 'HTTP 500' in error:
                                        error_summary['SolrHttp500']['count'] += 1
                                        error_summary['SolrHttp500']['ids'].append(dataset.get('name'))
                                    elif 'HTTP 510' in error:
                                        error_summary['SolrHttp510']['count'] += 1
                                        error_summary['SolrHttp510']['ids'].append(dataset.get('name'))
                                    elif 'TimeoutError' in error:
                                        error_summary['SolrTimeoutError']['count'] += 1
                                        error_summary['SolrTimeoutError']['ids'].append(dataset.get('name'))
                                    else:
                                        error_summary['OtherSolrError']['count'] += 1
                                        error_summary['OtherSolrError']['ids'].append(dataset.get('name'))
                                elif 'Invalid CSV' in error:
                                    error_summary['InvalidCSV']['count'] += 1
                                    error_summary['InvalidCSV']['ids'].append(dataset.get('name'))
                                elif 'InvalidChunkLength' in error:
                                    error_summary['InvalidChunkLength']['count'] += 1
                                    error_summary['InvalidChunkLength']['ids'].append(dataset.get('name'))
                                elif 'Invalid OLE signature' in error:
                                    error_summary['InvalidOLESignature']['count'] += 1
                                    error_summary['InvalidOLESignature']['ids'].append(dataset.get('name'))
                                elif 'Read timed out' in error:
                                    error_summary['ReadTimedOut']['count'] += 1
                                    error_summary['ReadTimedOut']['ids'].append(dataset.get('name'))
                                elif 'Max retries exceeded' in error:
                                    error_summary['MaxRetriesExceeded']['count'] += 1
                                    error_summary['MaxRetriesExceeded']['ids'].append(dataset.get('name'))
                                elif 'unsupported file format' in error:
                                    error_summary['UnsupportedFileFormat']['count'] += 1
                                    error_summary['UnsupportedFileFormat']['ids'].append(dataset.get('name'))
                                elif 'Status code: 404' in error:
                                    error_summary['StatusCode404']['count'] += 1
                                    error_summary['StatusCode404']['ids'].append(dataset.get('name'))
                                elif 'Status code: 403' in error:
                                    error_summary['StatusCode403']['count'] += 1
                                    error_summary['StatusCode403']['ids'].append(dataset.get('name'))
                                elif 'Status code: 504' in error:
                                    error_summary['StatusCode504']['count'] += 1
                                    error_summary['StatusCode504']['ids'].append(dataset.get('name'))
                                elif 'qsv validate failed' in error:
                                    error_summary['QSVValidationFailure']['count'] += 1
                                    error_summary['QSVValidationFailure']['ids'].append(dataset.get('name'))
                                else:
                                    error_summary['Other']['count'] += 1
                                    error_summary['Other']['ids'].append(dataset.get('name'))
                                    error_summary_other.append( error )

                            #logecho('task_info found', 'info')
                            #if task_info.get('status') == 'error':
                            #    logecho(dp_status.get('task_info',False).get('error'), 'error')
                            #else:
                            #    logecho('Success!', 'info')
                            #else:
                            #logecho('task_info not found', 'error')
                            #logecho(dp_status.get('status'), 'error')

                        result['dp_status'] = dp_status

                    if dp_status.get("status") != "complete":
                        errors.append( result )


                else:
                    result['dp_status'] = 'N/A'

            except NotFound as e:
                #logecho('Not Found exception!', 'error')
                #logecho( "No datapusher_status found", "warning")
                if( "datastore_active" in resource):
                    if( resource.get("datastore_active") == True ):
                        logecho( "No datapusher_status found", "warning")
                        logecho("This is NOT ok because datastore_active is set to {}".format(resource.get('datastore_active')), 'error')
                        errors.append( result )

                else:
                    logecho("This is ok because datastore_active is does not exist on this resource", 'info')
            except Exception as e:
                logecho( "Exception loading dataset {}: {}".format( id, e ), 'error')
                print(traceback.format_exc())
                exit(1)

            results.append( result )

    with open("{}/dp-results.json".format(report_dest), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)

    with open("{}/dp-errors.json".format(report_dest), "w", encoding="utf-8") as f:
        json.dump(errors, f, indent=4)

    with open("{}/dp-error-urls.json".format(report_dest), "w", encoding="utf-8") as f:
        json.dump(error_urls, f, indent=4)

    with open("{}/dp-error-messages.json".format(report_dest), "w", encoding="utf-8") as f:
        json.dump(error_messages, f, indent=4)

    with open("{}/dp-error-detail.json".format(report_dest), "w", encoding="utf-8") as f:
        json.dump(error_detail, f, indent=4)

    with open("{}/dp-error-summary.json".format(report_dest), "w", encoding="utf-8") as f:
        json.dump(error_summary, f, indent=4)


    headers = error_detail[0].keys()
    with open("{}/dp-error-detail.csv".format(report_dest), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()  # Writes the header row
        writer.writerows(error_detail)  # Writes all data rows


    for error in error_summary:
        logecho( "{}: {}".format( error, error_summary[error]['count'] ), 'warning')
    if len(error_summary_other) > 0:
        logecho( "Other errors:", 'warning')
        for error in error_summary_other:
            logecho( "{}".format( error ), 'warning')
    logecho( "{} resources inspected".format(res_count))
    logecho( "{} errors found".format(len(errors)))


def tag_report(ctx,dest):
    """
    Generate report of tags and associated datasets
    """

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    host = ctx.obj['host']

    logecho( "Running tag report")
    report_dest = mk_report_dir(ctx,dest)

    results = {}
    try:
        tags = twdh.action.tag_list()
        tags.sort()

        for tag in tags:
            datasets = twdh.action.package_search(
                fq_list=[
                    'tags:{}'.format(tag)
                ],

                rows=1000,
                sort="name asc",
                #fl=["type","name","title"]
            )

            logecho("{} ({})".format(tag,datasets.get('count',0)),"info")

            if( datasets.get('count',0) > 0 ):
                results[tag] = {}
                results[tag]['url'] = "{}/dataset/?tags={}".format(host,tag)
                results[tag]['count'] = datasets.get('count')
                results[tag]['application_count'] = 0
                results[tag]['dataset_count'] = 0
                results[tag]['applications'] = []
                results[tag]['datasets'] = []

                for dataset in datasets.get('results',[]):
                    logecho("  {}".format(dataset.get("title"),"info"))
                    result = {}
                    result["title"] = dataset.get("title")
                    #result["type"] = dataset.get("type")
                    result["url"] = "{}/dataset/{}".format(host,dataset.get("name"))
                    if tag in dataset['primary_tags']:
                        result['primary'] = True
                    else:
                        result['primary'] = False
                    if( dataset.get("type") == "application"):
                        results[tag]['application_count'] += 1
                        results[tag]['applications'].append(result)
                    else:
                        results[tag]['dataset_count'] += 1
                        results[tag]['datasets'].append(result)

                if results[tag]["application_count"] == 0:
                    results[tag].pop("applications")

                if results[tag]["dataset_count"] == 0:
                    results[tag].pop("datasets")

    except Exception as e:
        logecho( str(e), 'error')
        print(traceback.format_exc())
        exit(1)

    report = {
        'report': 'Tags Report',
        'host': host,
        'timestamp': datetime.now().isoformat(),
        'tags': results
    }

    with open('{}/tag-report.json'.format(report_dest), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

def mk_report_dir(ctx,dest):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']
    host = ctx.obj['host']

    if not os.path.exists(dest):
        logecho('Destination directory {} not found'.format(dest), level='error')
        sys.exit()

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        parsed_address = urlparse(twdh.address)
        logecho( parsed_address.netloc )
        report_dest = "{}/{}_{}".format( dest, parsed_address.netloc, timestamp )
        Path(report_dest).mkdir(parents=True)

    except Exception as e:
        logecho('An error occurred: {}'.e, level='error')
        sys.exit(1)

    return report_dest