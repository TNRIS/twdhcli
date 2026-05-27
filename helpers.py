import os
import sys
import csv
import json
import subprocess
import traceback

from datetime import datetime, date

from pathlib import Path
from urllib.parse import urlparse

from shapely import from_geojson, to_geojson
from shapely.geometry import shape, mapping, MultiPolygon, Polygon
from shapely.ops import unary_union
from shapely.validation import make_valid

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

    snapshot_datasets( ctx, snap_dest )
    snapshot_datasets_jsonl( ctx, snap_dest )
    snapshot_data_dictionaries( ctx, snap_dest )
    snapshot_resource_views( ctx, snap_dest )
    snapshot_spatial( ctx, snap_dest )
    spatial_stats( ctx, [], '{}/spatial-stats.csv'.format( snap_dest ) )

    logecho("Snapshot complete!", 'celebration')


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
                            json_file.write(json.dumps(dd) + '\n')

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
                            json_file.write(json.dumps(views) + '\n')

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


        logecho("Successfully dumped datasets to {}".format(obj_file), 'info')

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
                json_file.write(json.dumps(spatial_data) + '\n')
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

