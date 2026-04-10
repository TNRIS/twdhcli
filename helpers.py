import os
import sys
import csv
import json
import subprocess

from datetime import datetime, date

from pathlib import Path
from urllib.parse import urlparse

from shapely import from_geojson, to_geojson
from shapely.geometry import shape, mapping, MultiPolygon, Polygon
from shapely.ops import unary_union


def snapshot(ctx,dest):

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

    ##########################################
    # Create spatial stats report
    ##########################################
    spatial_stats( ctx, [], '{}/spatial-stats.csv'.format( snap_dest ), True )

    ##########################################
    # Create human readable dataset and 
    # application backups
    ##########################################
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

    ##########################################
    # Create resource 'data dictionary' backup
    ##########################################
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
                        json_file.write(json.dumps(dd) + '\n')

        logecho( 'Created snapshot file: {}'.format(dd_file), 'info' )

    except FileNotFoundError:
        logecho( "Unable to write JSON / Destination not found error", 'error' )
        sys.exit(1)
    except Exception as e:
        logecho( "An unexpected error occurred: {}".format(e), 'error' )
        sys.exit(1)



    ##########################################
    # Create resource 'views' backup
    ##########################################
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

    ##########################################
    # Create JSONL backups of datasets, 
    # applications, organizations, users. 
    # Datasets include type 'dataset' and 
    # 'application' all in the same file.
    ##########################################
    obj_types = [ 
        'datasets', 
        'groups', 
        'organizations', 
        'users'
    ]
    for obj_type in obj_types:
        obj_file = '{}/{}.jsonl'.format(snap_dest, obj_type)
        try:

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


def spatial_stats(ctx, ids, csvout, quiet):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    datasets = fetch_datasets(ctx, ids)

    dataset_count = 0
    spatial_dataset_count = 0
    nonspatial_dataset_count = 0
    spatial_full_total = 0
    spatial_simp_total = 0

    logecho( "", "divider" )

    csvdata = [['id','name','spatial_full_size','spatial_simp_size','spatial_simp_reduction']]
    for dataset in datasets:
        dataset_count += 1
        spatial_full_size = 0
        spatial_simp_size = 0
        spatial_simp_reduction = 0
        if "gazetteer" in dataset:
            if dataset["gazetteer"]["spatial_full"] is not None:
                spatial_full_size = len(dataset["gazetteer"]["spatial_full"].encode('utf-8'))
                spatial_full_total += spatial_full_size
            else:
                spatial_full_size = 0

            if dataset["gazetteer"]["spatial_simp"] is not None:
                spatial_simp_size = len(dataset["gazetteer"]["spatial_simp"].encode('utf-8'))
                spatial_simp_total += spatial_simp_size
            else:
                spatial_simp_size = 0

            if dataset["gazetteer"]["spatial_full"] is not None or dataset["gazetteer"]["spatial_simp"] is not None:
                spatial_dataset_count += 1
                if spatial_full_size > 0:
                  spatial_simp_reduction = '{}%'.format(round(( 100 - ( ( spatial_simp_size / spatial_full_size ) * 100 ) ), 2))
                else:
                  spatial_simp_reduction = 'n/a'
                logecho("{} / spatial_full: {} / spatial_simp: {} / reduction: {}".format(dataset["name"], spatial_full_size, spatial_simp_size, spatial_simp_reduction ), "info")

            else:
                nonspatial_dataset_count += 1
                spatial_simp_reduction = 0

        else:
            nonspatial_dataset_count += 1

    
        csvdata.append( [dataset['id'], dataset['name'], spatial_full_size, spatial_simp_size, spatial_simp_reduction] )

    logecho( "", "divider" )
    logecho("{} spatial datasets".format(spatial_dataset_count), "info")
    csvdata.insert(0,["# {} spatial datasets".format(spatial_dataset_count)])
    logecho("{} nonspatial datasets".format(nonspatial_dataset_count), "info")
    csvdata.insert(1,["# {} nonspatial datasets".format(nonspatial_dataset_count)])
    logecho("spatial_full_total = {} bytes".format(spatial_full_total), "info")
    csvdata.insert(2,["# spatial_full_total = {} bytes".format(spatial_full_total)])
    logecho("spatial_simp_total = {} bytes".format(spatial_simp_total), "info")
    csvdata.insert(3,["# spatial_simp_total = {} bytes".format(spatial_simp_total)])


    if spatial_full_total > 0:
        simplification_reduction = 100 - ( ( spatial_simp_total / spatial_full_total ) * 100 )
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


def simplify_geojson_by_size(ctx, json_data, max_bytes, tolerance_step=0.0001):

    twdh = ctx.obj['twdh']
    logecho = ctx.obj['logecho']

    try:
        data = json.loads(json_data)
    except (json.JSONDecodeError, AttributeError, IndexError, TypeError) as e:
        log.error(f"Error processing json data: {e}")
        return json_data

    tolerance = 0.0
    current_size = len(json_data.encode('utf-8'))
    orig_size = current_size
    
    while current_size > max_bytes and tolerance < 0.25: # Max 0.25 tolerance
        tolerance += tolerance_step
        new_features = []
        for feature in data['features']:
            geom = shape(feature['geometry'])
            # Simplify geometry
            simplified_geom = geom.simplify(tolerance, preserve_topology=True)
            
            # Update feature
            new_feature = feature.copy()
            new_feature['geometry'] = mapping(simplified_geom)
            new_features.append(new_feature)
            
        new_data = {'type': 'FeatureCollection', 'features': new_features}
        # Serialize with low precision to save bytes
        json_str = json.dumps(new_data, separators=(',', ':'))
        current_size = len(json_str.encode('utf-8'))
        
        #log.info( "-=+=-=+=-=+=-=+=-=+=-=+=-=+=-=+=-")
        if current_size <= max_bytes:
            reduction = 100 - (( current_size / orig_size ) * 100)
            logecho(f"Original Size: {orig_size} bytes / Final size: {current_size} bytes / Reduction: {round(reduction, 2)}% / Tolerance: {round(tolerance, 4)}", 'info')
            return json_str

    logecho("Could not reach target size without losing too much detail. Current size={}".format(current_size), 'info')
    return json_data
