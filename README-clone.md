# TWDH Clone Process

1. Verify hardcoded values
   - In helpers.py, check:

     ```python
     PROD_HOSTS = {
         "txwaterdatahub.org",
         "www.txwaterdatahub.org"
     }
     ```

   - In helpers-clone.py, check:

     ```python

     # Items from the clone source will be assigned to
     # these users on the clone destination
     CANONICAL_CLONE_USERS = {
         "sysadmin": "sysadmin",
         "admin": "clone-admin",
         "editor": "clone-editor",
         "member": "clone-member"
     }

     # S3 Base
     S3_FILESTORE_BUCKET_URL = "https://twdh-s3filestore.s3.us-east-1.amazonaws.com"

     # S3 Source and Destination
     # For PROD -> DEV
     SOURCE_FILESTORE_PREFIX = "prod"
     DEST_FILESTORE_PREFIX = "dev"

     # For DEV -> Local
     # SOURCE_FILESTORE_PREFIX = "dev"
     # DEST_FILESTORE_PREFIX = "docker-twdh-local"

     # PII email fields and what to replace them with
     PII_PACKAGE_EMAIL_FIELDS = {
         "author_email",
         "data_contact_email"
     }
     PII_EMAIL_VALUE = "dipak.shetty@twdb.texas.gov"

     DEFAULT_VALUES = {
       "next_update": "2099-12-31",
      "data_admin_approved": "approved"
     }
     ```

     Make sure destination users already exist:
     - sysadmin
     - clone-admin
     - clone-editor
     - clone-member

     Also,

     ```python

     DEFAULT_VALUES = {
       "next_update" = "2099-12-31",
       "data_admin_approved" = "approved"
     }
     ```

     These default values are used when source package data is missing that validators require.

2. Snapshot clone source deployment (DEV/PROD)

   > <em>First confirm all datasets are indexed on source deployment using `ckan -c production.ini search-index check`</em>

   Update .env to point to source deployment (DEV/PROD):

   ```bash
     host=https://txwaterdatahub.org
     apikey=<PROD_API_KEY>

     # host=https://dev.txwaterdatahub.org
     # apikey=<DEV_API_KEY>

   ```

   `python twdhcli.py snapshot`

3. Copy S3 uploaded files

   Copy source uploaded resource files to local clone prefix:

   s3://twdh-s3filestore/dev/resources/ to
   s3://twdh-s3filestore/docker-twdh-local/resources/

4. Switch .env to destination local

   ```bash
   host=http://172.19.0.1:5000
   apikey=<LOCAL_API_KEY>
   ```

5. Wipe destination

   This removes datasets, applications, orgs, and groups.

   ```python
   python twdhcli.py wipe-data --confirm-wipe
   ```

   Optional: Confirm wipe by looking at the database.

6. Run clone

   ```python
   python twdhcli.py clone \
   --dest-host http://local.txwaterdatahub.org:5000 \
   --snapshot-dir ./twdh-snapshots/dev.txwaterdatahub.org_2026-06-16_12-49-51/ \
   > output.txt
   ```

7. Resubmit Datapusher+ resources to Datastore

   ```bash
   ckan -c production.ini datapusher resubmit --yes
   ```

For spatial patch errors, the clone checks the package again after the error. If spatial_extent, spatial_full exists on the package, the error is treated as a false failure and ignored.
