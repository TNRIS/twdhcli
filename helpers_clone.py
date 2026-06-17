import json
from pathlib import Path

CANONICAL_CLONE_USERS = {
    "sysadmin": "sysadmin",
    "admin": "clone-admin",
    "editor": "clone-editor",
    "member": "clone-member",
}

S3_FILESTORE_BUCKET_URL = "https://twdh-s3filestore.s3.us-east-1.amazonaws.com"
SOURCE_FILESTORE_PREFIX = "dev"
DEST_FILESTORE_PREFIX = "docker-twdh-local"
PII_EMAIL_VALUE = "dipak.shetty@twdb.texas.gov"

PII_PACKAGE_EMAIL_FIELDS = {
    "author_email",
    # "maintainer_email",
    "data_contact_email",
}

def replace_package_email_fields(cleaned):
    """
    Replace dataset/application email fields from snapshot with canonical PII email.
    """
    for field in PII_PACKAGE_EMAIL_FIELDS:
        cleaned[field] = PII_EMAIL_VALUE

    return cleaned

def apply_dummy_defaults(cleaned):
    """
    Local clone defaults for fields required by TWDH validators.
    """

    if not cleaned.get("next_update"):
        cleaned["next_update"] = "2099-12-31"

    if not cleaned.get("data_admin_approved"):
        cleaned["data_admin_approved"] = "approved"

    return cleaned

def read_jsonl_file(path):
    records = []

    if not Path(path).exists():
        return records

    with open(path, "r") as json_file:
        for line in json_file:
            line = line.strip()
            if not line:
                continue

            records.append(json.loads(line))

    return records

def get_existing_canonical_user_ids(dest_twdh, logecho):

    canonical_user_ids = {}

    for role, username in CANONICAL_CLONE_USERS.items():
        try:
            user = dest_twdh.action.user_show(id=username)
            canonical_user_ids[role] = user["id"]

            logecho(
                "Using canonical {} user: {} ({})".format(
                    role,
                    username,
                    user["id"],
                ),
                "info",
            )

        except Exception as e:
            raise RuntimeError(
                "Missing canonical clone user '{}'. "
                "Create this user on destination before cloning. Error: {}".format(
                    username,
                    e,
                )
            )

    return canonical_user_ids


def get_source_owner_org_id(pkg):
    

    if pkg.get("owner_org"):
        return pkg.get("owner_org")

    organization = pkg.get("organization") or {}

    return organization.get("id") or organization.get("name")


def load_source_org_users(snapshot_dir):
    

    orgs_file = "{}/organizations.jsonl".format(snapshot_dir)
    orgs = read_jsonl_file(orgs_file)

    org_user_roles = {}

    for org in orgs:
        org_id = org.get("id")
        org_name = org.get("name")

        users_by_id = {}

        for user in org.get("users", []) or []:
            user_id = user.get("id")
            if not user_id:
                continue

            if user.get("sysadmin") is True:
                role = "sysadmin"
            else:
                role = user.get("capacity") or "member"

            if role not in ("sysadmin", "admin", "editor", "member"):
                role = "member"

            users_by_id[user_id] = role

        if org_id:
            org_user_roles[org_id] = users_by_id

        if org_name:
            org_user_roles[org_name] = users_by_id

    return org_user_roles


def get_source_package_creator_role(pkg, source_org_user_roles):
    """
    get source package creator's role inside the package owner org.
    """

    source_creator_id = pkg.get("creator_user_id")
    source_owner_org_id = get_source_owner_org_id(pkg)

    if not source_creator_id or not source_owner_org_id:
        return "member"

    users_by_id = source_org_user_roles.get(source_owner_org_id) or {}

    return users_by_id.get(source_creator_id, "member")


def get_canonical_creator_user_id(pkg, source_org_user_roles, canonical_user_ids):
    source_role = get_source_package_creator_role(pkg, source_org_user_roles)

    canonical_user_id = (
        canonical_user_ids.get(source_role)
        or canonical_user_ids["member"]
    )

    return canonical_user_id, source_role

def add_canonical_users_to_cloned_orgs(dest_twdh, snapshot_dir, logecho, report):
    orgs_file = "{}/organizations.jsonl".format(snapshot_dir)
    orgs = read_jsonl_file(orgs_file)

    role_to_org_role = {
        "sysadmin": "admin",
        "admin": "admin",
        "editor": "editor",
        "member": "member",
    }

    canonical_usernames = CANONICAL_CLONE_USERS

    for org in orgs:
        source_org_id = org.get("id")
        org_name = org.get("name")

        if not source_org_id or not org_name:
            continue

        cloned_org_id_or_name = org_name

        roles_needed = set()

        for user in org.get("users", []) or []:
            if user.get("sysadmin") is True:
                roles_needed.add("sysadmin")
                continue

            capacity = user.get("capacity") or "member"

            if capacity in ("admin", "editor", "member"):
                roles_needed.add(capacity)

        if not roles_needed:
            continue

        for source_role in sorted(roles_needed):
            username = canonical_usernames[source_role]
            org_role = role_to_org_role[source_role]

            try:
                dest_twdh.action.organization_member_create(
                    id=cloned_org_id_or_name,
                    username=username,
                    role=org_role,
                )

                logecho(
                    "Added canonical user {} as {} to org {}".format(
                        username,
                        org_role,
                        cloned_org_id_or_name,
                    ),
                    "info",
                )

            except Exception as e:
                message = str(e)

                if "already" in message.lower() or "duplicate" in message.lower():
                    logecho(
                        "Canonical user {} already has role {} in org {}, skipped.".format(
                            username,
                            org_role,
                            cloned_org_id_or_name,
                        ),
                        "warning",
                    )
                    continue

                report["organizations"]["failed"].append({
                    "name": cloned_org_id_or_name,
                    "stage": "canonical_user_membership",
                    "user": username,
                    "role": org_role,
                    "error": message,
                })

                logecho(
                    "Failed adding canonical user {} as {} to org {}: {}".format(
                        username,
                        org_role,
                        cloned_org_id_or_name,
                        message,
                    ),
                    "error",
                )

def uploaded_resource_url_for_destination(source_url):

    if not source_url:
        return None

    source_prefix = "{}/{}".format(
        S3_FILESTORE_BUCKET_URL,
        SOURCE_FILESTORE_PREFIX,
    )

    dest_prefix = "{}/{}".format(
        S3_FILESTORE_BUCKET_URL,
        DEST_FILESTORE_PREFIX,
    )

    if not source_url.startswith(source_prefix + "/"):
        return None

    return source_url.replace(source_prefix, dest_prefix, 1)

def restore_snapshot(dest_twdh, snapshot_dir, logecho):
    """
    Restore snapshot data into destination CKAN without wiping or purging.

    - creates missing groups
    - creates missing organizations
    - creates missing packages
    - creates missing resources
    - skips records that already exist
    - does not delete anything
    """
    logecho("Starting restore from snapshot.", "info")

    report = {
        "groups": {"created": 0, "skipped": 0, "failed": []},
        "organizations": {"created": 0, "skipped": 0, "failed": []},
        "datasets": {"created": 0, "skipped": 0, "failed": []},
        "applications": {"created": 0, "skipped": 0, "failed": []},
        "resources": {"created": 0, "skipped": 0, "failed": []},
        # "resource_views": {"created": 0, "skipped": 0, "failed": []},
        "data_dictionaries": {"created": 0, "skipped": 0, "failed": []},
        "spatial_extents": {"created": 0, "skipped": 0, "failed": []},
        "mappings": {
            "packages": {},
            "resources": {},
        },
    }

    canonical_user_ids = get_existing_canonical_user_ids(dest_twdh, logecho)
    source_org_user_roles = load_source_org_users(snapshot_dir)

    report["mappings"]["users"] = {
        "canonical": canonical_user_ids,
        "source_org_roles": source_org_user_roles,
    }

    restore_groups(dest_twdh, snapshot_dir, logecho, report)
    restore_organizations(dest_twdh, snapshot_dir, logecho, report)
    add_canonical_users_to_cloned_orgs(dest_twdh,snapshot_dir,logecho,report)
    restore_packages(dest_twdh, snapshot_dir, logecho, report, package_type="dataset")
    restore_packages(dest_twdh, snapshot_dir, logecho, report, package_type="application")
    restore_resources(dest_twdh, snapshot_dir, logecho, report)
    restore_data_dictionaries(dest_twdh, snapshot_dir, logecho, report)
    restore_spatial_extents(dest_twdh, snapshot_dir, logecho, report)

    logecho("Restore complete.", "info")
    return report

DROP_GROUP_FIELDS = {
    "id",
    "created",
    "display_name",
    "packages",
    "users",
    "groups",
    "num_followers",
    "package_count",
    "approval_status",
}


def clean_group_for_create(group):
    cleaned = dict(group)

    for field in DROP_GROUP_FIELDS:
        cleaned.pop(field, None)

    if cleaned.get("image_url") is None:
        cleaned.pop("image_url", None)

    return cleaned


def restore_groups(dest_twdh, snapshot_dir, logecho, report):
    groups_file = "{}/groups.jsonl".format(snapshot_dir)
    groups = read_jsonl_file(groups_file)

    logecho("Restoring groups from {}.".format(groups_file), "info")

    for group in groups:
        group_name = group.get("name")

        if not group_name:
            continue

        try:
            try:
                dest_twdh.action.group_show(id=group_name)
                report["groups"]["skipped"] += 1
                logecho("Group already exists, skipped: {}".format(group_name), "warning")
                continue
            except Exception:
                pass

            payload = clean_group_for_create(group)
            dest_twdh.action.group_create(**payload)

            report["groups"]["created"] += 1
            logecho("Created group: {}".format(group_name), "info")

        except Exception as e:
            report["groups"]["failed"].append({
                "name": group_name,
                "error": str(e),
            })
            logecho("Failed to create group {}: {}".format(group_name, e), "error")

DROP_ORG_FIELDS = {
    "id",
    "created",
    "display_name",
    "packages",
    "users",
    "groups",
    "num_followers",
    "package_count",
    "approval_status",
}

def normalize_spatial_value(value):
    if value in (None, "", {}, []):
        return None
    return value

def check_spatial_patch_succeeded(dest_twdh, package_id):
    """
    Sometimes package_patch returns 500, but spatial extras are already saved.
    Verify by reading package_show after the exception.
    """

    try:
        pkg = dest_twdh.action.package_show(id=package_id)

        if pkg.get("spatial_extent") or pkg.get("spatial_full") or pkg.get("spatial_extent_full"):
            return True

    except Exception:
        pass

    return False

def restore_spatial_extent_for_package(dest_twdh,package_name, new_package_id, spatial_data, logecho):
    """
    Restore spatial extent 
    """

    spatial_extent = None
    spatial_full = None

    if isinstance(spatial_data, dict):
        spatial_extent = (
            spatial_data.get("spatial_extent")
        )

        spatial_full = (
            spatial_data.get("spatial_extent_full")
        )

    spatial_extent = normalize_spatial_value(spatial_extent)
    spatial_full = normalize_spatial_value(spatial_full)

    if not spatial_extent and not spatial_full:
        return False

    payload = {
        "id": new_package_id,
    }

    if spatial_extent:
        payload["spatial_extent"] = spatial_extent

    if spatial_full:
        payload["spatial_full"] = json.dumps(spatial_full)

    try:
        dest_twdh.action.package_patch(**payload)
        return True
    except Exception as e:
        
        if check_spatial_patch_succeeded(dest_twdh, new_package_id):
            if logecho:
                logecho(
                    "Spatial extent appears restored for {}, ignoring package_patch 500: {}".format(
                        package_name or new_package_id,
                        e,
                    ),
                    "warning",
                )
            return True

        raise

def restore_spatial_extents(dest_twdh, snapshot_dir, logecho, report):
    spatial_file = "{}/spatial_data.jsonl".format(snapshot_dir)
    spatial_records = read_jsonl_file(spatial_file)

    logecho("Restoring spatial extents from {}.".format(spatial_file), "info")

    for record in spatial_records:
        if not isinstance(record, dict):
            report["spatial_extents"]["skipped"] += 1
            logecho(
                "Skipping invalid spatial row. Expected dict, got {}.".format(
                    type(record).__name__
                ),
                "warning",
            )
            continue

        old_package_id = record.get("package_id")
        package_name = record.get("package_name")
        spatial_data = record.get("spatial_data")
        if not package_name and isinstance(spatial_data, dict):
            package_name = spatial_data.get("name")

        # Backward compatibility if row itself is raw spatial object
        if not old_package_id and record.get("id"):
            old_package_id = record.get("id")
            spatial_data = record

        new_package_id = report["mappings"]["packages"].get(old_package_id)

        if not new_package_id:
            report["spatial_extents"]["skipped"] += 1
            logecho(
                "Skipping spatial extent for {}, package was not mapped.".format(
                    package_name or old_package_id
                ),
                "warning",
            )
            continue

        if not spatial_data:
            report["spatial_extents"]["skipped"] += 1
            logecho(
                "Skipping spatial extent for {}, no spatial data found.".format(
                    package_name or old_package_id
                ),
                "warning",
            )
            continue

        try:
            restored = restore_spatial_extent_for_package(
                dest_twdh,
                package_name,
                new_package_id,
                spatial_data,
                logecho
            )

            if not restored:
                report["spatial_extents"]["skipped"] += 1
                logecho(
                    "Skipping spatial extent for {}, no spatial_extent or spatial_full found.".format(
                        package_name or old_package_id
                    ),
                    "warning",
                )
                continue

            report["spatial_extents"]["created"] += 1
            logecho(
                "Restored spatial extent for package: {}".format(
                    package_name or new_package_id
                ),
                "info",
            )

        except Exception as e:
            report["spatial_extents"]["failed"].append({
                "old_package_id": old_package_id,
                "new_package_id": new_package_id,
                "package_name": package_name,
                "error": str(e),
            })

            logecho(
                "Failed to restore spatial extent for {}: {}".format(
                    package_name or new_package_id,
                    e,
                ),
                "error",
            )

def clean_organization_for_create(org):
    cleaned = dict(org)

    for field in DROP_ORG_FIELDS:
        cleaned.pop(field, None)

    if cleaned.get("image_url") is None:
        cleaned.pop("image_url", None)

    if not cleaned.get("description"):
        cleaned["description"] = "Cloned organization"

    if not cleaned.get("website_link"):
        cleaned["website_link"] = "https://example.com"

    return cleaned


def restore_organizations(dest_twdh, snapshot_dir, logecho, report):
    orgs_file = "{}/organizations.jsonl".format(snapshot_dir)
    orgs = read_jsonl_file(orgs_file)

    logecho("Restoring organizations from {}.".format(orgs_file), "info")

    for org in orgs:
        org_name = org.get("name")

        if not org_name:
            continue

        try:
            try:
                dest_twdh.action.organization_show(id=org_name,)
                report["organizations"]["skipped"] += 1
                logecho("Organization already exists, skipped: {}".format(org_name), "warning")
                continue
            except Exception:
                pass

            payload = clean_organization_for_create(org)
            dest_twdh.action.organization_create(**payload)

            report["organizations"]["created"] += 1
            logecho("Created organization: {}".format(org_name), "info")

        except Exception as e:
            report["organizations"]["failed"].append({
                "name": org_name,
                "error": str(e),
            })
            logecho("Failed to create organization {}: {}".format(org_name, e), "error")


DROP_PACKAGE_FIELDS = {
    "id",
    "metadata_created",
    "metadata_modified",
    "revision_id",
    "isopen",
    "num_resources",
    "num_tags",
    "organization",
    "relationships_as_object",
    "relationships_as_subject",
    "tracking_summary",
    "resources",
}

def first_value(value):
    if isinstance(value, list):
        if len(value) == 0:
            return None

        first = value[0]

        if isinstance(first, dict):
            return first.get("name") or first.get("value") or first.get("id")

        return first

    if isinstance(value, dict):
        return value.get("name") or value.get("value") or value.get("id")

    return value

def clean_package_for_create(pkg):
    cleaned = dict(pkg)

    for field in DROP_PACKAGE_FIELDS:
        cleaned.pop(field, None)

    for field in list(cleaned.keys()):
        if field.startswith("__"):
            cleaned.pop(field, None)

    cleaned = normalize_owner_org_for_create(pkg, cleaned)

    if cleaned.get("type") == "application":
        groups = pkg.get("groups") or []

        group_value = first_value(cleaned.get("group"))

        if not group_value:
            if groups and groups[0].get("name"):
                group_value = groups[0]["name"]
            else:
                group_value = "planning"

        cleaned["group"] = group_value

        valid_primary_tags = {
            "administrative",
            "agricultural",
            "boundaries",
            "climate",
            "conservation",
            "demographic",
            "economic",
            "environmental",
            "flood",
            "groundwater",
            "hydrology",
            "infrastructure",
            "planning",
            "regulatory",
            "surface_water",
            "water_quality",
            "water_use",
        }

        primary_tags = first_value(cleaned.get("primary_tags"))

        if not primary_tags or primary_tags not in valid_primary_tags:
            primary_tags = "administrative"

        cleaned["primary_tags"] = primary_tags

    else:
        cleaned.pop("group", None)

    cleaned = replace_package_email_fields(cleaned)
    cleaned = apply_dummy_defaults(cleaned)

    return cleaned

def normalize_owner_org_for_create(pkg, cleaned):
    
    organization = pkg.get("organization") or {}

    if organization.get("name"):
        cleaned["owner_org"] = organization["name"]

    return cleaned

def get_packages_from_snapshot(snapshot_dir, package_type):
    if package_type == "dataset":
        path = "{}/datasets.json".format(snapshot_dir)
    else:
        path = "{}/applications.json".format(snapshot_dir)

    if not Path(path).exists():
        return []

    with open(path, "r") as json_file:
        data = json.load(json_file)

    return data.get("results", [])


def restore_packages(dest_twdh, snapshot_dir, logecho, report, package_type):
    packages = get_packages_from_snapshot(snapshot_dir, package_type)

    logecho(
        "Restoring {} packages from snapshot.".format(package_type),
        "info",
    )

    report_key = "datasets" if package_type == "dataset" else "applications"

    for pkg in packages:
        old_package_id = pkg.get("id")
        package_name = pkg.get("name")

        if not package_name:
            continue

        try:
            try:
                existing = dest_twdh.action.package_show(id=package_name)
                report[report_key]["skipped"] += 1
                report["mappings"]["packages"][old_package_id] = existing.get("id")
                logecho(
                    "{} already exists, skipped: {}".format(package_type, package_name),
                    "warning",
                )
                continue
            except Exception:
                pass

            payload = clean_package_for_create(pkg)

            canonical_user_id, source_role = get_canonical_creator_user_id(
                pkg,
                report["mappings"]["users"]["source_org_roles"],
                report["mappings"]["users"]["canonical"],
            )

            payload["creator_user_id"] = canonical_user_id

            logecho(
                "Creating {} {} with owner_org={} creator_role={}".format(
                    package_type,
                    package_name,
                    payload.get("owner_org"),
                    source_role,
                ),
                "info",
            )

            created = dest_twdh.action.package_create(**payload)

            report[report_key]["created"] += 1
            report["mappings"]["packages"][old_package_id] = created.get("id")

            logecho("Created {}: {}".format(package_type, package_name), "info")

        except Exception as e:
            report[report_key]["failed"].append({
                "id": old_package_id,
                "name": package_name,
                "error": str(e),
            })
            logecho(
                "Failed to create {} {}: {}".format(package_type, package_name, e),
                "error",
            )


DROP_RESOURCE_FIELDS = {
    "id",
    "package_id",
    "created",
    "last_modified",
    "metadata_modified",
    "revision_id",
    "tracking_summary",
    "datastore_active",
    "cache_last_updated",
    "cache_url",
    "webstore_last_updated",
    "webstore_url",
    "group",
    "groups",
    "owner_org",
    "organization",
}


def clean_resource_for_create(resource, new_package_id):
    cleaned = dict(resource)

    for field in DROP_RESOURCE_FIELDS:
        cleaned.pop(field, None)

    cleaned.pop("group", None)
    cleaned.pop("groups", None)
    cleaned.pop("owner_org", None)
    cleaned.pop("organization", None)

    cleaned["package_id"] = new_package_id

    # CKAN requires url unless doing upload.
    if not cleaned.get("url"):
        cleaned["url"] = "https://example.com/placeholder-resource-url"

    return cleaned


def resource_exists(dest_twdh, package_id, resource_name):
    try:
        package = dest_twdh.action.package_show(id=package_id)
    except Exception:
        return None

    for resource in package.get("resources", []):
        if resource.get("name") == resource_name:
            return resource

    return None


def restore_resources_for_package_list(dest_twdh, packages, logecho, report, package_type):
    for pkg in packages:
        old_package_id = pkg.get("id")
        new_package_id = report["mappings"]["packages"].get(old_package_id)
        package_name = pkg.get("name")

        if not new_package_id:
            logecho(
                "Skipping resources for {}, package was not created/mapped.".format(package_name),
                "warning",
            )
            continue

        for resource in pkg.get("resources", []) or []:
            old_resource_id = resource.get("id")
            resource_name = resource.get("name") or resource.get("id")

            try:
                existing_resource = resource_exists(
                    dest_twdh,
                    new_package_id,
                    resource_name,
                )

                if existing_resource:
                    report["resources"]["skipped"] += 1
                    report["mappings"]["resources"][old_resource_id] = existing_resource.get("id")
                    logecho(
                        "Resource already exists, skipped: {} / {}".format(
                            package_name,
                            resource_name,
                        ),
                        "warning",
                    )
                    continue

                payload = clean_resource_for_create(resource, new_package_id)

                if old_resource_id:
                    payload["id"] = old_resource_id

                if resource.get("url_type") == "upload":
                    dest_upload_url = uploaded_resource_url_for_destination(
                        resource.get("dest_twdh.action.package_patch(**payload)url")
                    )

                    if dest_upload_url:
                        payload["url"] = dest_upload_url
                        payload["url_type"] = "upload"

                        logecho(
                            "Rewriting upload URL for {} / {} to backup-clone S3 path.".format(
                                package_name,
                                resource_name,
                            ),
                            "info",
                        )

                created = dest_twdh.action.resource_create(**payload)

                created_resource_id = created.get("id")

                report["resources"]["created"] += 1
                report["mappings"]["resources"][old_resource_id] = created_resource_id

                logecho(
                    "Created resource: {} / {} with id={}".format(
                        package_name,
                        resource_name,
                        created_resource_id,
                    ),
                    "info",
                )

            except Exception as e:
                report["resources"]["failed"].append({
                    "package": package_name,
                    "resource": resource_name,
                    "old_resource_id": old_resource_id,
                    "error": str(e),
                })
                logecho(
                    "Failed to create resource {} / {}: {}".format(
                        package_name,
                        resource_name,
                        e,
                    ),
                    "error",
                )

def extract_data_dictionary_fields(data_dictionary):
    

    if isinstance(data_dictionary, list):
        return data_dictionary

    if isinstance(data_dictionary, dict):
        return (
            data_dictionary.get("data-dictionary")
            or data_dictionary.get("fields")
            or []
        )

    return []

def restore_data_dictionary_for_resource(dest_twdh, new_resource_id, data_dictionary):
    

    dictionary_fields = extract_data_dictionary_fields(data_dictionary)

    fields = []

    for item in dictionary_fields:
        if not isinstance(item, dict):
            continue

        field_id = item.get("id")
        if not field_id:
            continue

        field_type = item.get("type") or "text"

        info = item.get("info")
        if not isinstance(info, dict):
            info = {}


        if isinstance(info.get("_info"), dict):
            info = info["_info"]

        fields.append({
            "id": field_id,
            "type": field_type,
            "info": info,
        })

    if not fields:
        return False

    dest_twdh.action.datastore_create(
        resource_id=new_resource_id,
        force=True,
        fields=fields,
    )

    return True

def restore_data_dictionaries(dest_twdh, snapshot_dir, logecho, report):
    dd_file = "{}/data-dicts.jsonl".format(snapshot_dir)
    dd_records = read_jsonl_file(dd_file)

    logecho("Restoring data dictionaries from {}.".format(dd_file), "info")

    for record in dd_records:
        if not isinstance(record, dict) or "resource_id" not in record:
            report["data_dictionaries"]["skipped"] += 1
            logecho(
                "Skipping old-format data dictionary row. Re-run snapshot to include resource_id mapping.",
                "warning",
            )
            continue

        old_resource_id = record.get("resource_id")
        resource_name = record.get("resource_name")
        package_name = record.get("package_name")

        new_resource_id = report["mappings"]["resources"].get(old_resource_id)
        data_dictionary = record.get("data_dictionary") or {}

        if not new_resource_id:
            report["data_dictionaries"]["skipped"] += 1
            logecho(
                "Skipping data dictionary for {} / {}, resource was not mapped.".format(
                    package_name,
                    resource_name or old_resource_id,
                ),
                "warning",
            )
            continue

        dictionary_fields = extract_data_dictionary_fields(data_dictionary)

        if not dictionary_fields:
            report["data_dictionaries"]["skipped"] += 1
            logecho(
                "Skipping data dictionary for {} / {}, no fields found.".format(
                    package_name,
                    resource_name or old_resource_id,
                ),
                "warning",
            )
            continue

        try:
            restored = restore_data_dictionary_for_resource(
                dest_twdh,
                new_resource_id,
                data_dictionary,
            )

            if not restored:
                report["data_dictionaries"]["skipped"] += 1
                logecho(
                    "Skipping data dictionary for {} / {}, no valid fields after cleanup.".format(
                        package_name,
                        resource_name or old_resource_id,
                    ),
                    "warning",
                )
                continue

            report["data_dictionaries"]["created"] += 1
            logecho(
                "Restored data dictionary for {} / {} with {} fields.".format(
                    package_name,
                    resource_name or new_resource_id,
                    len(dictionary_fields),
                ),
                "info",
            )

        except Exception as e:
            report["data_dictionaries"]["failed"].append({
                "old_resource_id": old_resource_id,
                "new_resource_id": new_resource_id,
                "error": str(e),
            })

            logecho(
                "Failed to restore data dictionary for resource {}: {}".format(
                    new_resource_id,
                    e,
                ),
                "error",
            )

def restore_resources(dest_twdh, snapshot_dir, logecho, report):
    dataset_packages = get_packages_from_snapshot(snapshot_dir, "dataset")
    application_packages = get_packages_from_snapshot(snapshot_dir, "application")

    restore_resources_for_package_list(
        dest_twdh,
        dataset_packages,
        logecho,
        report,
        "dataset",
    )

    restore_resources_for_package_list(
        dest_twdh,
        application_packages,
        logecho,
        report,
        "application",
    )

def validate_clone_basic(dest_twdh, snapshot_dir, logecho):
    report = {
        "groups": {"snapshot": 0, "found": 0, "missing": []},
        "organizations": {"snapshot": 0, "found": 0, "missing": []},
        "datasets": {"snapshot": 0, "found": 0, "missing": []},
        "applications": {"snapshot": 0, "found": 0, "missing": []},
        "resources": {"snapshot": 0, "found": 0, "missing": []},
    }

    # Groups
    groups = read_jsonl_file("{}/groups.jsonl".format(snapshot_dir))
    report["groups"]["snapshot"] = len(groups)

    for group in groups:
        name = group.get("name") or group.get("id")
        try:
            dest_twdh.action.group_show(id=name)
            report["groups"]["found"] += 1
        except Exception:
            report["groups"]["missing"].append(name)

    # Organizations
    orgs = read_jsonl_file("{}/organizations.jsonl".format(snapshot_dir))
    report["organizations"]["snapshot"] = len(orgs)

    for org in orgs:
        name = org.get("name") or org.get("id")
        try:
            dest_twdh.action.organization_show(id=name)
            report["organizations"]["found"] += 1
        except Exception:
            report["organizations"]["missing"].append(name)

    # Datasets
    datasets = get_packages_from_snapshot(snapshot_dir, "dataset")
    report["datasets"]["snapshot"] = len(datasets)

    for pkg in datasets:
        name = pkg.get("name") or pkg.get("id")
        try:
            cloned_pkg = dest_twdh.action.package_show(id=name)
            report["datasets"]["found"] += 1

            snapshot_resources = pkg.get("resources", []) or []
            report["resources"]["snapshot"] += len(snapshot_resources)

            dest_resource_ids = {
                r.get("id")
                for r in cloned_pkg.get("resources", []) or []
            }

            for resource in snapshot_resources:
                old_resource_id = resource.get("id")

                if old_resource_id in dest_resource_ids:
                    report["resources"]["found"] += 1
                else:
                    report["resources"]["missing"].append({
                        "package": name,
                        "resource": resource.get("name") or old_resource_id,
                        "id": old_resource_id,
                    })

        except Exception:
            report["datasets"]["missing"].append(name)

    # Applications
    applications = get_packages_from_snapshot(snapshot_dir, "application")
    report["applications"]["snapshot"] = len(applications)

    for pkg in applications:
        name = pkg.get("name") or pkg.get("id")
        try:
            cloned_pkg = dest_twdh.action.package_show(id=name)
            report["applications"]["found"] += 1

            snapshot_resources = pkg.get("resources", []) or []
            report["resources"]["snapshot"] += len(snapshot_resources)

            dest_resource_ids = {
                r.get("id")
                for r in cloned_pkg.get("resources", []) or []
            }

            for resource in snapshot_resources:
                old_resource_id = resource.get("id")

                if old_resource_id in dest_resource_ids:
                    report["resources"]["found"] += 1
                else:
                    report["resources"]["missing"].append({
                        "package": name,
                        "resource": resource.get("name") or old_resource_id,
                        "id": old_resource_id,
                    })

        except Exception:
            report["applications"]["missing"].append(name)

    logecho(
        "Clone validation report: {}".format(
            json.dumps(report, indent=2)
        ),
        "info",
    )

    return report