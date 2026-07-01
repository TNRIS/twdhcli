
import json


def wipe_destination(dest_twdh, logecho):
    """
    Wipe existing data from local/DEV using CKAN API delete/purge endpoints.
    """
    logecho("Starting data wipe using CKAN API delete/purge endpoints.", "warning")

    wipe_report = {
        "applications": {"purged": 0, "failed": []},
        "datasets": {"purged": 0, "failed": []},
        "organizations": {"purged": 0, "failed": []},
        "groups": {"purged": 0, "failed": []},
    }

    wipe_report["applications"] = purge_packages_by_type(
        dest_twdh,
        logecho,
        package_type="application",
    )

    wipe_report["datasets"] = purge_packages_by_type(
        dest_twdh,
        logecho,
        package_type="dataset",
    )

    wipe_report["organizations"] = purge_organizations(
        dest_twdh,
        logecho,
    )

    logecho(
        "Running second organization purge pass.",
        "warning",
    )

    second_org_pass = purge_organizations(
        dest_twdh,
        logecho,
    )

    wipe_report["organizations"]["purged"] += second_org_pass["purged"]
    wipe_report["organizations"]["failed"].extend(second_org_pass["failed"])

    wipe_report["groups"] = purge_groups(
        dest_twdh,
        logecho,
    )

    logecho("Data wipe complete.", "celebration")
    logecho("Wipe report: {}".format(json.dumps(wipe_report, indent=2)), "info")

    return wipe_report

def purge_packages_by_type(dest_twdh, logecho, package_type):
    """
    Delete and purge all CKAN packages of a given type.

    """
    report = {
        "purged": 0,
        "failed": [],
    }

    logecho("Purging CKAN packages of type '{}'.".format(package_type), "warning")

    try:
        results = dest_twdh.action.package_search(
            fq="type:{}".format(package_type),
            rows=100000,
            include_deleted=True,
            include_drafts=True,
            include_private=True,
        )

    except Exception as e:
        report["failed"].append({
            "stage": "package_search",
            "type": package_type,
            "error": str(e),
        })
        logecho("Unable to search {} packages: {}".format(package_type, e), "error")
        return report

    packages = results.get("results", [])

    for package in packages:
        package_id = package.get("id")
        package_name = package.get("name")

        if not package_id:
            continue

        try:
            try:
                dest_twdh.action.package_delete(id=package_id)
            except Exception as delete_error:
                logecho(
                    "package_delete warning for {} '{}': {}".format(
                        package_type,
                        package_name,
                        delete_error,
                    ),
                    "warning",
                )

            dest_twdh.action.dataset_purge(id=package_id)

            report["purged"] += 1
            logecho(
                "Purged {}: {}".format(package_type, package_name),
                "info",
            )

        except Exception as e:
            report["failed"].append({
                "id": package_id,
                "name": package_name,
                "error": str(e),
            })
            logecho(
                "Failed to purge {} '{}': {}".format(
                    package_type,
                    package_name,
                    e,
                ),
                "error",
            )

    logecho(
        "Finished purging {} packages. Purged: {}, Failed: {}".format(
            package_type,
            report["purged"],
            len(report["failed"]),
        ),
        "info",
    )

    return report

def purge_organizations(dest_twdh, logecho):
    """
    Delete and purge all organizations.
    """
    report = {
        "purged": 0,
        "failed": [],
    }

    logecho("Purging organizations.", "warning")

    try:
        organizations = dest_twdh.action.organization_list(
            all_fields=True,
            include_extras=True,
            include_users=False,
            include_groups=False,
        )

    except Exception as e:
        report["failed"].append({
            "stage": "organization_list",
            "error": str(e),
        })
        logecho("Unable to list organizations: {}".format(e), "error")
        return report

    for organization in organizations:
        org_id = organization.get("id")
        org_name = organization.get("name")

        if not org_id:
            continue

        try:
            try:
                dest_twdh.action.organization_delete(id=org_id)
            except Exception as delete_error:
                logecho(
                    "organization_delete warning for '{}': {}".format(
                        org_name,
                        delete_error,
                    ),
                    "warning",
                )

            dest_twdh.action.organization_purge(id=org_id)

            report["purged"] += 1
            logecho("Purged organization: {}".format(org_name), "info")

        except Exception as e:
            report["failed"].append({
                "id": org_id,
                "name": org_name,
                "error": str(e),
            })
            logecho(
                "Failed to purge organization '{}': {}".format(org_name, e),
                "error",
            )

    logecho(
        "Finished purging organizations. Purged: {}, Failed: {}".format(
            report["purged"],
            len(report["failed"]),
        ),
        "info",
    )

    return report

def purge_groups(dest_twdh, logecho):
    """
    Delete and purge all groups.
    """
    report = {
        "purged": 0,
        "failed": [],
    }

    logecho("Purging groups.", "warning")

    try:
        groups = dest_twdh.action.group_list(
            all_fields=True,
            include_extras=True,
            include_users=False,
            include_groups=False,
        )

    except Exception as e:
        report["failed"].append({
            "stage": "group_list",
            "error": str(e),
        })
        logecho("Unable to list groups: {}".format(e), "error")
        return report

    for group in groups:
        group_id = group.get("id")
        group_name = group.get("name")

        if not group_id:
            continue

        try:
            try:
                dest_twdh.action.group_delete(id=group_id)
            except Exception as delete_error:
                logecho(
                    "group_delete warning for '{}': {}".format(
                        group_name,
                        delete_error,
                    ),
                    "warning",
                )

            dest_twdh.action.group_purge(id=group_id)

            report["purged"] += 1
            logecho("Purged group: {}".format(group_name), "info")

        except Exception as e:
            report["failed"].append({
                "id": group_id,
                "name": group_name,
                "error": str(e),
            })
            logecho(
                "Failed to purge group '{}': {}".format(group_name, e),
                "error",
            )

    logecho(
        "Finished purging groups. Purged: {}, Failed: {}".format(
            report["purged"],
            len(report["failed"]),
        ),
        "info",
    )

    return report
