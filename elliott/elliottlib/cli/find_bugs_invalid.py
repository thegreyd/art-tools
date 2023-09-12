import json
import click
import requests
import re
from elliottlib import (Runtime, logutil, constants)
from elliottlib.cli import common
from elliottlib.cli.find_bugs_sweep_cli import FindBugsSweep

logger = logutil.getLogger(__name__)


@common.cli.command("find-bugs:invalid-trackers", short_help="Find ART managed jira tracker bugs that are missing or "
                                                             "have incorrect labels ")
@click.option('--fix', is_flag=True, default=False, help='Attempt to fix the bugs by adding missing labels')
@click.pass_obj
@common.click_coroutine
async def find_bugs_invalid_cli(runtime: Runtime, fix):
    """Find ART managed jira tracker bugs that are missing or have incorrect labels
    And optionally fix them

    Only to be used with --assembly=stream

    $ elliott -g openshift-4.12 find-bugs:invalid-trackers

    """
    if runtime.assembly != 'stream':
        raise click.BadParameter("This command is intended to work only with --assembly=stream",
                                 param_hint='--assembly')

    runtime.initialize()
    find_bugs_obj = FindBugsSweep()
    bug_tracker = runtime.get_bug_tracker('jira')

    bugs = find_bugs_obj.search(bug_tracker_obj=bug_tracker, verbose=runtime.debug)

    def has_cve_in_summary(b):
        return bool(re.search(r'CVE-\d+-\d+', b.summary))

    look_like_trackers = [b for b in bugs if has_cve_in_summary(b) and 'art:cloned-kernel-bug' not in b.keywords]
    report = {}
    actionable_bugs = {}
    for b in look_like_trackers:
        # "CVE-2022-23525 CVE-2022-23526 special-resource-operator-container: various flaws [openshift-4]"
        # so we want to match CVE-2022-23525, CVE-2022-23526 into a list
        # and get special-resource-operator-container as the component

        match = re.search(r'((?:CVE-\d+-\d+ )+)((?:\w+-?)+)', b.summary)
        if not match:
            print(f"Skipping {b.id} - {b.summary} - could not parse cve from summary")
            continue
        component = match.group(2)
        cve_list = [c.strip() for c in match.group(1).split()]

        cve_url = "https://access.redhat.com/hydra/rest/securitydata/cve/{cve_name}.json"

        flaw_ids = []
        for cve in cve_list:
            url = cve_url.format(cve_name=cve)
            response = requests.get(url)
            response.raise_for_status()
            flaw_id = response.json()['bugzilla']['id']
            flaw_ids.append(int(flaw_id))

        issues = []
        labels = []
        if not b.is_tracker_bug():
            issues.append("Missing tracker labels.")
        if not b.whiteboard_component:
            issues.append("Missing component label.")
            labels.append(f"pscomponent:{component}")
        if not b.corresponding_flaw_bug_ids:
            issues.append("Missing flaw bug label.")
            labels.extend([f"flaw:bz#{i}" for i in flaw_ids])
        elif not set(flaw_ids).issubset(set(b.corresponding_flaw_bug_ids)):
            issues.append(f"Flaw bug labels not found. Expected: {flaw_ids} to be in {b.corresponding_flaw_bug_ids}.")

        if issues:
            report[b.id] = {'issues': ' '.join(issues), 'labels': labels}
            actionable_bugs[b.id] = b

    click.echo(f'Found {len(report)} bugs that are invalid')
    click.echo(json.dumps(report, indent=4))
    click.echo(f'If tracker labels are missing, add them manually as needed as that is a risky operation. Tracker '
               f'labels are: {constants.TRACKER_BUG_KEYWORDS}')

    if not fix:
        return

    for bug_id, data in report.items():
        labels = data['labels']
        if labels:
            bug = actionable_bugs[bug_id]
            click.echo(f'{bug.id}({bug.status}) - {bug.summary}')
            if click.confirm(f"Add labels {labels} to bug {bug_id}?"):
                bug = actionable_bugs[bug_id]
                bug.bug.fields.labels.extend(labels)
                bug.bug.update(fields={"labels": bug.bug.fields.labels})
                click.echo("Added labels")
