import json
from typing import Dict, List, Set

import click
from artcommonlib import logutil

from elliottlib import Runtime, constants
from elliottlib.bzutil import Bug
from elliottlib.cli import common
from elliottlib.cli.common import click_coroutine
from elliottlib.cli.find_bugs_sweep_cli import (
    FindBugsSweep,
    categorize_bugs_by_type,
    get_assembly_bug_ids,
    get_bugs_sweep,
)

LOGGER = logutil.get_logger(__name__)
type_bug_list = List[Bug]
type_bug_set = Set[Bug]


@common.cli.command("find-bugs", short_help="Find eligible bugs for the given assembly")
@click.option(
    "--advance-release", is_flag=True, help="Indicate that the assembly contains an advance advisory for release"
)
@click.option(
    "--filter-attached/--no-filter-attached",
    is_flag=True,
    default=True,
    help="Try and filter out bugs that are attached/associated with other advisories",
)
@click.option(
    "--permissive",
    is_flag=True,
    default=False,
    help="Ignore bugs that are determined to be invalid and continue",
)
@click.option(
    '--output',
    '-o',
    type=click.Choice(['json']),
    help='Output in the specified format',
)
@click.pass_obj
@click_coroutine
async def find_bugs_cli(
    runtime: Runtime,
    advance_release,
    filter_attached,
    permissive,
    output,
):
    """Find OCP bugs for the given group and assembly, eligible for release.

    The --group and --assembly sets the criteria for the bugs to be found.
    default jira search statuses: ['MODIFIED', 'ON_QA', 'VERIFIED']
    By default, bugs that are already attached to other advisories are filtered out (--filter-attached)

    Security Tracker Bugs are validated and categorized based on attached builds
    to advisories that are in the assembly. The assumption is that:
    - For every tracker bug, there is a corresponding build attached to an advisory in the assembly.
    - The tracker bug will follow the advisory kind of that corresponding build.
    To disable, use --permissive

    Find bugs for all advisory types in assembly and output them in JSON:

    \b
        $ elliott -g openshift-4.18 --assembly 4.18.5 find-bugs -o json

    """
    cli = FindBugsCli(
        runtime=runtime,
        advance_release=advance_release,
        filter_attached=filter_attached,
        permissive=permissive,
        output=output,
    )
    await cli.run()


class FindBugsCli:
    def __init__(
        self,
        runtime: Runtime,
        advance_release: bool,
        filter_attached: bool,
        permissive: bool,
        output: str,
    ):
        self.runtime = runtime
        self.advance_release = advance_release
        self.filter_attached = filter_attached
        self.permissive = permissive
        self.output = output
        self.bug_tracker = None

    async def run(self):
        self.runtime.initialize(mode="both")
        self.bug_tracker = self.runtime.get_bug_tracker('jira')

        bugs_dict: type_bug_list = await self.find_bugs()

        bugs = []
        for bug_set in bugs_dict.values():
            bugs.extend([b.id for b in bug_set])

        if self.output == 'json':
            bugs_dict_formatted = {key: sorted([b.id for b in bug_set]) for key, bug_set in bugs_dict.items()}
            click.echo(json.dumps(bugs_dict_formatted, indent=4))
        else:
            click.echo(f"Found {len(bugs)} bugs")
            if bugs:
                click.echo(", ".join(sorted(bugs)))

    async def find_bugs(self) -> Dict[str | int, type_bug_set]:
        """Find bugs based on the given find_bugs_obj and bug_tracker.
        Attach them to the advisory if specified.

        returns a dict of
        - {kind: bugs} if bugs are not requested to be attached, where kind is the advisory type for which bugs were found e.g "rpm", "image", "extras", "microshift"
        - {advisory_id: bugs} if bugs are requested to be attached, where advisory_id is the advisory for which bugs were found and attached
        """

        find_bugs_obj = FindBugsSweep()
        statuses = sorted(find_bugs_obj.status)
        tr = self.bug_tracker.target_release()
        LOGGER.info(f"Searching {self.bug_tracker.type} for bugs with status {statuses} and target releases: {tr}\n")

        bugs = await get_bugs_sweep(self.runtime, find_bugs_obj, self.bug_tracker)
        advisory_ids = self.runtime.get_default_advisories()
        included_bug_ids, _ = get_assembly_bug_ids(self.runtime, bug_tracker_type=self.bug_tracker.type)
        major_version, _ = self.runtime.get_major_minor()
        bugs_by_type, _ = categorize_bugs_by_type(
            bugs,
            advisory_ids,
            included_bug_ids,
            permissive=self.permissive,
            major_version=major_version,
            advance_release=self.advance_release,
        )
        for kind, kind_bugs in bugs_by_type.items():
            LOGGER.info(f'{kind} bugs: {[b.id for b in kind_bugs]}')

        return bugs_by_type
