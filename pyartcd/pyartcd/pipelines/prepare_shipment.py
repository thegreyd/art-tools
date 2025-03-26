import click

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from artcommonlib import exectools


class PrepareShipmentPipeline:
    """ Prepare Shipment config for a Konflux release """

    def __init__(self,
                 runtime: Runtime,
                 group: str,
                 assembly: str,
                 application: str,
                 advisory_key: str):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.application = application
        self.advisory_key = advisory_key

        self._working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self._working_dir / "elliott-working"

        self._elliott_base_command = [
            'elliott',
            f'--group={self.group}',
            f'--assembly={self.assembly}',
            f'--working-dir={self.elliott_working_dir}',
        ]

    async def run(self):
        await self.init_shipment()

    async def init_shipment(self) -> int:
        cmd = self._elliott_base_command + [
            "shipment",
            "init",
            "--application", self.application,
            "--advisory-key", self.advisory_key
        ]
        await exectools.cmd_assert_async(cmd)


@cli.command("prepare-shipment")
@click.option("-g", "--group", metavar='GROUP', required=True,
              help="The group to operate on. e.g. openshift-4.18")
@click.option("--assembly", metavar="ASSEMBLY", required=True,
              help="The name of the associated assembly e.g. 4.18.1")
@click.option("--application", metavar="APPLICATION", required=True,
              help="Name of the Konflux application that the shipment is for")
@click.option("--advisory-key", metavar="ADVISORY_KEY",
              help="Advisory template to use from the group's erratatool.yml")
@pass_runtime
@click_coroutine
async def prepare_shipment(runtime: Runtime, group: str, assembly: str, application: str, advisory_key: str):
    pipeline = PrepareShipmentPipeline(runtime=runtime,
                                       group=group,
                                       assembly=assembly,
                                       application=application,
                                       advisory_key=advisory_key)
    await pipeline.run()
