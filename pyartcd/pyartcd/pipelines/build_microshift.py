import asyncio
import json
import logging
import os
import re
import traceback
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional, Tuple, cast

import click

from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.util import get_ocp_version_from_group, isolate_major_minor_in_group
from artcommonlib.release_util import isolate_assembly_in_release
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, Engine, ArtifactType
from artcommonlib import exectools
from doozerlib.util import isolate_nightly_name_components
from ghapi.all import GhApi
from ruamel.yaml import YAML
from semver import VersionInfo
from tenacity import retry, stop_after_attempt, wait_fixed

from pyartcd import constants, oc, util, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime
from pyartcd.util import (get_assembly_type, isolate_el_version_in_release, load_group_config, load_releases_config)

yaml = YAML(typ="rt")
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.width = 4096


class BuildMicroShiftPipeline:
    """ Rebase and build MicroShift for an assembly """

    SUPPORTED_ASSEMBLY_TYPES = {AssemblyTypes.STANDARD, AssemblyTypes.CANDIDATE, AssemblyTypes.PREVIEW, AssemblyTypes.STREAM, AssemblyTypes.CUSTOM}

    def __init__(self, runtime: Runtime, group: str, assembly: str, payloads: Tuple[str, ...],
                 no_rebase: bool, no_advisory_prep: bool,
                 force: bool, force_bootc: bool,
                 data_path: str, slack_client, logger: Optional[logging.Logger] = None):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.assembly_type = AssemblyTypes.STREAM
        self.payloads = payloads
        self.no_rebase = no_rebase
        self.no_advisory_prep = no_advisory_prep
        self.force = force
        self.force_bootc = force_bootc
        self._logger = logger or runtime.logger
        self._working_dir = self.runtime.working_dir.absolute()
        self.releases_config = None
        self.slack_client = slack_client

        # determines OCP version
        self._ocp_version = get_ocp_version_from_group(group)

        # sets environment variables for Elliott and Doozer
        self._elliott_env_vars = os.environ.copy()
        self._elliott_env_vars["ELLIOTT_WORKING_DIR"] = str(self._working_dir / "elliott-working")
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self._working_dir / "doozer-working")

        if not data_path:
            data_path = self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
        if data_path:
            self._doozer_env_vars["DOOZER_DATA_PATH"] = data_path
            self._elliott_env_vars["ELLIOTT_DATA_PATH"] = data_path

    async def run(self):
        # Make sure our api.ci token is fresh
        await oc.registry_login(self.runtime)

        group_config = await load_group_config(self.group, self.assembly, env=self._doozer_env_vars)
        advisories = group_config.get("advisories", {})
        self.releases_config = await load_releases_config(
            group=self.group,
            data_path=self._doozer_env_vars.get("DOOZER_DATA_PATH", None) or constants.OCP_BUILD_DATA_URL
        )
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
        if self.assembly_type not in self.SUPPORTED_ASSEMBLY_TYPES:
            raise ValueError(f"Building MicroShift for assembly type {self.assembly_type.value} is not currently "
                             "supported.")

        if self.assembly_type is AssemblyTypes.STREAM:
            await self._rebase_and_build_for_stream()
        else:
            await self._rebase_and_build_for_named_assembly()
            # Check if microshift advisory is defined in assembly
            if 'microshift' not in advisories:
                self._logger.info(f"Skipping advisory prep since microshift advisory is not defined in assembly {self.assembly}")
            elif self.no_advisory_prep:
                self._logger.info("Skipping advisory prep since --no-advisory-prep flag is set")
            else:
                await self._prepare_advisory(advisories['microshift'])

            await self._rebase_and_build_bootc()

    async def _rebase_and_build_for_stream(self):
        # Do a sanity check
        if self.assembly_type != AssemblyTypes.STREAM:
            raise ValueError(f"Cannot process assembly type {self.assembly_type.value}")

        major, minor = isolate_major_minor_in_group(self.group)
        # rebase against nightlies
        # rpm version-release will be like `4.12.0~test-202201010000.p?`
        if self.no_rebase:
            # Without knowing the nightly name, it is hard to determine rpm version-release.
            raise ValueError("--no-rebase is not supported to build against assembly stream.")

        if not self.payloads:
            raise ValueError("Release payloads must be specified to rebase against assembly stream.")

        payload_infos = await self.parse_release_payloads(self.payloads)
        if "x86_64" not in payload_infos or "aarch64" not in payload_infos:
            raise ValueError("x86_64 payload and aarch64 payload are required for rebasing microshift.")

        for info in payload_infos.values():
            payload_version = VersionInfo.parse(info["version"])
            if (payload_version.major, payload_version.minor) != (major, minor):
                raise ValueError(f"Specified payload {info['pullspec']} does not match group major.minor {major}.{minor}: {payload_version}")

        # use the version of the x86_64 payload to generate the rpm version-release.
        release_name = payload_infos["x86_64"]["version"]
        custom_payloads = payload_infos

        # Rebase and build microshift
        version, release = self.generate_microshift_version_release(release_name)

        try:
            await self._rebase_and_build_rpm(version, release, custom_payloads)
        except Exception as build_err:
            self._logger.error(build_err)
            # Send a message to #microshift-alerts for STREAM failures
            await self._notify_microshift_alerts(f"{version}-{release}")
            raise

    async def _rebase_and_build_for_named_assembly(self):
        # Do a sanity check
        if self.assembly_type == AssemblyTypes.STREAM:
            raise ValueError(f"Cannot process assembly type {self.assembly_type.value}")

        major, minor = isolate_major_minor_in_group(self.group)

        # For named assemblies, check if builds are pinned or already exist
        nvrs = []
        pinned_nvrs = dict()

        if self.payloads:
            raise ValueError(f"Specifying payloads for assembly type {self.assembly_type.value} is not allowed.")

        release_name = util.get_release_name_for_assembly(self.group, self.releases_config, self.assembly)

        await self.slack_client.say_in_thread(f":construction: Microshift prep for assembly {self.assembly} :construction:")

        if not self.force:
            pinned_nvrs = util.get_rpm_if_pinned_directly(self.releases_config, self.assembly, 'microshift')
            if pinned_nvrs:
                message = (f"For assembly {self.assembly} builds are already pinned: {pinned_nvrs}. Use FORCE to "
                           "rebuild.")
                self._logger.info(message)
                await self.slack_client.say_in_thread(message)
                nvrs = list(pinned_nvrs.values())
            else:
                nvrs = await self._find_builds()

        if nvrs:
            self._logger.info("Builds already exist: %s", nvrs)
        else:
            # Rebase and build microshift
            version, release = self.generate_microshift_version_release(release_name)
            nvrs = await self._rebase_and_build_rpm(version, release, custom_payloads=None)
            message = f"microshift for assembly {self.assembly} has been successfully built."
            await self.slack_client.say_in_thread(message)

        # Check if we need create a PR to pin eligible builds
        diff = set(nvrs) - set(pinned_nvrs.values())
        if diff:
            self._logger.info("Creating PR to pin microshift build: %s", diff)
            pr = await self._create_or_update_pull_request(nvrs)
            message = f"PR to pin microshift build to the {self.assembly} assembly has been merged: {pr.html_url}"
            await self.slack_client.say_in_thread(message)

        if self.assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
            version = f'{major}.{minor}'
            try:
                if self.runtime.dry_run:
                    self._logger.info("[DRY RUN] Would have triggered microshift_sync job for version %s and assembly %s",
                                      version, self.assembly)
                else:
                    jenkins.start_microshift_sync(version=version, assembly=self.assembly)

                message = f"microshift_sync for version {version} and assembly {self.assembly} has been triggered\n" \
                          f"This will publish the microshift build to mirror"
                await self.slack_client.say_in_thread(message)
            except Exception as err:
                self._logger.warning("Failed to trigger microshift_sync job: %s", err)
                message = f"@release-artists Please start <{constants.JENKINS_UI_URL}" \
                          "/job/aos-cd-builds/job/build%252Fmicroshift_sync|microshift sync> manually."
                await self.slack_client.say_in_thread(message)

    async def _prepare_advisory(self, microshift_advisory_id):
        await self.slack_client.say_in_thread(f"Start preparing microshift advisory for assembly {self.assembly}..")
        await self._attach_builds()
        await self._sweep_bugs()
        await self._attach_cve_flaws()
        await self._change_advisory_status()
        await self._verify_microshift_bugs(microshift_advisory_id)
        await self.slack_client.say_in_thread("Completed preparing microshift advisory.")

    async def _attach_builds(self):
        """ attach the microshift builds to advisory
        """
        cmd = [
            "elliott",
            "--group", self.group,
            "--assembly", self.assembly,
            "--rpms", "microshift",
            "find-builds",
            "-k", "rpm",
            "--member-only",
            "--use-default-advisory", "microshift"
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    async def _sweep_bugs(self):
        """ sweep the microshift bugs to advisory
        """
        cmd = [
            "elliott",
            "--group", self.group,
            "--assembly", self.assembly,
            "find-bugs:sweep",
            "--permissive",  # this is so we don't error out on non-microshift bugs
            "--use-default-advisory", "microshift"
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    async def _attach_cve_flaws(self):
        """ attach CVE flaws to advisory
        """
        cmd = [
            "elliott",
            "--group", self.group,
            "--assembly", self.assembly,
            "attach-cve-flaws",
            "--use-default-advisory", "microshift"
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    async def _verify_microshift_bugs(self, advisory_id):
        """ verify attached bugs on microshift advisory
        """
        cmd = [
            "elliott",
            "--group", self.group,
            "--assembly", self.assembly,
            "verify-attached-bugs",
            "--verify-flaws",
            str(advisory_id)
        ]
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    # Advisory can have several pending checks, so retry it a few times
    @retry(reraise=True, stop=stop_after_attempt(5), wait=wait_fixed(1200))
    async def _change_advisory_status(self):
        """ move advisory status to QE
        """
        cmd = [
            "elliott",
            "--group", self.group,
            "--assembly", self.assembly,
            "change-state",
            "-s", "QE",
            "--from", "NEW_FILES",
            "--use-default-advisory", "microshift"
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    @staticmethod
    async def parse_release_payloads(payloads: Iterable[str]):
        result = {}
        pullspecs = []
        for payload in payloads:
            if "/" not in payload:
                # Convert nightly name to pullspec
                # 4.12.0-0.nightly-2022-10-25-210451 ==> registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-10-25-210451
                _, brew_cpu_arch, _ = isolate_nightly_name_components(payload)
                pullspecs.append(constants.NIGHTLY_PAYLOAD_REPOS[brew_cpu_arch] + ":" + payload)
            else:
                # payload is a pullspec
                pullspecs.append(payload)
        payload_infos = await asyncio.gather(*(oc.get_release_image_info(pullspec) for pullspec in pullspecs))
        for info in payload_infos:
            arch = info["config"]["architecture"]
            brew_arch = brew_arch_for_go_arch(arch)
            version = info["metadata"]["version"]
            result[brew_arch] = {
                "version": version,
                "arch": arch,
                "pullspec": info["image"],
                "digest": info["digest"],
            }
        return result

    @staticmethod
    def generate_microshift_version_release(ocp_version: str, timestamp: Optional[str] = None):
        """ Generate version and release strings for microshift builds
        Example version-releases:
        - 4.12.42-202210011234
        - 4.13.0~rc.4-202210011234
        """
        if not timestamp:
            timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M")
        release_version = VersionInfo.parse(ocp_version)
        version = f"{release_version.major}.{release_version.minor}.{release_version.patch}"
        if release_version.prerelease is not None:
            version += f"~{release_version.prerelease.replace('-', '_')}"
        release = f"{timestamp}.p?"
        return version, release

    async def _find_builds(self) -> List[str]:
        """ Find microshift builds in Brew
        :param release: release field for rebase
        :return: NVRs
        """
        cmd = [
            "elliott",
            "--group", self.group,
            "--assembly", self.assembly,
            "-r", "microshift",
            "find-builds",
            "-k", "rpm",
            "--member-only",
        ]
        with TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/out.json"
            cmd.append(f"--json={path}")
            await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)
            with open(path) as f:
                result = json.load(f)

        nvrs = cast(List[str], result["builds"])

        # microshift builds are special in that they build for each assembly after payload is promoted
        # and they include the assembly name in its build name
        # so make sure found nvrs are related to assembly
        return [n for n in nvrs if isolate_assembly_in_release(n) == self.assembly]

    async def _rebase_and_build_rpm(self, version, release: str, custom_payloads: Optional[Dict[str, str]]) -> List[str]:
        """ Rebase and build RPM
        :param release: release field for rebase
        :return: NVRs
        """
        cmd = [
            "doozer",
            "--group", self.group,
            "--assembly", self.assembly,
            "-r", "microshift",
            "rpms:rebase-and-build",
            "--version", version,
            "--release", release,
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        set_env = self._doozer_env_vars.copy()
        if custom_payloads:
            set_env["MICROSHIFT_PAYLOAD_X86_64"] = custom_payloads["x86_64"]["pullspec"]
            set_env["MICROSHIFT_PAYLOAD_AARCH64"] = custom_payloads["aarch64"]["pullspec"]
        if self.no_rebase:
            set_env["MICROSHIFT_NO_REBASE"] = "1"
        await exectools.cmd_assert_async(cmd, env=set_env)

        if self.runtime.dry_run:
            return [f"microshift-{version}-{release.replace('.p?', '.p0')}.el8"]

        # parse record.log
        with open(Path(self._doozer_env_vars["DOOZER_WORKING_DIR"]) / "record.log", "r") as file:
            record_log = parse_record_log(file)
            return record_log["build_rpm"][-1]["nvrs"].split(",")

    def _pin_nvrs(self, nvrs: List[str], releases_config) -> Dict:
        """ Update releases.yml to pin the specified NVRs.
        Example:
            releases:
                4.11.7:
                    assembly:
                    members:
                        rpms:
                        - distgit_key: microshift
                        metadata:
                            is:
                                el8: microshift-4.11.7-202209300751.p0.g7ebffc3.assembly.4.11.7.el8
        """
        is_entry = {}
        dg_key = "microshift"
        for nvr in nvrs:
            el_version = isolate_el_version_in_release(nvr)
            assert el_version is not None
            is_entry[f"el{el_version}"] = nvr

        rpms_entry = releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("members", {}).setdefault("rpms", [])
        microshift_entry = next(filter(lambda rpm: rpm.get("distgit_key") == dg_key, rpms_entry), None)
        if microshift_entry is None:
            microshift_entry = {"distgit_key": dg_key, "why": "Pin microshift to assembly"}
            rpms_entry.append(microshift_entry)
        microshift_entry.setdefault("metadata", {})["is"] = is_entry
        return microshift_entry

    async def _create_or_update_pull_request(self, nvrs: List[str]):
        self._logger.info("Creating ocp-build-data PR...")
        # Clone ocp-build-data
        build_data_path = self._working_dir / "ocp-build-data-push"
        build_data = GitRepository(build_data_path, dry_run=self.runtime.dry_run)
        ocp_build_data_repo_push_url = self.runtime.config["build_config"]["ocp_build_data_repo_push_url"]
        await build_data.setup(ocp_build_data_repo_push_url)
        branch = f"auto-pin-microshift-{self.group}-{self.assembly}"
        await build_data.fetch_switch_branch(branch, self.group)
        # Make changes
        releases_yaml_path = build_data_path / "releases.yml"
        releases_yaml = yaml.load(releases_yaml_path)
        self._pin_nvrs(nvrs, releases_yaml)
        yaml.dump(releases_yaml, releases_yaml_path)
        # Create a PR
        title = f"Pin microshift build for {self.group} {self.assembly}"
        body = f"Created by job run {jenkins.get_build_url()}"
        match = re.search(r"github\.com[:/](.+)/(.+)(?:.git)?", ocp_build_data_repo_push_url)
        if not match:
            raise ValueError(f"Couldn't create a pull request: {ocp_build_data_repo_push_url} is not a valid github repo")
        head = f"{match[1]}:{branch}"
        base = self.group
        if self.runtime.dry_run:
            self._logger.warning("[DRY RUN] Would have created pull-request with head '%s', base '%s' title '%s', body '%s'", head, base, title, body)
            d = {"html_url": "https://github.example.com/foo/bar/pull/1234", "number": 1234}
            result = namedtuple('pull_request', d.keys())(*d.values())
            return result
        pushed = await build_data.commit_push(f"{title}\n{body}")
        result = None
        if pushed:
            github_token = os.environ.get('GITHUB_TOKEN')
            if not github_token:
                raise ValueError("GITHUB_TOKEN environment variable is required to create a pull request")
            repo = "ocp-build-data"
            api = GhApi(owner=constants.GITHUB_OWNER, repo=repo, token=github_token)
            existing_prs = api.pulls.list(state="open", base=base, head=head)
            if not existing_prs.items:
                result = api.pulls.create(head=head, base=base, title=title, body=body, maintainer_can_modify=True)
                api.pulls.merge(owner=constants.GITHUB_OWNER, repo=repo, pull_number=result.number, merge_method="squash")
            else:
                pull_number = existing_prs.items[0].number
                result = api.pulls.update(pull_number=pull_number, title=title, body=body)
                api.pulls.merge(owner=constants.GITHUB_OWNER, repo=repo, pull_number=pull_number, merge_method="squash")
        else:
            self._logger.warning("PR is not created: Nothing to commit.")
        return result

    async def _notify_microshift_alerts(self, version_release: str):
        doozer_log_file = Path(self._doozer_env_vars["DOOZER_WORKING_DIR"]) / "debug.log"
        slack_client = self.runtime.new_slack_client()
        slack_client.channel = "C0310LGMQQY"  # microshift-alerts
        message = f":alert: @here ART build failure: microshift-{version_release}."
        message += "\nPing @ release-artists if you need help."
        slack_response = await slack_client.say(message)
        slack_thread = slack_response["message"]["ts"]
        if doozer_log_file.exists():
            await slack_client.upload_file(
                file=str(doozer_log_file),
                filename="microshift-build.log",
                initial_comment="Build logs",
                thread_ts=slack_thread)
        else:
            await slack_client.say("Logs are not available.", thread_ts=slack_thread)

    async def _rebase_and_build_bootc(self):
        bootc_image_name = "microshift-bootc"
        major, minor = isolate_major_minor_in_group(self.group)
        # do not run for version < 4.18
        if major < 4 or (major == 4 and minor < 18):
            self._logger.info("Skipping bootc image build for version < 4.18")
            return

        # check if an image build already exists in Konflux DB
        if not self.force_bootc:
            build = await runtime.konflux_db.get_latest_build(
                name=bootc_image_name,
                group=self.group,
                assembly=self.assembly,
                outcome=KonfluxBuildOutcome.SUCCESS,
                engine=Engine.KONFLUX,
                artifact_type=ArtifactType.IMAGE,
                el_target='el9'
            )
            if build:
                self._logger.info("Bootc image build already exists: %s", build.nvr)
                return

        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')
        if not kubeconfig:
            raise ValueError(f"KONFLUX_SA_KUBECONFIG environment variable is required to build {bootc_image_name} image")

        # Rebase and build bootc image
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M")
        version = f"v{major}.{minor}.0"
        release = f"{timestamp}.p?"
        rebase_cmd = [
            "doozer",
            "--group", self.group,
            "--assembly", self.assembly,
            "--latest-parent-version",
            "-i", bootc_image_name,
            # regardless of assembly cutoff time lock to HEAD in release branch
            # also not passing this breaks the command since we try to use brew to find the appropriate commit
            "--lock-upstream", bootc_image_name, "HEAD",
            "beta:images:konflux:rebase",
            "--version", version,
            "--release", release,
            "--message", f"Updating Dockerfile version and release {version}-{release}",
        ]
        if not self.runtime.dry_run:
            rebase_cmd.append("--push")
        await exectools.cmd_assert_async(rebase_cmd, env=self._doozer_env_vars)

        build_cmd = [
            "doozer",
            "--group", self.group,
            "--assembly", self.assembly,
            "--latest-parent-version",
            "-i", bootc_image_name,
            # regardless of assembly cutoff time lock to HEAD in release branch
            # also not passing this breaks the command since we try to use brew to find the appropriate commit
            "--lock-upstream", bootc_image_name, "HEAD",
            "beta:images:konflux:build",
            "--konflux-kubeconfig", kubeconfig,
        ]
        if self.runtime.dry_run:
            build_cmd.append("--dry-run")
        await exectools.cmd_assert_async(build_cmd, env=self._doozer_env_vars)


@cli.command("build-microshift")
@click.option("--data-path", metavar='BUILD_DATA', default=None,
              help=f"Git repo or directory containing groups metadata e.g. {constants.OCP_BUILD_DATA_URL}")
@click.option("-g", "--group", metavar='NAME', required=True,
              help="The group of components on which to operate. e.g. openshift-4.9")
@click.option("--assembly", metavar="ASSEMBLY_NAME", required=True,
              help="The name of an assembly to rebase & build for. e.g. 4.9.1")
@click.option("--payload", "payloads", metavar="PULLSPEC", multiple=True,
              help="[Multiple] Release payload to rebase against; Can be a nightly name or full pullspec")
@click.option("--no-rebase", is_flag=True,
              help="Don't rebase microshift code; build the current source we have in the upstream repo for testing purpose")
@click.option("--no-advisory-prep", is_flag=True,
              help="Skip preparing microshift advisory if applicable.")
@click.option("--force", is_flag=True,
              help="(For named assemblies) Rebuild even if a build already exists")
@click.option("--force-bootc", is_flag=True,
              help="Rebuild microshift-bootc image even if a build already exists")
@pass_runtime
@click_coroutine
async def build_microshift(runtime: Runtime, data_path: str, group: str, assembly: str, payloads: Tuple[str, ...],
                           no_rebase: bool, no_advisory_prep: bool, force: bool, force_bootc: bool):
    # slack client is dry-run aware and will not send messages if dry-run is enabled
    slack_client = runtime.new_slack_client()
    slack_client.bind_channel(group)
    try:
        pipeline = BuildMicroShiftPipeline(runtime=runtime, group=group, assembly=assembly, payloads=payloads,
                                           no_rebase=no_rebase, no_advisory_prep=no_advisory_prep,
                                           force=force, force_bootc=force_bootc,
                                           data_path=data_path, slack_client=slack_client)
        await pipeline.run()
    except Exception as err:
        slack_message = f"build-microshift pipeline encountered error: {err}"
        reaction = None
        error_message = slack_message + f"\n {traceback.format_exc()}"
        runtime.logger.error(error_message)
        if assembly not in ["stream", "test", "microshift"]:
            slack_message += "\n@release-artists"
            reaction = "art-attention"
        await slack_client.say_in_thread(slack_message, reaction)
        raise
