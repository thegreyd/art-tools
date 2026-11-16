import os
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.scheduled.schedule_ocp4_scan_konflux import run_for

MODULE = 'pyartcd.pipelines.scheduled.schedule_ocp4_scan_konflux'


class TestScheduleOcp4ScanKonflux(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(working_dir=Path('/tmp'), dry_run=False)
        self.locks = MagicMock()
        self.locks.is_locked = AsyncMock(return_value=False)
        self.env = patch.dict(os.environ, TASKRUN_NAME='task', TEKTON_PIPELINERUN_NAME='scheduler')
        self.env.start()
        self.addCleanup(self.env.stop)
        self.permitted = patch(f'{MODULE}.util.is_build_permitted', new=AsyncMock(return_value=True)).start()
        self.start = patch(f'{MODULE}.tekton.start_pipeline_run', return_value='scan-abc').start()
        self.wait = patch(f'{MODULE}.tekton.wait_for_pipeline_run', new=AsyncMock(return_value='Succeeded')).start()
        self.jenkins = patch(f'{MODULE}.jenkins').start()
        self.addCleanup(patch.stopall)

    async def test_parallel_schedules_artc_scan_without_waiting_or_jenkins(self):
        await run_for('5.1', self.runtime, self.locks)
        self.start.assert_called_once_with(
            pipeline_name='ocp4-scan-konflux', params={'version': '5.1', 'dry-run': 'false'}, pipeline_timeout='4h'
        )
        self.wait.assert_not_awaited()
        self.jenkins.start_ocp4_scan_konflux.assert_not_called()

    async def test_serial_waits_for_child_and_continues_after_failure(self):
        self.wait.side_effect = [RuntimeError('Failed'), 'Succeeded']
        await run_for('4.18', self.runtime, self.locks, serial=True)
        await run_for('4.17', self.runtime, self.locks, serial=True)
        self.assertEqual(self.start.call_count, 2)
        self.assertEqual(self.wait.await_count, 2)

    async def test_scan_and_build_locks_prevent_scheduling(self):
        for states in ([True], [False, True]):
            with self.subTest(states=states):
                self.locks.is_locked.side_effect = states
                await run_for('5.1', self.runtime, self.locks)
        self.start.assert_not_called()
        self.permitted.assert_not_awaited()

    async def test_freeze_prevents_scheduling(self):
        self.permitted.return_value = False
        await run_for('5.1', self.runtime, self.locks)
        self.start.assert_not_called()

    async def test_dry_run_never_starts_artc_or_jenkins_jobs(self):
        self.runtime.dry_run = True
        for tekton_context in (True, False):
            with patch(f'{MODULE}.tekton.is_tekton_context', return_value=tekton_context):
                await run_for('5.1', self.runtime, self.locks, serial=True)
        self.start.assert_not_called()
        self.wait.assert_not_awaited()
        self.jenkins.start_ocp4_scan_konflux.assert_not_called()

    async def test_missing_child_name_fails_scheduler(self):
        self.start.return_value = None
        with self.assertRaisesRegex(RuntimeError, 'Could not identify'):
            await run_for('5.1', self.runtime, self.locks)

    async def test_jenkins_parallel_and_serial_routing_is_preserved(self):
        with patch(f'{MODULE}.tekton.is_tekton_context', return_value=False):
            await run_for('5.1', self.runtime, self.locks)
            self.jenkins.start_ocp4_scan_konflux.assert_called_with(version='5.1', block_until_building=False)
            await run_for('4.18', self.runtime, self.locks, serial=True)
            self.jenkins.start_ocp4_scan_konflux.assert_called_with(
                version='4.18', block_until_building=True, block_until_complete=True
            )
        self.start.assert_not_called()
