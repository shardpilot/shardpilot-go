#!/usr/bin/env python3
"""Exercise the workflow's actual comparison against isolated Git fixtures."""

import os
from pathlib import Path
import subprocess
import tempfile
import textwrap

root = Path(__file__).resolve().parent.parent
workflow = (root / '.github/workflows/ci.yml').read_text()
job = workflow.split('\n  gate-integrity:\n', 1)[1].split('\n  release-consistency:\n', 1)[0]
assert job.count('        run: |\n') == 1
command = textwrap.dedent(job.split('        run: |\n', 1)[1])
workflow += '\n  outside-fixture:\n    runs-on: ubuntu-latest\n    steps:\n      - run: ./outside.sh\n'
env = {'PATH': os.defpath, 'LC_ALL': 'C', 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull}

with tempfile.TemporaryDirectory(prefix='gate-integrity-') as directory:
    for scene, changed, expected in [
        ('unchanged', None, 0),
        ('ordinary-script', 'scripts/ordinary.sh', 1),
        ('quoted-script', 'scripts/quoted"name.sh', 1),
        ('outside-scripts', 'outside.sh', 0),
    ]:
        repo = Path(directory) / scene
        (repo / 'scripts').mkdir(parents=True)
        (repo / '.github/workflows').mkdir(parents=True)
        (repo / '.github/workflows/ci.yml').write_text(workflow)
        for name in ['scripts/ordinary.sh', 'scripts/quoted"name.sh', 'outside.sh']:
            (repo / name).write_text('# synthetic gate input\n')

        def git(*args):
            return subprocess.check_output(['git', *args], cwd=repo, env=env, text=True).strip()

        git('init', '-q', '-b', 'main')
        git('add', '.')
        git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
            'commit', '-qm', 'Synthetic gate inputs')
        base = git('rev-parse', 'HEAD')
        listed = git('ls-files', '--', 'scripts/')
        assert '"scripts/quoted\\"name.sh"' in listed, listed
        if changed:
            with (repo / changed).open('a') as target:
                target.write('# changed input\n')
        run = subprocess.run(
            ['bash', '-c', command], cwd=repo, capture_output=True, text=True,
            env=dict(env, EVENT_NAME='pull_request', PR_BASE_SHA=base,
                     DEFAULT_BRANCH='main', PUSH_BEFORE=''), timeout=30,
        )
        assert run.returncode == expected, (scene, run.returncode, run.stdout, run.stderr)
        if expected: assert 'alters the gate that judges it' in run.stderr, run.stderr
        print('PASS', scene, 'exit', run.returncode, flush=True)
