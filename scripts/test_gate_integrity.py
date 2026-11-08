#!/usr/bin/env python3
"""Exercise the workflow's actual comparison against isolated Git fixtures."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap

root = Path(__file__).resolve().parent.parent
config_path = re.compile(
    rb'(GIT_CONFIG(?:_GLOBAL|_SYSTEM|_VALUE_[0-9]+)?|core\.(?:hooksPath|attributesFile|excludesFile)'
    rb'|include\.path|commit\.template)(?:\s*[^a-z0-9\s_./-]){0,12}\s*'
    rb'(/dev/null|os\.devnull)([^a-z0-9_./-]|$)', re.IGNORECASE,
)
comment_line = re.compile(rb'^[\t \r\f\x0b]*(?:#|//|/\*|\*)[^\n]*', re.MULTILINE)


def direct_config_paths(repo, env=None):
    candidates = subprocess.run(
        ['git', 'grep', '-IilzE', '-e', '/dev/null|os[.]devnull', '--',
         '*.go', '*.py', '*.sh', '*.js', '*.mjs', '*.cjs', '*.ts', '*.yml', '*.yaml',
         '*.json', '*.toml', 'Makefile'],
        cwd=repo, env=env, stdout=subprocess.PIPE,
    )
    if candidates.returncode == 1:
        return []
    candidates.check_returncode()
    matches = []
    for name in candidates.stdout.rstrip(b'\0').split(b'\0'):
        path = Path(os.fsdecode(name))
        source = comment_line.sub(b'X', (repo / path).read_bytes())
        if config_path.search(source):
            matches.append(path)
    return matches


unsafe_paths = direct_config_paths(root)
if unsafe_paths:
    raise SystemExit('Git config paths must use private temporary files.\n'
                     + '\n'.join(str(path) for path in unsafe_paths))

workflow = (root / '.github/workflows/ci.yml').read_text()
job = workflow.split('\n  gate-integrity:\n', 1)[1].split('\n  release-consistency:\n', 1)[0]
assert job.count('        run: |\n') == 1
command = textwrap.dedent(job.split('        run: |\n', 1)[1])
workflow += '\n  outside-fixture:\n    runs-on: ubuntu-latest\n    steps:\n      - run: ./outside.sh\n'
with tempfile.TemporaryDirectory(prefix='gate-integrity-') as directory:
    config = Path(directory) / 'gitconfig'
    config.touch(exist_ok=False)
    env = {'PATH': os.defpath, 'LC_ALL': 'C', 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': str(config)}
    guard_repo = Path(directory) / 'git-config-guard'
    subprocess.run(['git', 'init', '-q', str(guard_repo)], env=env, check=True)
    fixture = guard_repo / 'paths with spaces.py'
    key = b'GIT_CONFIG_GLOBAL'
    single = b'env = {"' + key + b'": os.devnull}\n'
    wrapped = b'env = {"' + key + b'":\n    os.devnull}\n'
    fixture.write_bytes(single)
    subprocess.run(['git', 'add', '--', fixture.name], cwd=guard_repo, env=env, check=True)
    for source in (single, wrapped):
        fixture.write_bytes(source)
        assert direct_config_paths(guard_repo, env) == [Path(fixture.name)]
    fixture.write_bytes(b'# ' + single + b'# ' + wrapped.replace(b'\n    ', b'\n#     '))
    (guard_repo / 'untracked.py').write_bytes(single)
    assert not direct_config_paths(guard_repo, env)
    print('PASS direct Git config paths, wrapped assignments and comment control', flush=True)

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
