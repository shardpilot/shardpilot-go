import os, pathlib, shlex, shutil, subprocess, sys, tempfile

root = pathlib.Path(__file__).resolve().parent.parent
entries = {'ratchet': ['scripts/test_surface_ratchet.sh'], 'receipts': ['scripts/public-gate.sh', '--test', '--compact']}
assert set(sys.argv[1:]).issubset(entries), 'unknown entrypoint selection'
status = subprocess.call([sys.executable, str(root / 'scripts/check_publication_host.py')])
if status: sys.exit(status)
print('PASS capable host preflight', flush=True)
tools = {name: shutil.which(name) for name in ['python3', 'mv', 'timeout', 'dirname', 'git', 'bash']}
assert all(tools.values()), tools
faults = {'mv-noop': 'exit 0',
          'mv-follows-directory': 'shift; exec ' + shlex.quote(tools['mv']) + ' -f "$@"',
          'timeout-noop': 'exit 0', 'timeout-fake-expiry': 'exit 124',
          'timeout-masks-status': shlex.quote(tools['timeout']) + ' "$@"; exit 0',
          'timeout-ignores-limit': 'shift; shift; exec "$@"'}
for case in ['missing-mv', 'missing-timeout', 'missing-python3', *faults, 'capable']:
    with tempfile.TemporaryDirectory(prefix='publication-host-controls-') as directory:
        folder = pathlib.Path(directory); marker = folder / 'started'; bindir = folder / 'bin'; bindir.mkdir()
        for name in ['python3', 'mv', 'timeout', 'dirname']:
            if case != 'missing-' + name: (bindir / name).symlink_to(tools[name])
        if case in faults:
            stub = bindir / ('mv' if case.startswith('mv-') else 'timeout'); stub.unlink()
            stub.write_text('#!/bin/sh\n' + faults[case] + '\n'); stub.chmod(0o755)
        git = bindir / 'git'
        git.write_text('#!/bin/sh\ncase "$1" in status) exit 0;; ls-files|write-tree) : > "$PUBLICATION_CONTROL_STARTED"; exit 99;; esac\nexec ' + shlex.quote(tools['git']) + ' "$@"\n')
        git.chmod(0o755)
        env = {'PATH': str(bindir), 'LANG': 'C', 'GIT_CONFIG_GLOBAL': os.devnull,
               'GIT_CONFIG_NOSYSTEM': '1', 'PUBLICATION_CONTROL_STARTED': str(marker)}
        for label, args in entries.items():
            if sys.argv[1:] and label not in sys.argv[1:]: continue
            marker.unlink(missing_ok=True)
            run = subprocess.run([tools['bash'], *args], cwd=root, env=env, capture_output=True, text=True, timeout=10)
            if case == 'capable':
                assert marker.is_file() and 'PREREQUISITE:' not in run.stderr, (case, label, run.stderr)
            else:
                assert not marker.exists(), (case, label, 'dependent Git work started')
                assert (run.returncode == 2 and run.stderr.count('PREREQUISITE:') == 1
                        and len(run.stderr.splitlines()) == 1 and not run.stdout), (case, label, run.returncode, run.stderr)
                if case == 'timeout-ignores-limit': assert 'timeout did not bound execution' in run.stderr, run.stderr
                assert 'FAIL [' not in run.stderr and 'surface ratchet:' not in run.stderr, run.stderr
            print('PASS', case, label, flush=True)
