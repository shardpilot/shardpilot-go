import os, pathlib, shutil, signal, subprocess, sys, tempfile

def require(condition, reason):
    if not condition: raise RuntimeError(reason)

try:
    for name in ['mv', 'timeout']:
        require(shutil.which(name), 'missing ' + name)
    with tempfile.TemporaryDirectory(prefix='publication host ') as directory:
        root = pathlib.Path(directory); source = root / 'probe'; target = root / 'target'; outside = root / 'directory'
        outside.mkdir(); (outside / 'keep').write_text('keep'); source.write_text('probe')
        target.symlink_to(outside, target_is_directory=True)
        result = subprocess.run(['mv', '-fT', str(source), str(target)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
        require(result.returncode == 0 and not source.exists() and not target.is_symlink()
                and target.read_text() == 'probe' and (outside / 'keep').read_text() == 'keep'
                and list(outside.iterdir()) == [outside / 'keep'], 'mv -fT must replace directory symlinks')
        started = root / 'started'
        process = subprocess.Popen(
            ['timeout', '--kill-after=0.1s', '1s', sys.executable, '-c',
             'import pathlib,sys,time; pathlib.Path(sys.argv[1]).touch(); time.sleep(10)', str(started)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try: os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            process.wait()
            raise RuntimeError('timeout did not bound execution')
        require(process.returncode == 124 and started.is_file(), 'timeout must run the probe and report deadline expiry')
except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
    print('PREREQUISITE: publication controls require Python 3, GNU mv -fT and timeout --kill-after: ' + str(error), file=sys.stderr)
    sys.exit(2)
