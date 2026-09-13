#!/usr/bin/env bash
# Run the receipt scenes against the current index in an isolated repository.
# The normal gate sources the receipt functions in this file.
# The baseline-write scene requires GNU mv, as does the existing write command.
if [ "${1:-}" = --test ]; then
  python3 - "$0" "${@:2}" <<'PY'
import json, os, pathlib, subprocess, sys, tempfile
source = pathlib.Path(sys.argv[1]).resolve().parent.parent
compact = '--compact' in sys.argv[2:]
selected = set(sys.argv[2:]) - {'--compact'}

def call(args, cwd, **kw):
    return subprocess.run(args, cwd=cwd, check=True, **kw)

def receipt(channel):
    prefix = 'RECEIPT scripts/check_public_surface.sh: '
    rows = [s[len(prefix):] for s in channel.read_text().splitlines() if s.startswith(prefix)] if channel.is_file() else []
    return json.loads(rows[-1]) if rows else None

def complete(r):
    if not isinstance(r, dict): return False
    planned, observed, excluded = (r.get(k, []) for k in ['planned', 'observed', 'excluded'])
    return (r.get('form') == 1 and r.get('producer') == 'scripts/check_public_surface.sh'
            and len(planned) == len(set(planned)) and len(observed) == len(set(observed))
            and len(excluded) == len(set(excluded)) and set(observed).issubset(planned)
            and set(excluded).issubset(planned) and not set(observed).intersection(excluded)
            and r.get('read') == len(observed) and r.get('expected') == len(planned) - len(excluded)
            and r['read'] == r['expected'] and r.get('aborted') is False and r.get('reasons') == [])

with tempfile.TemporaryDirectory(prefix='public-gate-scenes-') as directory:
    root = pathlib.Path(directory)
    repo = root / 'repo'; repo.mkdir()
    tree = subprocess.check_output(['git', 'write-tree'], cwd=source, text=True).strip()
    archive = subprocess.Popen(['git', 'archive', tree], cwd=source, stdout=subprocess.PIPE)
    call(['tar', '-x', '-C', str(repo)], source, stdin=archive.stdout)
    archive.stdout.close()
    if archive.wait() != 0: raise RuntimeError('index archive failed')
    if compact:
        # Keep the real debt-bearing source corpus and its exact baseline. The
        # compact scenes still execute the whole gate against a real index.
        baseline = 'scripts/public-surface-lane-b-baseline.txt'
        keep = {'README.md', baseline, 'scripts/public-gate.sh', 'scripts/check_public_surface.sh'}
        keep.update(s.split(' ', 1)[1] for s in (repo / baseline).read_text().splitlines() if s and not s.startswith('#'))
        paths = subprocess.check_output(['git', 'ls-tree', '-rz', '--name-only', tree], cwd=source).decode().split('\0')
        for name in paths:
            if name and name not in keep: (repo / name).unlink()
        print('Compact real-index fixture:', len(keep), 'tracked files', flush=True)
    call(['git', 'init', '-q'], repo)
    call(['git', 'add', '.'], repo)
    call(['git', '-c', 'user.name=Synthetic', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'Synthetic receipt subject'], repo)
    gate = repo / 'scripts/check_public_surface.sh'
    original = gate.read_text()
    helper = repo / 'scripts/public-gate.sh'
    original_helper = helper.read_text()
    subject = 'scan_tree "$PWD"'
    if original.count(subject) != 1: raise RuntimeError('actual scan invocation moved')
    total = 0
    for name in ['clean-zero-comparison', 'completed-finding', 'stopped-after-finding', 'dropped-tree-read', 'killed-reader', 'short-tree-walk', 'stale-startup', 'helper-mismatch', 'channel-write-failure', 'baseline-write']:
        if selected and name not in selected: continue
        gate.write_text(original)
        helper.write_text(original_helper)
        planted = repo / 'receipt-scene.txt'
        if planted.exists(): planted.unlink()
        if name == 'completed-finding': planted.write_text('See ' + 'ADR-' + '0' * 4 + '.\n')
        if name in ['stopped-after-finding', 'dropped-tree-read', 'killed-reader']:
            replacement = {'stopped-after-finding': 'echo "FAIL synthetic partial scan"; exit 1', 'dropped-tree-read': ':', 'killed-reader': 'kill -KILL "$$"'}[name]
            line = next(s for s in original.splitlines() if s.startswith(subject))
            gate.write_text(original.replace(line, replacement, 1))
        if name == 'short-tree-walk':
            point = '    scan_files=$((scan_files + 1))'
            assert original.count(point) == 1
            gate.write_text(original.replace(point, point + '\n    if [ "${2:-}" = receipt ]; then echo "synthetic short tree"; break; fi', 1))
        call(['git', 'add', '-A'], repo)
        call(['bash', '-n', str(gate)], repo)
        channel = root / (name + '.declared')
        if name in ['stale-startup', 'helper-mismatch']:
            seed = {'form': 1, 'producer': 'scripts/check_public_surface.sh', 'planned': ['synthetic'], 'observed': ['synthetic'], 'excluded': [], 'read': 1, 'expected': 1, 'findings': 0, 'aborted': False, 'reasons': []}
            assert complete(seed)
            channel.write_text('RECEIPT scripts/check_public_surface.sh: ' + json.dumps(seed) + '\n')
            changed = gate if name == 'stale-startup' else helper
            changed.write_text(changed.read_text() + '\n')
        if name == 'channel-write-failure': channel.mkdir()
        env = dict(os.environ, NIGHTLY_COMPLETION_FILE=str(channel))
        env.pop('PUBLIC_SURFACE_BASE_REF', None)
        if name != 'clean-zero-comparison': env['PUBLIC_SURFACE_BASE_REF'] = 'HEAD'
        args = ['bash', str(gate)] + (['--write-baseline'] if name == 'baseline-write' else [])
        run = subprocess.run(args, cwd=repo, env=env, capture_output=True, text=True, timeout=600)
        r = receipt(channel)
        if name in ['clean-zero-comparison', 'completed-finding', 'baseline-write']:
            assert complete(r), (name, run.returncode, r, run.stdout[-1000:], run.stderr[-1000:])
            assert run.returncode == (1 if name == 'completed-finding' else 0), (name, run.returncode)
            assert r['findings'] == (1 if name == 'completed-finding' else 0), (name, r)
            assert not any(s.startswith('INCOMPLETE ') for s in channel.read_text().splitlines()), channel.read_text()
            if name == 'clean-zero-comparison': assert 'target-check' in r['excluded'], r
            if name == 'baseline-write': assert 'baseline-write' in r['observed'] and set(r['excluded']) == {'baseline-check', 'target-check'}, r
        else:
            assert not complete(r), (name, r)
            assert run.returncode != 0, (name, run.returncode)
            if name == 'stopped-after-finding': assert 'FAIL synthetic partial scan' in run.stdout
            if name == 'short-tree-walk': assert 'synthetic short tree' in run.stdout
            if name == 'helper-mismatch': assert 'receipt helper differs from the index' in run.stderr
            if name == 'stale-startup': assert 'this script' in run.stderr
            if name == 'channel-write-failure': assert run.returncode == 2
        total += 1
        print('PASS', name, 'exit', run.returncode, 'receipt', r, flush=True)
    assert total > 0, 'no scene selected'
    print('PASS', total, 'receipt scenes', flush=True)
PY
  exit $?
fi

# Each caller allocates a fresh declaration channel and reads the last receipt.
# IDs describe checks, never input paths. A start record stays aborted if the
# process is killed; only the final, fully observed plan can replace it.
public_gate_init() {
  public_gate_plan=(audit roster self-test tree baseline-check target-check baseline-write)
  public_gate_states=(pending pending pending pending pending pending pending)
  public_gate_findings=0
  public_gate_status=0
  public_gate_ready=yes
  public_gate_emit true no
}

public_gate_record() {
  local i
  for ((i=0; i<${#public_gate_plan[@]}; i++)); do
    if [ "${public_gate_plan[$i]}" = "$1" ] && [ "${public_gate_states[$i]}" = pending ]; then
      public_gate_states[$i]="$2"
      return 0
    fi
  done
  echo 'REFUSING: invalid or repeated receipt observation.' >&2
  exit 2
}

public_gate_observe() { public_gate_record "$1" observed; }
public_gate_exclude() { public_gate_record "$1" excluded; }
public_gate_finding() {
  public_gate_findings=$((public_gate_findings + 1))
  public_gate_status=1
}

public_gate_counts() {
  local state
  public_gate_read=0
  public_gate_expected=${#public_gate_plan[@]}
  for state in "${public_gate_states[@]}"; do
    case "$state" in
      observed) public_gate_read=$((public_gate_read + 1)) ;;
      excluded) public_gate_expected=$((public_gate_expected - 1)) ;;
    esac
  done
}

public_gate_array() {
  local i comma=
  printf '['
  for ((i=0; i<${#public_gate_plan[@]}; i++)); do
    if [ "$1" = planned ] || [ "${public_gate_states[$i]}" = "$1" ]; then
      printf '%s"%s"' "$comma" "${public_gate_plan[$i]}"
      comma=,
    fi
  done
  printf ']'
}

public_gate_emit() {
  local line declaration
  public_gate_counts
  # All serialized strings come from the fixed plan above; input text and
  # environment values never enter this record.
  line="$(printf 'RECEIPT scripts/check_public_surface.sh: {"form":1,"producer":"scripts/check_public_surface.sh","planned":';
    public_gate_array planned
    printf ',"observed":'; public_gate_array observed
    printf ',"excluded":'; public_gate_array excluded
    printf ',"read":%d,"expected":%d,"findings":%d,"aborted":%s,"reasons":' \
      "$public_gate_read" "$public_gate_expected" "$public_gate_findings" "$1"
    if [ "$1" = true ]; then printf '["reading-stopped"]'; else printf '[]'; fi
    printf '}')" || return 2
  if [ -n "${NIGHTLY_COMPLETION_FILE:-}" ]; then
    printf '%s\n' "$line" >> "$NIGHTLY_COMPLETION_FILE" || return 2
  fi
  if [ "$2" = yes ]; then
    printf '%s\n' "$line" || return 2
    declaration=COMPLETE
    [ "$1" = false ] || declaration=INCOMPLETE
    line="$declaration scripts/check_public_surface.sh: read $public_gate_read of expected $public_gate_expected units; findings $public_gate_findings"
    printf '%s\n' "$line" || return 2
    if [ -n "${NIGHTLY_COMPLETION_FILE:-}" ]; then
      printf '%s\n' "$line" >> "$NIGHTLY_COMPLETION_FILE" || return 2
    fi
  fi
}

public_gate_finish() {
  local aborted=true
  public_gate_counts
  if [ "${gate_finished:-no}" = yes ] && [ "$1" -eq "$public_gate_status" ] \
     && [ "$public_gate_read" -eq "$public_gate_expected" ]; then
    aborted=false
  fi
  public_gate_emit "$aborted" yes || return 2
  [ "$aborted" = false ] || return 2
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  exec bash "$(dirname "$0")/check_public_surface.sh" "$@"
fi
