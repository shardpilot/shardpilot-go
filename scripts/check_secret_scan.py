#!/usr/bin/env python3
"""Compare a history scan with its independently measured Git corpus."""
import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys

IMAGE = "ghcr.io/gitleaks/gitleaks:v8.30.1"
LOG_OPTIONS = [
    "--full-history", "--all", "--diff-merges=first-parent", "--diff-filter=d",
]


class Refusal(Exception):
    pass


def git(repo, *args, binary=False):
    result = subprocess.run(
        ["git", "--no-pager", "-C", str(repo), *args],
        text=not binary, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise Refusal("git " + args[0] + " could not measure the corpus")
    return result.stdout


def digest(lines):
    return hashlib.sha256("\0".join(sorted(lines)).encode()).hexdigest()


def material_commits(data):
    # NUL-delimited numstat keeps filenames opaque, including rename pairs,
    # tabs and newlines. Text changes form patches; metadata-only changes do not.
    records = data.split(b"\0")
    current = None
    material = set()
    index = 0
    while index < len(records):
        record = records[index]
        index += 1
        if record in (b"", b"\n"):
            continue
        if re.fullmatch(rb"[0-9a-f]{40}|[0-9a-f]{64}", record):
            current = record.decode("ascii")
            continue
        fields = record.removeprefix(b"\n").split(b"\t", 2)
        if current is None or len(fields) != 3:
            raise Refusal("git numstat returned an unreadable record")
        added, removed, filename = fields
        if any(value != b"-" and not value.isdigit() for value in (added, removed)):
            raise Refusal("git numstat returned an unreadable text count")
        if filename == b"":
            if index + 1 >= len(records) or not records[index] or not records[index + 1]:
                raise Refusal("git numstat returned an incomplete rename pair")
            index += 2
        if any(value.isdigit() and int(value) > 0 for value in (added, removed)):
            material.add(current)
    return sorted(material)


def snapshot(repo):
    if git(repo, "rev-parse", "--is-shallow-repository").strip() != "false":
        raise Refusal("the repository is shallow; fetch complete history before scanning")
    total = git(repo, "rev-list", "--count", "--all").strip()
    if not total.isdecimal() or int(total) == 0:
        raise Refusal("the reachable commit corpus is empty or unreadable")
    refs = git(repo, "for-each-ref", "--format=%(refname) %(objectname)").splitlines()
    head = git(repo, "rev-parse", "--verify", "HEAD").strip()
    material = material_commits(git(
        repo, "log", *LOG_OPTIONS, "--format=%x00%H%x00", "--numstat", "-z", binary=True,
    ))
    if not material:
        raise Refusal("the material commit corpus is empty")
    if len(material) != len(set(material)) or len(material) > int(total):
        raise Refusal("the material commit corpus is inconsistent")
    if any(not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", oid) for oid in material):
        raise Refusal("git log returned an unreadable commit identity")
    return {
        "total": int(total), "expected": len(material),
        "excluded_from_patch_stream": int(total) - len(material),
        "log_options": LOG_OPTIONS,
        "refs": len(refs), "ref_digest": digest(refs),
        "commit_digest": digest(material), "head": head,
    }


def scanner_command(repo, container):
    args = [
        "git", "--redact", "--no-banner", "--no-color", "--timeout=300",
        "--log-opts=" + " ".join(LOG_OPTIONS),
    ]
    if not container:
        return ["gitleaks", *args, str(repo)]
    if ":" in str(repo) or "\n" in str(repo):
        raise Refusal("the container bind path contains an unsupported separator")
    return [
        "docker", "run", "--rm", "--volume", str(repo) + ":/repo:ro",
        "--workdir", "/repo",
        "--env", "GIT_CONFIG_COUNT=1",
        "--env", "GIT_CONFIG_KEY_0=safe.directory",
        "--env", "GIT_CONFIG_VALUE_0=/repo",
        IMAGE, *args, "/repo",
    ]


def run(repo, container):
    before = snapshot(repo)
    print("HISTORY CORPUS expected: " + json.dumps(before, sort_keys=True), flush=True)
    # Merge-resolution additions need a patch too. Git's default log omits
    # those patches. Commits without text patches are reported separately.
    counts = []
    with subprocess.Popen(
        scanner_command(repo, container), cwd=repo, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    ) as process:
        for line in process.stdout:
            print(line, end="", flush=True)
            counts.extend(int(n) for n in re.findall(r"\b([0-9]+) commits? scanned\.", line))
        status = process.wait()
    if status:
        raise Refusal("gitleaks exited with status " + str(status))
    if len(counts) != 1:
        raise Refusal("gitleaks did not report exactly one commit count")
    observed = counts[0]
    print("HISTORY CORPUS observed: " + str(observed), flush=True)
    if observed != before["expected"]:
        raise Refusal("gitleaks commit count disagrees with the material corpus")
    if snapshot(repo) != before:
        raise Refusal("the Git corpus changed during the scan")
    print("HISTORY CORPUS verified: " + str(observed) + " material commits", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", action="store_true", help="use the pinned scanner image")
    parser.add_argument("repo", nargs="?", default=".")
    args = parser.parse_args()
    try:
        run(pathlib.Path(args.repo).resolve(strict=True), args.container)
    except (Refusal, OSError) as error:
        print("REFUSING history scan: " + str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
