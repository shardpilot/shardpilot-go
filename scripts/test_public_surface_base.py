#!/usr/bin/env python3
"""Exercise the CI selector with local Git event fixtures; no network service."""
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest


SELECTOR = Path(__file__).with_name("select_public_surface_base.sh").resolve()
GIT = shutil.which("git")
ZERO = "0" * 40


class ComparisonBaseScenes(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="surface-base-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote = self.root / "origin"
        self.remote.mkdir()
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_CONFIG_NOSYSTEM="1")
        for key in ("BASH_ENV", "ENV", "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
            self.env.pop(key, None)
        self.git(self.remote, "init", "-q", "-b", "main")
        self.git(self.remote, "config", "user.name", "surface-base-test")
        self.git(self.remote, "config", "user.email", "surface-base-test@invalid")
        self.first = self.commit("root")
        self.git(self.remote, "switch", "-q", "-c", "event")
        self.event = self.commit("event")
        self.git(self.remote, "switch", "-q", "main")
        self.main = self.commit("unrelated default")
        self.worker = self.root / "worker"
        self.calls = 0

    def git(self, cwd, *args, input=None):
        return subprocess.check_output([GIT, *args], cwd=cwd, env=self.env,
                                       input=input, text=True, stderr=subprocess.PIPE).strip()

    def commit(self, text):
        (self.remote / "content.txt").write_text(text + "\n")
        self.git(self.remote, "add", "content.txt")
        self.git(self.remote, "commit", "-qm", text)
        return self.git(self.remote, "rev-parse", "HEAD")

    def clone(self, shallow=True):
        args = ["clone", "-q", "--no-hardlinks", "--branch", "event"]
        if shallow:
            args.append("--depth=1")
        self.git(self.root, *args, self.remote.as_uri(), str(self.worker))
        self.assertEqual(self.git(self.worker, "rev-parse", "--is-shallow-repository"),
                         "true" if shallow else "false")

    def stub(self, body):
        directory = self.root / "bin"
        directory.mkdir()
        script = directory / "git"
        script.write_text("#!/bin/sh\n" + body + "\nexec " + shlex.quote(GIT) + ' "$@"\n')
        script.chmod(0o755)
        return str(directory) + os.pathsep + self.env["PATH"]

    def select(self, *, want=0, needle="", **inputs):
        self.calls += 1
        output = self.root / f"environment-{self.calls}"
        env = dict(self.env, BASE_SHA="", PUSH_BEFORE=ZERO, EVENT_SHA=self.event,
                   DEFAULT_BRANCH="main", GITHUB_ENV=str(output))
        env.update(inputs)
        result = subprocess.run(["bash", str(SELECTOR)], cwd=self.worker, env=env,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, want, result.stdout + result.stderr)
        self.assertIn(needle, result.stdout + result.stderr)
        if want:
            self.assertFalse(output.exists(), "a refused selection published a base")
            return None
        value = output.read_text()
        self.assertRegex(value, r"\ASP_LANE_B_BASE=[0-9a-f]{40}\n\Z")
        return value.removeprefix("SP_LANE_B_BASE=").strip()

    def test_new_ref_fetches_its_missing_parent(self):
        self.clone()
        missing = subprocess.run([GIT, "cat-file", "-e", self.first], cwd=self.worker,
                                 env=self.env, capture_output=True)
        self.assertNotEqual(missing.returncode, 0)
        self.assertEqual(self.select(), self.first)

    def test_moving_ref_does_not_replace_event_ancestry(self):
        self.clone()
        self.git(self.remote, "switch", "-q", "event")
        later = self.commit("later tip")
        self.assertNotEqual(later, self.event)
        self.assertEqual(self.select(), self.first)

    def test_complete_history_needs_no_fetch(self):
        self.clone(shallow=False)
        path = self.stub('if [ "$1" = fetch ]; then exit 23; fi')
        self.assertEqual(self.select(PATH=path), self.first)

    def test_pull_request_base_takes_precedence(self):
        self.clone()
        self.assertEqual(self.select(BASE_SHA=self.main, PUSH_BEFORE=self.first), self.main)

    def test_existing_push_uses_before(self):
        self.clone()
        self.assertEqual(self.select(PUSH_BEFORE=self.main), self.main)

    def test_root_uses_default_branch(self):
        self.clone(shallow=False)
        self.assertEqual(self.select(EVENT_SHA=self.first), self.main)

    def test_parent_fetch_failure_is_not_a_root(self):
        self.clone()
        path = self.stub('if [ "$1" = fetch ]; then exit 23; fi')
        self.select(want=2, needle="could not fetch the event parent", PATH=path)

    def test_unreadable_event_object_is_not_a_root(self):
        self.clone()
        path = self.stub('if [ "$1" = cat-file ] && [ "$2" = commit ]; then exit 23; fi')
        self.select(want=2, needle="could not read the event commit object", PATH=path)

    def test_missing_parent_in_complete_repository_refuses(self):
        self.clone(shallow=False)
        tree = self.git(self.worker, "rev-parse", "HEAD^{tree}")
        commit = (f"tree {tree}\nparent {'0' * 39}2\n"
                  "author Fixture <fixture@invalid> 1 +0000\n"
                  "committer Fixture <fixture@invalid> 1 +0000\n\nmissing parent\n")
        event = self.git(self.worker, "hash-object", "-t", "commit", "-w", "--stdin", input=commit)
        self.select(want=2, needle="event parent is missing from a complete repository", EVENT_SHA=event)

    def test_unreadable_shallow_state_refuses(self):
        self.clone()
        path = self.stub('if [ "$1" = rev-parse ] && [ "$2" = --is-shallow-repository ]; then exit 23; fi')
        self.select(want=2, needle="could not read shallow-repository state", PATH=path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
