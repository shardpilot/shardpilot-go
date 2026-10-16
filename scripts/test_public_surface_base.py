#!/usr/bin/env python3
"""Exercise the CI selector with local Git event fixtures; no network service."""
import json
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
                   EVENT_REF="refs/heads/event",
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

    def test_new_ref_fetches_its_published_ancestry(self):
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

    def test_complete_history_still_uses_published_ancestry(self):
        self.clone(shallow=False)
        self.assertEqual(self.select(), self.first)

    def test_pull_request_base_takes_precedence(self):
        self.clone()
        self.assertEqual(self.select(BASE_SHA=self.main, PUSH_BEFORE=self.first), self.main)

    def test_existing_push_uses_before(self):
        self.clone()
        self.assertEqual(self.select(PUSH_BEFORE=self.main), self.main)

    def test_already_published_root_selects_itself(self):
        self.clone(shallow=False)
        self.assertEqual(self.select(EVENT_SHA=self.first), self.first)

    def test_published_history_fetch_failure_refuses(self):
        self.clone()
        path = self.stub('if [ "$1" = fetch ]; then exit 23; fi')
        self.select(want=2, needle="could not fetch published comparison history", PATH=path)

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
        self.select(want=2, needle="could not fetch published comparison history", EVENT_SHA=event)

    def test_unreadable_shallow_state_refuses(self):
        self.clone()
        path = self.stub('if [ "$1" = rev-parse ] && [ "$2" = --is-shallow-repository ]; then exit 23; fi')
        self.select(want=2, needle="could not read shallow-repository state", PATH=path)

    def test_newer_published_side_branch_is_the_base(self):
        self.git(self.remote, "branch", "published", self.event)
        self.git(self.remote, "switch", "-q", "event")
        published = self.event
        self.event = self.commit("later event")
        self.clone()
        self.assertEqual(self.select(), published)

    def test_annotated_published_tag_is_the_base(self):
        self.git(self.remote, "tag", "-am", "published", "published", self.event)
        self.git(self.remote, "switch", "-q", "event")
        published = self.event
        self.event = self.commit("later event")
        self.clone()
        self.assertEqual(self.select(), published)

    def test_new_tag_excludes_its_own_published_tip(self):
        self.git(self.remote, "switch", "-q", "event")
        self.event = self.commit("later tag event")
        self.git(self.remote, "tag", "-am", "new tag", "new-tag", self.event)
        self.git(self.remote, "switch", "-q", "main")
        self.git(self.remote, "branch", "-D", "event")
        self.git(self.root, "clone", "-q", "--depth=1", "--branch", "new-tag",
                 self.remote.as_uri(), str(self.worker))
        self.assertEqual(self.select(EVENT_REF="refs/tags/new-tag"), self.first)

    def test_unrelated_root_does_not_guess_a_default_base(self):
        self.git(self.remote, "switch", "-q", "--orphan", "independent")
        self.event = self.commit("independent root")
        self.git(self.remote, "branch", "-D", "event")
        self.git(self.remote, "branch", "-m", "event")
        self.clone()
        self.select(want=2, needle="the event has no published ancestor")

    def test_incomparable_published_ancestors_refuse(self):
        self.git(self.remote, "switch", "-q", "event")
        self.git(self.remote, "branch", "published", self.event)
        self.git(self.remote, "merge", "-q", "--no-edit", "-s", "ours", "main")
        self.event = self.git(self.remote, "rev-parse", "HEAD")
        self.clone()
        self.select(want=2, needle="multiple incomparable published ancestors")

    def test_missing_event_ref_refuses(self):
        self.clone()
        self.select(want=2, needle="new-ref event has no ref name", EVENT_REF="")

    def test_unreadable_published_inventory_refuses(self):
        self.clone()
        path = self.stub('if [ "$1" = ls-remote ]; then exit 23; fi')
        self.select(want=2, needle="could not read published refs", PATH=path)


class WholeGatePublicationScenes(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="surface-publication-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.publisher = self.root / "publisher"
        self.publisher.mkdir()
        self.origin = self.root / "origin.git"
        self.worker = self.root / "worker"
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        for key in ("BASH_ENV", "ENV", "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE",
                    "PUBLIC_SURFACE_BASE_REF", "LANE_B_BASELINE"):
            self.env.pop(key, None)
        for name in ("check_public_surface.sh", "public-gate.sh",
                     "public-surface-lane-b-baseline.txt", SELECTOR.name):
            target = self.publisher / "scripts" / name
            target.parent.mkdir(exist_ok=True)
            shutil.copy2(SELECTOR.parent / name, target)
        self.readme = "An end-to-end workflow for github.com/shardpilot/shardpilot-go.\n"
        (self.publisher / "README.md").write_text(self.readme)
        (self.publisher / "config.go").write_text("package fixture\n")
        self.git(self.publisher, "init", "-q", "-b", "main")
        self.git(self.publisher, "config", "user.name", "surface-publication-test")
        self.git(self.publisher, "config", "user.email", "surface-publication-test@invalid")
        self.git(self.root, "init", "-q", "--bare", "-b", "main", str(self.origin))
        self.git(self.publisher, "remote", "add", "origin", self.origin.as_uri())
        self.git(self.publisher, "add", "-A")
        self.gate(self.publisher, write=True)
        self.base = self.commit("published zero baseline")
        self.git(self.publisher, "push", "-q", "origin", "main")

    def git(self, cwd, *args):
        return subprocess.check_output([GIT, *args], cwd=cwd, env=self.env,
                                       text=True, stderr=subprocess.PIPE).strip()

    def commit(self, message):
        self.git(self.publisher, "add", "-A")
        self.git(self.publisher, "commit", "-qm", message)
        return self.git(self.publisher, "rev-parse", "HEAD")

    def gate(self, cwd, *, base=None, write=False, want=0):
        env = dict(self.env)
        if base is not None:
            env["PUBLIC_SURFACE_BASE_REF"] = base
        args = ["bash", "scripts/check_public_surface.sh"]
        if write:
            args.append("--write-baseline")
        result = subprocess.run(args, cwd=cwd, env=env, capture_output=True,
                                text=True, timeout=90)
        output = result.stdout + result.stderr
        prefix = "RECEIPT scripts/check_public_surface.sh: "
        receipts = [json.loads(line[len(prefix):]) for line in output.splitlines()
                    if line.startswith(prefix)]
        self.assertEqual(len(receipts), 1, output)
        receipt = receipts[0]
        self.assertEqual(receipt["read"], receipt["expected"], output)
        self.assertGreater(receipt["read"], 0, output)
        self.assertFalse(receipt["aborted"], output)
        self.assertEqual(result.returncode, want, output)
        self.assertEqual(receipt["findings"] == 0, want == 0, output)
        print(json.dumps({"scene": self._testMethodName, "base": base,
                          "exit": result.returncode, "receipt": receipt}), flush=True)

    def increase(self):
        marker = "AD" + "R-" + "9999"
        (self.publisher / "config.go").write_text("package fixture\n\n// See " + marker + ".\n")
        self.git(self.publisher, "add", "config.go")
        self.gate(self.publisher, write=True)
        baseline = (self.publisher / "scripts/public-surface-lane-b-baseline.txt").read_text()
        self.assertIn("\n1 config.go\n", baseline)
        return self.commit("new source and baseline increase")

    def document(self, text):
        (self.publisher / "README.md").write_text(self.readme + text + "\n")
        return self.commit(text)

    def publish(self, branch, commits):
        absent = subprocess.run([GIT, "--git-dir", str(self.origin), "show-ref", "--verify",
                                 "refs/heads/" + branch], env=self.env, capture_output=True)
        self.assertNotEqual(absent.returncode, 0)
        for sha in commits:
            absent = subprocess.run([GIT, "--git-dir", str(self.origin), "cat-file", "-e",
                                     sha + "^{commit}"], env=self.env, capture_output=True)
            self.assertNotEqual(absent.returncode, 0, "commit was already published")
        self.git(self.publisher, "push", "-q", "origin", branch)

    def select(self, branch, tip, before=ZERO):
        self.git(self.root, "clone", "-q", "--depth=1", "--branch", branch,
                 self.origin.as_uri(), str(self.worker))
        self.assertEqual(self.git(self.worker, "rev-parse", "--is-shallow-repository"), "true")
        output = self.root / "environment"
        env = dict(self.env, BASE_SHA="", PUSH_BEFORE=before, EVENT_SHA=tip,
                   EVENT_REF="refs/heads/" + branch, DEFAULT_BRANCH="main", GITHUB_ENV=str(output))
        result = subprocess.run(["bash", str(SELECTOR)], cwd=self.worker, env=env,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        exported = output.read_text()
        self.assertRegex(exported, r"\ASP_LANE_B_BASE=[0-9a-f]{40}\n\Z")
        return exported.removeprefix("SP_LANE_B_BASE=").strip()

    def test_new_multi_commit_increase_is_refused(self):
        self.git(self.publisher, "switch", "-q", "-c", "new-multiple")
        first = self.increase()
        tip = self.document("Later ordinary documentation.")
        self.publish("new-multiple", [first, tip])
        selected = self.select("new-multiple", tip)
        self.git(self.worker, "fetch", "-q", "--depth=1", "origin", self.base)
        self.gate(self.worker, base=self.base, want=1)
        self.gate(self.worker, base=selected, want=1)

    def test_new_single_commit_increase_is_refused(self):
        self.git(self.publisher, "switch", "-q", "-c", "new-single")
        tip = self.increase()
        self.publish("new-single", [tip])
        self.gate(self.worker, base=self.select("new-single", tip), want=1)

    def test_new_clean_multi_commit_ref_passes(self):
        self.git(self.publisher, "switch", "-q", "-c", "new-clean")
        first = self.document("First ordinary documentation.")
        tip = self.document("Second ordinary documentation.")
        self.publish("new-clean", [first, tip])
        self.gate(self.worker, base=self.select("new-clean", tip))

    def test_existing_ref_ordinary_push_passes(self):
        self.git(self.publisher, "switch", "-q", "-c", "existing")
        before = self.increase()
        self.publish("existing", [before])
        self.assertEqual(self.git(self.root, "--git-dir", str(self.origin), "rev-parse",
                                  "refs/heads/existing"), before)
        tip = self.document("Later ordinary documentation.")
        self.git(self.publisher, "push", "-q", "origin", "existing")
        self.gate(self.worker, base=self.select("existing", tip, before))


if __name__ == "__main__":
    unittest.main(verbosity=2)
