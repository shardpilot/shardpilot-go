#!/usr/bin/env python3
"""Exercise history coverage refusals and the synthetic literal exception."""
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest

HERE = pathlib.Path(__file__).resolve().parent
SUBJECT = HERE / "check_secret_scan.py"
CONFIG = HERE.parent / ".gitleaks.toml"
CONTAINER = "--container" in sys.argv
if CONTAINER:
    sys.argv.remove("--container")


class SecretScanScenes(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="secret-scan-scenes-")
        self.root = pathlib.Path(self.temp.name).resolve()
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.env = os.environ.copy()
        self.env.update({
            "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Synthetic", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Synthetic", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        })
        self.git("init", "-q", "-b", "main")
        (self.repo / "content.txt").write_text("public fixture\n")
        self.commit("Synthetic root")

    def tearDown(self):
        self.temp.cleanup()

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-C", str(self.repo), *args], env=self.env,
            text=True, stderr=subprocess.STDOUT,
        )

    def commit(self, message):
        self.git("add", ".")
        self.git("-c", "commit.gpgsign=false", "commit", "-qm", message)

    def stub(self, mode):
        folder = self.root / "bin"
        folder.mkdir(exist_ok=True)
        scanner = folder / "gitleaks"
        scanner.write_text(
            "#!" + sys.executable + "\n"
            "import os,subprocess,sys\n"
            "mode=os.environ['SCAN_SCENE']\n"
            "if mode=='missing': print('no leaks found'); sys.exit(0)\n"
            "if mode=='failure': print('1 commits scanned.'); sys.exit(7)\n"
            "if mode=='changed': subprocess.run(['git','update-ref','refs/heads/added','HEAD'],check=True)\n"
            "count=0 if mode=='zero' else 2 if mode=='wrong' else 1\n"
            "print(str(count)+' commits scanned.')\n"
            "if mode=='duplicate': print('1 commits scanned.')\n"
        )
        scanner.chmod(0o755)
        self.env["PATH"] = str(folder) + os.pathsep + os.environ["PATH"]
        self.env["SCAN_SCENE"] = mode

    def run_subject(self, container=False):
        return subprocess.run(
            [sys.executable, str(SUBJECT), *(["--container"] if container else []), str(self.repo)],
            env=self.env, text=True, capture_output=True,
        )

    def refused(self, result, reason):
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn(reason, result.stderr)
        self.assertNotIn("HISTORY CORPUS verified:", result.stdout)

    def test_matching_material_count_passes(self):
        self.stub("okay")
        result = self.run_subject()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"total": 1', result.stdout)
        self.assertIn('"refs": 1', result.stdout)
        self.assertIn("verified: 1 material commits", result.stdout)

    def test_zero_commit_success_is_refused(self):
        self.stub("zero")
        self.refused(self.run_subject(), "count disagrees")

    def test_wrong_nonzero_count_is_refused(self):
        self.stub("wrong")
        self.refused(self.run_subject(), "count disagrees")

    def test_missing_count_is_refused(self):
        self.stub("missing")
        self.refused(self.run_subject(), "exactly one commit count")

    def test_duplicate_count_is_refused(self):
        self.stub("duplicate")
        self.refused(self.run_subject(), "exactly one commit count")

    def test_scanner_failure_is_refused(self):
        self.stub("failure")
        self.refused(self.run_subject(), "gitleaks exited with status 7")

    def test_ref_change_during_scan_is_refused(self):
        self.stub("changed")
        self.refused(self.run_subject(), "corpus changed")

    def test_unreadable_expected_corpus_is_refused(self):
        self.stub("okay")
        real_git = shutil.which("git")
        shim = self.root / "bin/git"
        shim.write_text(
            "#!/bin/sh\nfor arg do\n"
            "  if [ \"$arg\" = rev-list ]; then exit 17; fi\n"
            "done\nexec " + real_git + " \"$@\"\n"
        )
        shim.chmod(0o755)
        self.refused(self.run_subject(), "git rev-list could not measure")

    def test_empty_material_corpus_is_refused(self):
        self.git("checkout", "--orphan", "empty")
        (self.repo / "content.txt").unlink()
        self.git("add", "-u")
        self.git("-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "Empty root")
        self.git("branch", "-D", "main")
        self.stub("zero")
        self.refused(self.run_subject(), "material commit corpus is empty")

    def test_empty_commits_are_declared_not_mistaken_for_missing_scans(self):
        self.git("-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "Empty commit")
        self.stub("okay")
        result = self.run_subject()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"total": 2', result.stdout)
        self.assertIn('"excluded_from_patch_stream": 1', result.stdout)

    def test_shallow_history_is_refused(self):
        (self.repo / "second.txt").write_text("second fixture\n")
        self.commit("Second")
        shallow = self.root / "shallow"
        subprocess.check_output(
            ["git", "clone", "-q", "--depth=1", self.repo.as_uri(), str(shallow)],
            env=self.env, stderr=subprocess.STDOUT,
        )
        self.repo = shallow
        self.assertEqual(self.git("rev-parse", "--is-shallow-repository").strip(), "true")
        self.stub("okay")
        self.refused(self.run_subject(), "repository is shallow")

    def test_actual_merge_and_empty_commit_corpus(self):
        self.git("checkout", "-qb", "side")
        (self.repo / "side.txt").write_text("side fixture\n")
        self.commit("Side")
        self.git("checkout", "-q", "main")
        (self.repo / "main.txt").write_text("main fixture\n")
        self.commit("Main")
        self.git("-c", "commit.gpgsign=false", "merge", "--no-ff", "-qm", "Merge", "side")
        self.git("-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "Empty")
        result = self.run_subject(CONTAINER)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"total": 5', result.stdout)
        self.assertIn('"expected": 4', result.stdout)
        self.assertIn("verified: 4 material commits", result.stdout)

    def test_metadata_and_type_changes_have_exact_corpus(self):
        changed = self.repo / "next\tline\nname.txt"
        self.git("mv", "content.txt", changed.name)
        self.commit("Rename")
        changed.chmod(0o755)
        self.commit("Executable mode")
        changed.unlink()
        changed.symlink_to("public-target")
        self.commit("File type")
        result = self.run_subject(CONTAINER)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"total": 4', result.stdout)
        self.assertIn('"expected": 2', result.stdout)
        self.assertIn("verified: 2 material commits", result.stdout)

    def test_removed_lines_in_a_modified_file_still_form_a_patch(self):
        (self.repo / "content.txt").write_text("first fixture\nsecond fixture\n")
        self.commit("Two lines")
        (self.repo / "content.txt").write_text("first fixture\n")
        self.commit("Remove one line")
        result = self.run_subject(CONTAINER)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"total": 3', result.stdout)
        self.assertIn('"expected": 3', result.stdout)
        self.assertIn("verified: 3 material commits", result.stdout)

    def scan_directory(self, config):
        args = [
            "dir", "--redact", "--no-banner", "--no-color", "--config", config,
            "--report-format", "json", "--report-path", "-",
        ]
        if CONTAINER:
            command = [
                "docker", "run", "--rm", "--volume", str(self.repo) + ":/repo:ro",
                "--workdir", "/repo", "ghcr.io/gitleaks/gitleaks:v8.30.1",
                *args, "/repo",
            ]
        else:
            command = ["gitleaks", *args, str(self.repo)]
        result = subprocess.run(command, cwd=self.repo, env=self.env, text=True, capture_output=True)
        self.assertIn(result.returncode, (0, 1), result.stderr)
        findings = json.loads(result.stdout)
        return [f for f in findings if f["File"].endswith("fixture.txt")]

    def literal_fixture(self, suffix, prefix=""):
        config = CONFIG.read_text()
        patterns = [
            p for a in tomllib.loads(config)["allowlists"] for p in a["regexes"]
            if "header" in p
        ]
        self.assertEqual(len(patterns), 1)
        pattern = patterns[0]
        literal = pattern.removeprefix("^").removesuffix("$").replace(r"\.", ".")
        (self.repo / ".gitleaks.toml").write_text(config)
        (self.repo / "defaults.toml").write_text("[extend]\nuseDefault = true\n")
        # A synthetic sanitiser fixture, never a usable credential.
        (self.repo / "fixture.txt").write_text('api_key = "' + prefix + literal + suffix + '"\n')

    def test_extended_literal_is_not_an_exception(self):
        self.literal_fixture("q7W9r2T4u6I8o0P3a5S7d9F1g3H5j7K9")
        self.assertGreater(len(self.scan_directory("defaults.toml")), 0)
        self.assertGreater(len(self.scan_directory(".gitleaks.toml")), 0)

    def test_prefixed_literal_is_not_an_exception(self):
        self.literal_fixture("", "q7W9r2T4u6I8o0P3a5S7d9F1g3H5j7K9")
        self.assertGreater(len(self.scan_directory("defaults.toml")), 0)
        self.assertGreater(len(self.scan_directory(".gitleaks.toml")), 0)

    def test_exact_literal_keeps_its_exception(self):
        self.literal_fixture("")
        self.assertGreater(len(self.scan_directory("defaults.toml")), 0)
        self.assertEqual(self.scan_directory(".gitleaks.toml"), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
