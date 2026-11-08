package shardpilot

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestGitConfigPaths(t *testing.T) {
	if _, err := os.Stat(".git"); errors.Is(err, os.ErrNotExist) {
		t.Skip("requires a Git checkout")
	} else if err != nil {
		t.Fatal(err)
	}
	const pattern = `(GIT_CONFIG(_GLOBAL|_SYSTEM|_VALUE_[0-9]+)?|core[.](hooksPath|attributesFile|excludesFile)|include[.]path|commit[.]template)[^[:alnum:]_./-]{0,12}(/dev/null|os[.]devnull)([^[:alnum:]_./-]|$)`
	command := exec.Command("git", "grep", "-nIiE", "-e", pattern, "--and", "--not",
		"-e", `^[[:space:]]*(#|//|/\*|\*)`, "--",
		"*.go", "*.py", "*.sh", "*.js", "*.mjs", "*.cjs", "*.ts", "*.yml", "*.yaml", "*.json", "*.toml", "Makefile")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("Git config paths must use private temporary files.\n%s", output)
	}
	var status *exec.ExitError
	if !errors.As(err, &status) || status.ExitCode() != 1 {
		t.Fatalf("git grep failed: %v\n%s", err, output)
	}
}
