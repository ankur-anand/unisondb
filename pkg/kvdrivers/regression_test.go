package kvdrivers

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Isolate regressions that can segfault in C or leak a database writer lock.
func runDriverSubprocess(t *testing.T) bool {
	t.Helper()
	const childEnv = "UNISONDB_KVDRIVERS_TEST_CHILD"
	if os.Getenv(childEnv) == t.Name() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.timeout=10s")
	cmd.Env = append(os.Environ(), childEnv+"="+t.Name())
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	return true
}
