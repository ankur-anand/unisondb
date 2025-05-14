package umetrics

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally/v4"
)

func TestFallbackAutoScope_WithInvalidRandomPath(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "ankur.go")
	err := os.WriteFile(tmpFile, []byte("package main"), 0644)
	assert.NoError(t, err)

	scope := fallbackAutoScope(tmpFile)

	counter := scope.Counter("no_op_test")
	assert.IsType(t, tally.NoopScope.Counter("x"), counter)
}
