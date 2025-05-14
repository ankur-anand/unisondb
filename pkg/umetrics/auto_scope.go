package umetrics

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/uber-go/tally/v4"
)

type Scope = tally.Scope

type CallerResolverFunc func() string

var (
	moduleOnce          sync.Once
	cachedModPath       string
	cachedModDir        string
	customScopeResolver CallerResolverFunc
)

// AutoScope returns a tally.Scope scoped by the caller’s package name.
// It supports module-aware scoping, global override, and safe fallback.
// nolint:ireturn
func AutoScope() tally.Scope {
	if customScopeResolver != nil {
		return GetScope(customScopeResolver())
	}
	return AutoScopeWithSkip(2)
}

// SetCustomScopeResolver allows injecting a function to control scope resolution globally.
func SetCustomScopeResolver(f CallerResolverFunc) {
	customScopeResolver = f
}

// AutoScopeWithSkip allows advanced callers to control runtime.Caller depth.
// nolint:ireturn
func AutoScopeWithSkip(skip int) tally.Scope {
	_, file, _, ok := runtime.Caller(skip)
	if !ok {
		slog.Warn("[unisondb.umetrics] AutoScopeWithSkip: unable to determine caller, returning NoopScope")
		return tally.NoopScope
	}

	moduleOnce.Do(func() {
		cachedModPath, cachedModDir = findGoModuleRoot(file)
	})

	if cachedModPath == "" || cachedModDir == "" {
		slog.Warn("[unisondb.umetrics] AutoScopeWithSkip: go.mod not found, falling back to GOPATH logic", "file", file)
		return fallbackAutoScope(file)
	}

	rel, err := filepath.Rel(cachedModDir, filepath.Dir(file))
	if err != nil {
		slog.Warn("[unisondb.umetrics] AutoScopeWithSkip: cannot compute relative import path", "error", err)
		return tally.NoopScope
	}

	importPath := filepath.ToSlash(filepath.Join(cachedModPath, rel))
	last := filepath.Base(importPath)
	return GetScope(last)
}

// fallbackAutoScope handles GOPATH-style projects.
// nolint:ireturn
func fallbackAutoScope(file string) tally.Scope {
	for _, gopath := range filepath.SplitList(os.Getenv("GOPATH")) {
		src := filepath.Join(gopath, "src")
		if rel, err := filepath.Rel(src, file); err == nil && !strings.HasPrefix(rel, "..") {
			return GetScope(filepath.Base(filepath.Dir(rel)))
		}
	}
	return tally.NoopScope
}

// findGoModuleRoot locates the go.mod file and extracts module name + directory.
func findGoModuleRoot(start string) (modulePath, moduleDir string) {
	dir := filepath.Dir(start)
	for {
		goModPath := filepath.Join(dir, "go.mod")
		f, err := os.Open(goModPath)
		if err == nil {
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "module ") {
					modulePath = strings.TrimSpace(strings.TrimPrefix(line, "module"))
					moduleDir = dir
					return
				}
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", ""
}

// GetModulePath returns the module path (from go.mod), if available.
func GetModulePath() string {
	moduleOnce.Do(func() {
		_, file, _, ok := runtime.Caller(1)
		if ok {
			cachedModPath, cachedModDir = findGoModuleRoot(file)
		}
	})
	return cachedModPath
}

// GetModuleDir returns the directory where go.mod was found, if any.
func GetModuleDir() string {
	moduleOnce.Do(func() {
		_, file, _, ok := runtime.Caller(1)
		if ok {
			cachedModPath, cachedModDir = findGoModuleRoot(file)
		}
	})
	return cachedModDir
}
