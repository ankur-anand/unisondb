package umetrics

import (
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/uber-go/tally/v4"
)

type Scope = tally.Scope

type CallerResolverFunc func() string

const unknownPackage = "unknown"

var customScopeResolver CallerResolverFunc

func init() {
	SetCustomScopeResolver(scopeFromFilePath(2))
}

// scopeFromFilePath returns a resolver that extracts package name from the caller's file path.
func scopeFromFilePath(skip int) CallerResolverFunc {
	return func() string {
		_, file, _, ok := runtime.Caller(skip)
		if !ok {
			return unknownPackage
		}
		return extractPackageName(file)
	}
}

// e.g., /app/dbkernel/hlc.go -> dbkernel.
func extractPackageName(file string) string {
	dir := filepath.Dir(file)
	pkg := filepath.Base(dir)

	if pkg == "." || pkg == "/" || pkg == "" {
		return unknownPackage
	}

	// remove up any dots or slashes
	pkg = strings.TrimPrefix(pkg, ".")
	pkg = strings.TrimSuffix(pkg, "/")

	return pkg
}

// AutoScope returns a tally.Scope scoped by the caller's package name.
func AutoScope() tally.Scope {
	if customScopeResolver != nil {
		return GetScope(customScopeResolver())
	}
	slog.Warn("[unisondb.umetrics] AutoScope: no custom scope resolver set, returning NoopScope")
	return tally.NoopScope
}

// SetCustomScopeResolver allows injecting a function to control scope resolution globally.
func SetCustomScopeResolver(f CallerResolverFunc) {
	customScopeResolver = f
}

// AutoScopeWithSkip allows advanced callers to control runtime.Caller depth.
func AutoScopeWithSkip(skip int) tally.Scope {
	_, file, _, ok := runtime.Caller(skip)
	if !ok {
		slog.Warn("[unisondb.umetrics] AutoScopeWithSkip: unable to determine caller, returning NoopScope")
		return tally.NoopScope
	}

	pkg := extractPackageName(file)
	if pkg == unknownPackage {
		return tally.NoopScope
	}

	return GetScope(pkg)
}
