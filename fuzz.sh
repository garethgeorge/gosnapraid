#!/bin/bash

# This script finds and runs all fuzz tests in the current Go project.
# For each fuzz test, it runs it for 20s seconds.

set -e

# Find all packages in the project
packages=$(go list ./...)

for pkg in $packages; do
    # Find all fuzz tests in the package, redirecting stderr to /dev/null to suppress "no test files" warnings
    fuzz_tests=$(go test -list=. "$pkg" 2>/dev/null | grep ^Fuzz || true)

    if [ -n "$fuzz_tests" ]; then
        echo "--------------------------------------------------"
        echo "Found fuzz tests in package $pkg"
        echo "--------------------------------------------------"
        for test_name in $fuzz_tests; do
            echo "Running fuzz test: $test_name"
            go test -fuzz="$test_name" -fuzztime=20s -cover "$pkg"
        done
    fi
done

echo "All fuzz tests passed."
