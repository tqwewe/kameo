#!/bin/bash

echo "🔍 Running CI-like checks locally..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test functions
run_test() {
    local name="$1"
    local command="$2"
    echo -e "${YELLOW}Running $name...${NC}"
    if eval "$command"; then
        echo -e "${GREEN}✅ $name passed${NC}"
        return 0
    else
        echo -e "${RED}❌ $name failed${NC}"
        return 1
    fi
}

# Track failures
FAILED_TESTS=()

# 1. Basic compilation (what CI does first)
if ! run_test "Basic build" "cargo build --features remote --no-default-features"; then
    FAILED_TESTS+=("Basic build")
fi

# 2. Clippy with warnings as errors (the critical CI check)
if ! run_test "Clippy strict" "cargo clippy --features remote --no-default-features -- -D warnings"; then
    FAILED_TESTS+=("Clippy strict")
fi

# 3. Tests
if ! run_test "Tests" "cargo test --features remote --no-default-features"; then
    FAILED_TESTS+=("Tests")
fi

# 4. Check examples compile
if ! run_test "Examples build" "cargo build --examples --features remote --no-default-features"; then
    FAILED_TESTS+=("Examples build")
fi

# Summary
echo ""
echo "🏁 CI Test Summary"
echo "=================="

if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo -e "${GREEN}🎉 All CI checks passed!${NC}"
    echo "Your code should pass GitHub CI."
    exit 0
else
    echo -e "${RED}💥 ${#FAILED_TESTS[@]} CI check(s) failed:${NC}"
    for test in "${FAILED_TESTS[@]}"; do
        echo -e "  ${RED}❌ $test${NC}"
    done
    echo ""
    echo "Fix these issues before committing to ensure CI passes."
    exit 1
fi