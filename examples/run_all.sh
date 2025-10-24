#!/bin/bash
# Run all examples and fail if any fail
# This script tests all clickhouse-datafusion examples with proper feature flags
# Examples and their features are automatically discovered from Cargo.toml

set -e  # Exit immediately if any command fails

# Start total timer
total_start=$(date +%s)

echo "🚀 Running all clickhouse-datafusion examples..."
echo "================================================"
echo ""

# Parse Cargo.toml to extract examples and their required features
examples_data=$(grep -A 2 '^\[\[example\]\]' Cargo.toml | \
    grep -E 'name =|required-features' | \
    paste - - | \
    sed 's/name = "\(.*\)"/\1/' | \
    sed 's/required-features = \[//' | \
    sed 's/\]//' | \
    sed 's/"//g')

# Count total examples
total=$(echo "$examples_data" | wc -l)

# Track success/failure
passed=0
failed=0
current=0

# Run each example
while IFS=$'\t' read -r example features; do
    current=$((current + 1))

    echo "[$current/$total] Running example: $example (features: $features)"
    echo "---------------------------------------------------"

    # Start example timer
    example_start=$(date +%s)

    if cargo run --example "$example" --features "$features" --release > /dev/null 2>&1; then
        example_end=$(date +%s)
        elapsed=$((example_end - example_start))
        echo "✅ PASS: $example (${elapsed}s)"
        passed=$((passed + 1))
    else
        example_end=$(date +%s)
        elapsed=$((example_end - example_start))
        echo "❌ FAIL: $example (${elapsed}s)"
        failed=$((failed + 1))
        echo ""
        echo "Example $example failed. Aborting..."
        exit 1
    fi

    echo ""
done <<< "$examples_data"

# Summary
total_end=$(date +%s)
total_elapsed=$((total_end - total_start))

echo "================================================"
echo "✅ All examples passed! ($passed/$total)"
echo "⏱️  Total time: ${total_elapsed}s"
echo "================================================"

exit 0
