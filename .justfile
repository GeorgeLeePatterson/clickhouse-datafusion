LOG := env('RUST_LOG', '')
ARROW_DEBUG := env('CLICKHOUSE_NATIVE_DEBUG_ARROW', '')

# List of features
features := 'federation cloud'

# List of Examples

examples := ""

default:
    @just --list

# --- TESTS ---

test-unit:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils,mocks -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils,mocks,federation -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils,federation -- --nocapture --show-output

# Runs unit tests first then integration
test:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils,mocks -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils,mocks,federation -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils,federation -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils --test "e2e" -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils,federation --test "e2e" -- --nocapture --show-output

test-one test_name:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils,mocks "{{ test_name }}" -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils "{{ test_name }}" -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils,mocks,federation "{{ test_name }}" -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils,federation "{{ test_name }}" -- --nocapture --show-output

test-integration test_name='':
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils --test "e2e" "{{ test_name }}" -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils,federation --test "e2e" "{{ test_name }}" -- --nocapture --show-output

test-e2e test_name='':
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils --test "e2e" "{{ test_name }}" -- --nocapture --show-output

test-federation test_name='':
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils,federation --test "e2e" "{{ test_name }}"  -- --nocapture --show-output

# --- COVERAGE ---

coverage:
    cargo llvm-cov clean --workspace
    cargo llvm-cov --no-report --ignore-filename-regex "(examples).*" -F test-utils,mocks
    cargo llvm-cov --no-report --ignore-filename-regex "(examples).*" -F test-utils
    cargo llvm-cov --no-report --ignore-filename-regex "(examples).*" -F test-utils,mocks,federation
    cargo llvm-cov --no-report --ignore-filename-regex "(examples).*" -F test-utils,federation
    cargo llvm-cov report -vv --html --output-dir coverage --open

coverage-lcov:
    cargo llvm-cov clean --workspace
    cargo llvm-cov --lcov --no-report --ignore-filename-regex "(examples).*" -F test-utils,mocks
    cargo llvm-cov --lcov --no-report --ignore-filename-regex "(examples).*" -F test-utils
    cargo llvm-cov --lcov --no-report --ignore-filename-regex "(examples).*" -F test-utils,mocks,federation
    cargo llvm-cov --lcov --no-report --ignore-filename-regex "(examples).*" -F test-utils,federation
    cargo llvm-cov report --lcov --output-path lcov.info

# --- EXAMPLES ---

example example:
    cargo run -F test-utils --example "{{ example }}"

# --- DOCS ---

docs:
    cargo doc --open

# --- MAINTENANCE ---

# Run checks CI will
checks:
    cargo +nightly fmt -- --check
    cargo +nightly clippy --all-features --all-targets
    cargo +stable clippy --all-features --all-targets -- -D warnings
    just -f {{justfile()}} test

# Initialize development environment for maintainers
init-dev:
    @echo "Installing development tools..."
    cargo install cargo-release || true
    cargo install git-cliff || true
    cargo install cargo-edit || true
    cargo install cargo-outdated || true
    cargo install cargo-audit || true
    @echo ""
    @echo "✅ Development tools installed!"
    @echo ""
    @echo "Next steps:"
    @echo "1. Get your crates.io API token from https://crates.io/settings/tokens"
    @echo "2. Add it as CARGO_REGISTRY_TOKEN in GitHub repo settings → Secrets"
    @echo "3. Use 'cargo release patch/minor/major' to create releases"
    @echo ""
    @echo "Useful commands:"
    @echo "  just release-dry patch  # Preview what would happen"
    @echo "  just check-outdated     # Check for outdated dependencies"
    @echo "  just audit              # Security audit"

# Check for outdated dependencies
check-outdated:
    cargo outdated

# Run security audit
audit:
    cargo audit

# Prepare a release (creates PR with version bumps and changelog)
prepare-release version:
    #!/usr/bin/env bash
    set -euo pipefail

    # Validate version format
    if ! [[ "{{version}}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "Error: Version must be in format X.Y.Z (e.g., 0.2.0)"
        exit 1
    fi

    # Parse version components
    IFS='.' read -r MAJOR MINOR PATCH <<< "{{version}}"

    # Get current version for release notes
    CURRENT_VERSION=$(grep -E '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')

    # Create release branch
    git checkout -b "release-v{{version}}"

    # Update version in root Cargo.toml (in [package] section)
    # This uses a more specific pattern to only match the version under [package]
    awk '/^\[package\]/ {in_package=1} in_package && /^version = / {gsub(/"[^"]*"/, "\"{{version}}\""); in_package=0} {print}' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml

    # Update clickhouse-datafusion version references in README files (if they exist)
    # Look for patterns like: clickhouse-datafusion = "0.1.1" or clickhouse-datafusion = { version = "0.1.1"
    for readme in README.md; do
        if [ -f "$readme" ]; then
            # Update simple dependency format
            sed -i '' "s/clickhouse-datafusion = \"[0-9]*\.[0-9]*\.[0-9]*\"/clickhouse-datafusion = \"{{version}}\"/" "$readme" || true
            # Update version field in dependency table format
            sed -i '' "s/clickhouse-datafusion = { version = \"[0-9]*\.[0-9]*\.[0-9]*\"/clickhouse-datafusion = { version = \"{{version}}\"/" "$readme" || true
        fi
    done

    # Update Cargo.lock
    cargo update --workspace

    # Generate full changelog
    echo "Generating changelog..."
    git cliff -o CHANGELOG.md

    # Generate release notes for this version
    echo "Generating release notes..."
    git cliff --unreleased --tag v{{version}} --strip header -o RELEASE_NOTES.md

    # Stage all changes
    git add Cargo.toml Cargo.lock CHANGELOG.md RELEASE_NOTES.md
    # Also add README files if they were modified
    git add README.md 2>/dev/null || true

    # Commit
    git commit -m "chore: prepare release v{{version}}"

    # Push branch
    git push origin "release-v{{version}}"

    echo ""
    echo "✅ Release preparation complete!"
    echo ""
    echo "Release notes preview:"
    echo "----------------------"
    head -20 RELEASE_NOTES.md
    echo ""
    echo "Next steps:"
    echo "1. Create a PR from the 'release-v{{version}}' branch"
    echo "2. Review and merge the PR"
    echo "3. After merge, run: just tag-release {{version}}"
    echo ""

# Tag a release after the PR is merged
tag-release version:
    #!/usr/bin/env bash
    set -euo pipefail

    # Ensure we're on main and up to date
    git checkout main
    git pull origin main

    # Verify the version in Cargo.toml matches
    CARGO_VERSION=$(grep -E '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    if [ "$CARGO_VERSION" != "{{version}}" ]; then
        echo "Error: Cargo.toml version ($CARGO_VERSION) does not match requested version ({{version}})"
        echo "Did the release PR merge successfully?"
        exit 1
    fi

    # Verify publish will work
    cargo publish --dry-run -p clickhouse-datafusion --no-verify

    # Create and push tag
    git tag -a "v{{version}}" -m "Release v{{version}}"
    git push origin "v{{version}}"

    echo ""
    echo "✅ Tag v{{version}} created and pushed!"
    echo "The release workflow will now run automatically."
    echo ""

# Preview what a release would do (dry run)
release-dry version:
    @echo "This would:"
    @echo "1. Create branch: release-v{{version}}"
    @echo "2. Update version to {{version}} in:"
    @echo "   - Cargo.toml (workspace.package section only)"
    @echo "   - README files (if they contain clickhouse-datafusion version references)"
    @echo "3. Update Cargo.lock (usually done automatically with Cargo.toml change)"
    @echo "4. Generate CHANGELOG.md"
    @echo "5. Generate RELEASE_NOTES.md"
    @echo "6. Create commit and push branch"
    @echo ""
    @echo "After PR merge, 'just tag-release {{version}}' would:"
    @echo "1. Tag the merged commit as v{{version}}"
    @echo "2. Push the tag (triggering release workflow)"
