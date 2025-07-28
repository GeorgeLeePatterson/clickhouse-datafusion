LOG := env('RUST_LOG', '')
ARROW_DEBUG := env('CLICKHOUSE_NATIVE_DEBUG_ARROW', '')

# List of features
features := 'federation cloud'

# List of Examples

examples := ""

default:
    @just --list

# --- TESTS ---

# Runs unit tests first then integration
test:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     --no-default-features -F test-utils -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test --lib \
     -F test-utils -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     --no-default-features -F test-utils --test "e2e" -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils --test "e2e" -- --nocapture --show-output

test-one test_name:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils "{{ test_name }}" -- --nocapture --show-output

test-integration test_name='':
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     --no-default-features -F test-utils --test "e2e" "{{ test_name }}" -- --nocapture --show-output
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils --test "e2e" "{{ test_name }}" -- --nocapture --show-output

test-e2e test_name='':
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     --no-default-features -F test-utils --test "e2e" "{{ test_name }}" -- --nocapture --show-output

test-federation test_name='':
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test-utils --test "e2e" "{{ test_name }}"  -- --nocapture --show-output

coverage:
    cargo llvm-cov clean
    cargo llvm-cov --ignore-filename-regex "(examples).*" --no-default-features -F test-utils --no-report
    cargo llvm-cov --ignore-filename-regex "(examples).*" -F test-utils --no-report
    cargo llvm-cov report --html --output-dir coverage --open
