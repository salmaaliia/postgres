#!/usr/bin/env bash
# ==============================================================================
# test_btree_merge.sh
#
# Automated End-to-End Test Suite for PostgreSQL B-Tree Leaf Page Merging:
#   - Build & Setup (configure, compile, initdb, extensions)
#   - Section 1: Basic Leaf Page Merge, VACUUM Cleanup & amcheck Validation
#   - Section 2: Page Split on a Merged Page
#   - Section 3: Concurrent Scan Recovery across Merge Group Boundaries
#       * Case 1: Forward Scan Wait  → Simple Merge
#       * Case 2: Backward Scan Wait → Simple Merge
#       * Case 3: Forward Scan Wait  → Merge → Split
#       * Case 4: Backward Scan Wait → Merge → Split
#
# Usage:
#   ./test_btree_merge.sh              # Full clean, build, and test run
#   ./test_btree_merge.sh --skip-build # Skip distclean/build; test current binaries
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Configuration & Paths
# ------------------------------------------------------------------------------
PG_DEV="${HOME}/pg-dev"
PG_DATA="${HOME}/pg-data"
PG_PORT="${PGPORT:-5543}"
PG_BIN="${PG_DEV}/bin"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PATH="${PG_BIN}:${PATH}"

# Terminal color output
GREEN="\033[1;32m"
RED="\033[1;31m"
BLUE="\033[1;34m"
YELLOW="\033[1;33m"
NC="\033[0m"

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_fail()    { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

# ------------------------------------------------------------------------------
# Cleanup Handler
# ------------------------------------------------------------------------------
cleanup() {
    log_info "Stopping PostgreSQL test cluster if running..."
    "${PG_BIN}/pg_ctl" -D "${PG_DATA}" stop -m immediate 2>/dev/null || true
    rm -f /tmp/fwd_case*.out /tmp/bwd_case*.out
}
trap cleanup EXIT

# ------------------------------------------------------------------------------
# Helper: Assert a psql integer result matches expected value
# ------------------------------------------------------------------------------
assert_count() {
    local label="$1"
    local expected="$2"
    local sql="$3"

    local actual
    actual=$("${PG_BIN}/psql" -p "${PG_PORT}" -d postgres -t -A -c "${sql}" 2>/dev/null || echo "ERROR")

    if [ "${actual}" != "${expected}" ]; then
        log_fail "${label}: expected ${expected} rows, got '${actual}'"
    fi
    log_success "${label}: got expected count of ${expected}"
}

# ------------------------------------------------------------------------------
# Helper: Assert count from a background query output file
# ------------------------------------------------------------------------------
assert_file_count() {
    local label="$1"
    local expected="$2"
    local file="$3"

    local actual
    actual=$(tr -d '[:space:]' < "${file}")

    if [ "${actual}" != "${expected}" ]; then
        log_fail "${label}: expected ${expected} rows, got '${actual}'"
    fi
    log_success "${label}: got expected count of ${expected}"
}

# ------------------------------------------------------------------------------
# Helper: Wait for background backend to reach a named injection point
# ------------------------------------------------------------------------------
wait_for_injection_point() {
    local point_name="$1"
    local max_wait=100  # 20 seconds total (100 * 0.2s)
    local count=0

    while [ "${count}" -lt "${max_wait}" ]; do
        local waiting
        waiting=$("${PG_BIN}/psql" -p "${PG_PORT}" -d postgres -t -A -c "
            SELECT count(*) FROM pg_stat_activity 
            WHERE wait_event_type = 'InjectionPoint' AND wait_event = '${point_name}';
        " 2>/dev/null || echo "0")

        if [ "${waiting}" -ge 1 ]; then
            return 0
        fi
        sleep 0.2
        count=$((count + 1))
    done

    log_fail "Timed out waiting for backend to pause at injection point '${point_name}'"
}

# ------------------------------------------------------------------------------
# Build and Setup Phase
# ------------------------------------------------------------------------------
SKIP_BUILD=false
if [[ "${1:-}" == "--skip-build" ]]; then
    SKIP_BUILD=true
fi

if [ "${SKIP_BUILD}" = false ]; then
    log_info "=== [BUILD 1/4] Cleaning previous builds and data ==="
    cd "${SOURCE_DIR}"
    make distclean 2>/dev/null || true
    rm -rf "${PG_DATA}"

    log_info "=== [BUILD 2/4] Configuring PostgreSQL with debug & injection points ==="
    ./configure --prefix="${PG_DEV}" \
                --enable-debug \
                --enable-depend \
                --enable-injection-points \
                --enable-tap-tests \
                --enable-cassert \
                --with-lz4 \
                --with-zstd \
                CFLAGS="-ggdb -O0 -fno-omit-frame-pointer -DWAL_DEBUG"

    log_info "=== [BUILD 3/4] Compiling and installing PostgreSQL + extensions ==="
    make -j"$(nproc)" install
    make -j"$(nproc)" -C contrib/pageinspect install
    make -j"$(nproc)" -C contrib/amcheck install
    make -j"$(nproc)" -C src/test/modules/injection_points install
else
    log_info "Skipping build phase as requested (--skip-build)."
    rm -rf "${PG_DATA}"
fi

log_info "=== [INIT] Initializing fresh database cluster on port ${PG_PORT} ==="
"${PG_BIN}/initdb" -D "${PG_DATA}" --no-instructions -A trust >/dev/null

cat >> "${PG_DATA}/postgresql.conf" <<CONF
port = ${PG_PORT}
autovacuum = off
shared_preload_libraries = 'injection_points'
CONF

"${PG_BIN}/pg_ctl" -D "${PG_DATA}" -l "${PG_DATA}/server.log" start -w

PSQL="${PG_BIN}/psql -p ${PG_PORT} -d postgres -v ON_ERROR_STOP=1 -X"

log_info "Creating required extensions and configuring scan parameters..."
$PSQL <<'SQL'
CREATE EXTENSION IF NOT EXISTS amcheck;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS injection_points;
ALTER SYSTEM SET enable_seqscan = false;
SELECT pg_reload_conf();
SQL

# ==============================================================================
# SECTION 1: Basic Leaf Page Merge, VACUUM Cleanup & amcheck Validation
# ==============================================================================
log_info "======================================================================"
log_info "SECTION 1: Basic Leaf Page Merge, VACUUM Cleanup & amcheck"
log_info "======================================================================"

$PSQL <<'SQL'
DROP TABLE IF EXISTS merge_test CASCADE;

CREATE TABLE merge_test (id int);
ALTER TABLE merge_test SET (autovacuum_enabled = false);
INSERT INTO merge_test SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX merge_test_idx ON merge_test(id);

DELETE FROM merge_test WHERE id % 20 != 0;
VACUUM merge_test;

SELECT * FROM bt_find_merge_candidates('merge_test_idx', 10.0, 90.0, 10);
SELECT * FROM bt_merge_detail('merge_test_idx', 4, true);

SELECT * FROM bt_merge('merge_test_idx', 10.0, 90.0, 10);

SELECT * FROM bt_index_parent_check('merge_test_idx');
SELECT * FROM bt_index_check('merge_test_idx');

SELECT count(1) AS matching_rows FROM merge_test WHERE id BETWEEN 1000 AND 3000;

-- Trigger VACUUM to reclaim tombstone (BTP_MERGED_AWAY) pages
INSERT INTO merge_test (id) VALUES (-1);
DELETE FROM merge_test WHERE id = -1;
VACUUM merge_test;

SELECT * FROM bt_find_merge_candidates('merge_test_idx', 10.0, 90.0, 10);

SELECT * FROM bt_index_parent_check('merge_test_idx');
SELECT * FROM bt_index_check('merge_test_idx');
SQL

assert_count "Section 1 data visibility" "101" \
    "SELECT count(1) FROM merge_test WHERE id BETWEEN 1000 AND 3000;"

log_success "Section 1 passed cleanly."

# ==============================================================================
# SECTION 2: Page Split on a Merged Page
# ==============================================================================
log_info "======================================================================"
log_info "SECTION 2: Page Split on a Merged Page"
log_info "======================================================================"

$PSQL <<'SQL'
DROP TABLE IF EXISTS merge_test CASCADE;

CREATE TABLE merge_test (id int);
ALTER TABLE merge_test SET (autovacuum_enabled = false);
INSERT INTO merge_test SELECT generate_series(1, 5000);
CREATE INDEX merge_test_idx ON merge_test(id);

DELETE FROM merge_test WHERE id BETWEEN 375 AND 732;
DELETE FROM merge_test WHERE id BETWEEN 755 AND 1088;
VACUUM merge_test;

SELECT * FROM bt_merge('merge_test_idx', 10.0, 90.0, 10);

-- Insert into the merged key range to trigger a page split
INSERT INTO merge_test SELECT generate_series(367, 367 + 500);

-- Validate integrity after split
SELECT * FROM bt_index_parent_check('merge_test_idx');
SELECT * FROM bt_index_check('merge_test_idx');
SQL

log_success "Section 2 passed cleanly."

# ==============================================================================
# SECTION 3: Concurrent Scan Recovery across Merge Group Boundaries
# ==============================================================================
log_info "======================================================================"
log_info "SECTION 3: Scan Recovery Across Merge Group Boundaries"
log_info "======================================================================"

# --- Case 1: Forward Scan Wait → Simple Merge ---------------------------------
log_info "--> Case 1: Forward Scan Wait → Simple Merge"

$PSQL <<'SQL'
DROP TABLE IF EXISTS merge_test CASCADE;
CREATE TABLE merge_test (id int);
ALTER TABLE merge_test SET (autovacuum_enabled = false);
INSERT INTO merge_test SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX merge_test_idx ON merge_test(id);
DELETE FROM merge_test WHERE id % 20 != 0;
VACUUM merge_test;

SELECT injection_points_attach('before_read_next_page', 'wait');
SQL

# Launch Session 1 in background (will pause at block 4)
$PSQL -t -A -c "SELECT count(1) AS fwd_count FROM merge_test WHERE id >= 1 AND id <= 3000;" > /tmp/fwd_case1.out &
SCAN_PID=$!

wait_for_injection_point "before_read_next_page"
log_info "  Session 1 is paused at 'before_read_next_page'. Executing concurrent merge in Session 2..."

# Session 2: Merge, Wakeup, Detach
$PSQL <<'SQL'
SELECT * FROM bt_merge('merge_test_idx', 10.0, 90.0, 10);
SELECT injection_points_wakeup('before_read_next_page');
SELECT injection_points_detach('before_read_next_page');
SQL

wait $SCAN_PID
cat /tmp/fwd_case1.out

$PSQL <<'SQL'
SELECT * FROM bt_index_parent_check('merge_test_idx');
SELECT * FROM bt_index_check('merge_test_idx');
SQL

assert_file_count "Case 1 count" "150" /tmp/fwd_case1.out
log_success "Case 1 (Forward Scan Wait → Simple Merge) completed successfully."

# --- Case 2: Backward Scan Wait → Simple Merge --------------------------------
log_info "--> Case 2: Backward Scan Wait → Simple Merge"

$PSQL <<'SQL'
DROP TABLE IF EXISTS merge_test CASCADE;
CREATE TABLE merge_test (id int);
ALTER TABLE merge_test SET (autovacuum_enabled = false);
INSERT INTO merge_test SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX merge_test_idx ON merge_test(id);
DELETE FROM merge_test WHERE id % 20 != 0;
VACUUM merge_test;

SELECT injection_points_attach('before_read_prev_page', 'wait');
SQL

# Launch Session 1 in background (will pause at block 2)
# Wrap in subquery with ORDER BY id DESC to force Index Only Scan Backward
$PSQL -t -A -c "SELECT count(1) AS bwd_count FROM (SELECT id FROM merge_test WHERE id <= 3000 ORDER BY id DESC) sub;" > /tmp/bwd_case2.out &
SCAN_PID=$!

wait_for_injection_point "before_read_prev_page"
log_info "  Session 1 is paused at 'before_read_prev_page'. Executing concurrent merge in Session 2..."

# Session 2: Merge, Wakeup, Detach
$PSQL <<'SQL'
SELECT * FROM bt_merge('merge_test_idx', 10.0, 90.0, 10);
SELECT injection_points_wakeup('before_read_prev_page');
SELECT injection_points_detach('before_read_prev_page');
SQL

wait $SCAN_PID
cat /tmp/bwd_case2.out

$PSQL <<'SQL'
SELECT * FROM bt_index_parent_check('merge_test_idx');
SELECT * FROM bt_index_check('merge_test_idx');
SQL

assert_file_count "Case 2 count" "150" /tmp/bwd_case2.out
log_success "Case 2 (Backward Scan Wait → Simple Merge) completed successfully."

# --- Case 3: Forward Scan Wait → Merge → Split --------------------------------
log_info "--> Case 3: Forward Scan Wait → Merge → Split"

$PSQL <<'SQL'
DROP TABLE IF EXISTS merge_test CASCADE;
CREATE TABLE merge_test (id int);
ALTER TABLE merge_test SET (autovacuum_enabled = false);
INSERT INTO merge_test SELECT generate_series(1, 5000);
CREATE INDEX merge_test_idx ON merge_test(id);
DELETE FROM merge_test WHERE id BETWEEN 375 AND 732;
DELETE FROM merge_test WHERE id BETWEEN 755 AND 1088;
VACUUM merge_test;

SELECT injection_points_attach('before_read_next_page', 'wait');
SQL

# Launch Session 1 in background
$PSQL -t -A -c "SELECT count(1) AS fwd_split_count FROM merge_test WHERE id >= 1 AND id <= 3000;" > /tmp/fwd_case3.out &
SCAN_PID=$!

wait_for_injection_point "before_read_next_page"
log_info "  Session 1 paused. Session 2 executing merge and triggering page split..."

# Session 2: Merge, Split, Wakeup, Detach
$PSQL <<'SQL'
SELECT * FROM bt_merge('merge_test_idx', 10.0, 90.0, 10);
INSERT INTO merge_test SELECT generate_series(367, 367 + 500);
SELECT injection_points_wakeup('before_read_next_page');
SELECT injection_points_detach('before_read_next_page');
SQL

wait $SCAN_PID
cat /tmp/fwd_case3.out

$PSQL <<'SQL'
SELECT * FROM bt_index_parent_check('merge_test_idx');
SELECT * FROM bt_index_check('merge_test_idx');
SQL

assert_file_count "Case 3 count" "2308" /tmp/fwd_case3.out
log_success "Case 3 (Forward Scan Wait → Merge → Split) completed successfully."

# --- Case 4: Backward Scan Wait → Merge → Split -------------------------------
log_info "--> Case 4: Backward Scan Wait → Merge → Split"

$PSQL <<'SQL'
DROP TABLE IF EXISTS merge_test CASCADE;
CREATE TABLE merge_test (id int);
ALTER TABLE merge_test SET (autovacuum_enabled = false);
INSERT INTO merge_test SELECT generate_series(1, 5000);
CREATE INDEX merge_test_idx ON merge_test(id);
DELETE FROM merge_test WHERE id BETWEEN 375 AND 732;
DELETE FROM merge_test WHERE id BETWEEN 755 AND 1088;
VACUUM merge_test;

SELECT injection_points_attach('before_read_prev_page', 'wait');
SQL

# Launch Session 1 in background
# Wrap in subquery with ORDER BY id DESC to force Index Only Scan Backward
$PSQL -t -A -c "SELECT count(1) AS bwd_split_count FROM (SELECT id FROM merge_test WHERE id <= 3000 ORDER BY id DESC) sub;" > /tmp/bwd_case4.out &
SCAN_PID=$!

wait_for_injection_point "before_read_prev_page"
log_info "  Session 1 paused. Session 2 executing merge and triggering page split..."

# Session 2: Merge, Split, Wakeup, Detach
$PSQL <<'SQL'
SELECT * FROM bt_merge('merge_test_idx', 10.0, 90.0, 10);
INSERT INTO merge_test SELECT generate_series(367, 367 + 500);
SELECT injection_points_wakeup('before_read_prev_page');
SELECT injection_points_detach('before_read_prev_page');
SQL

wait $SCAN_PID
cat /tmp/bwd_case4.out

$PSQL <<'SQL'
SELECT * FROM bt_index_parent_check('merge_test_idx');
SELECT * FROM bt_index_check('merge_test_idx');
SQL

assert_file_count "Case 4 count" "2308" /tmp/bwd_case4.out
log_success "Case 4 (Backward Scan Wait → Merge → Split) completed successfully."

# ==============================================================================
# Final Status
# ==============================================================================
echo ""
echo -e "${GREEN}******************************************************************${NC}"
echo -e "${GREEN}>>> ALL B-TREE MERGE & CONCURRENT SCAN TESTS PASSED CLEANLY! <<<${NC}"
echo -e "${GREEN}******************************************************************${NC}"
