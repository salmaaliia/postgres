
# Copyright (c) 2024-2026, PostgreSQL Global Development Group

#
# Tests for B-tree page merge WAL logging.
#
# Tests that a merged destination page (BTP_MERGED) that is subsequently
# deduplicated and split correctly retains the merged-away block number
# through each WAL redo path, and that VACUUM-driven merge cleanup properly
# resolves recovery conflicts on a Hot Standby.
#
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Build a table with enough sparse pages for bt_merge() to find candidates.
# Autovacuum is disabled so VACUUM runs only when the test asks for it.
my $setup_sql = q{
	CREATE TABLE merge_test (id int);
	ALTER TABLE merge_test SET (autovacuum_enabled = false);
	INSERT INTO merge_test SELECT i FROM generate_series(1, 10000) i;
	CREATE INDEX merge_test_idx ON merge_test(id) WITH (deduplicate_items = on);
	DELETE FROM merge_test WHERE id % 20 != 0;
	VACUUM merge_test;
};

my $check_sql = q{SELECT bt_index_check('merge_test_idx', true)};

# Count leaf pages that carry BTP_MERGED (flag bit 512).
my $merged_page_count_sql = q{
	SELECT count(*)
	FROM generate_series(
		1,
		pg_relation_size('merge_test_idx') /
			current_setting('block_size')::int - 1) AS b(blkno)
	CROSS JOIN LATERAL bt_page_stats('merge_test_idx', b.blkno) AS stats
	WHERE stats.type = 'l' AND (stats.btpo_flags & 512) <> 0
};

#
# Run a merge followed by a deduplication pass and a page split, bracketing
# each workload with LSN snapshots so the caller can inspect WAL ranges.
# Also asserts that the split caused a new BTP_MERGED page (proving the split
# redo path propagates the flag to the right half).
#
sub merge_and_stress
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $name) = @_;

	my $merges = $node->safe_psql('postgres',
		q{SELECT merges_performed
		  FROM bt_merge('merge_test_idx', 10.0, 90.0, 10)});
	isnt($merges, '0', "$name: bt_merge() performed at least one merge");

	my $merged_before = $node->safe_psql('postgres', $merged_page_count_sql);

	my $dedup_start = $node->safe_psql('postgres',
		'SELECT pg_current_wal_lsn()');

	# Insert many copies of a key that lands on a merged page to force a
	# DEDUP record on that page.
	$node->safe_psql('postgres',
		q{INSERT INTO merge_test SELECT 20 FROM generate_series(1, 500)});

	my $dedup_end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	my $split_start = $node->safe_psql('postgres',
		'SELECT pg_current_wal_lsn()');

	# Re-insert the full key range to fill every merged page and force a split.
	$node->safe_psql('postgres',
		q{INSERT INTO merge_test SELECT i FROM generate_series(1, 10000) i});

	my $split_end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	my $merged_after = $node->safe_psql('postgres', $merged_page_count_sql);
	cmp_ok($merged_after, '>', $merged_before,
		"$name: split produced a new BTP_MERGED page");

	return ($dedup_start, $dedup_end, $split_start, $split_end);
}

sub waldump_range
{
	my ($node, $start_lsn, $end_lsn) = @_;
	my $waldir = $node->data_dir . '/pg_wal';

	return qx{pg_waldump -p $waldir -s $start_lsn -e $end_lsn 2>/dev/null};
}

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);
$node->start;

# Install extensions once; $setup_sql is called repeatedly and must not
# repeat CREATE EXTENSION.
$node->safe_psql('postgres',
	'CREATE EXTENSION amcheck; CREATE EXTENSION pageinspect');

$node->safe_psql('postgres', $setup_sql);


#
# Verify that bt_merge(), the subsequent dedup pass, and the forced page split
# each emit the expected WAL record types.
#

my ($dedup_start, $dedup_end, $split_start, $split_end) =
  merge_and_stress($node, 'WAL generation');
$node->safe_psql('postgres', 'CHECKPOINT');

like(
	waldump_range($node, $dedup_start, $dedup_end),
	qr/desc: DEDUP/,
	'pg_waldump shows a DEDUP record after inserting into a merged page');

like(
	waldump_range($node, $split_start, $split_end),
	qr/desc: SPLIT_[LR]/,
	'pg_waldump shows a SPLIT record after filling merged pages');

$node->safe_psql('postgres', $check_sql);

#
# With wal_consistency_checking = 'btree' and full_page_writes = off, the
# server re-applies every btree WAL record to a scratch copy of the page and
# compares the result byte-for-byte.  Any difference panics the server.
# Surviving the workload proves the DEDUP and SPLIT redo paths preserve the
# MA block number stored in pd_prune_xid.
#

$node->append_conf('postgresql.conf',
	"wal_consistency_checking = 'btree'\nfull_page_writes = off");
$node->restart;

$node->safe_psql('postgres', 'DROP TABLE merge_test CASCADE');
$node->safe_psql('postgres', $setup_sql);
merge_and_stress($node, 'WAL consistency checking');

# Server is still up — redo matched the write side for every record.
$node->safe_psql('postgres', $check_sql);

#
# stop('immediate') is equivalent to SIGKILL: dirty pages are lost.  Recovery
# must redo the MERGE, DEDUP, and SPLIT records to reconstruct the index.
#

$node->safe_psql('postgres', 'DROP TABLE merge_test CASCADE');
$node->safe_psql('postgres', $setup_sql);
merge_and_stress($node, 'crash recovery');

$node->stop('immediate');
$node->start;

$node->safe_psql('postgres', $check_sql);
pass('bt_index_check passes after crash recovery with merged pages');

#
# Streaming replica: with full_page_writes still off, the standby must
# reconstruct every modified page from deltas alone.  We also verify that
# VACUUM-driven merge cleanup triggers a snapshot recovery conflict for a
# standby transaction whose snapshot predates the merge's safemergexid.
#

$node->safe_psql('postgres', 'DROP TABLE merge_test CASCADE');
$node->safe_psql('postgres', $setup_sql);

$node->backup('merge_backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'merge_backup', has_streaming => 1);
$standby->append_conf('postgresql.conf',
	"max_standby_streaming_delay = '50ms'");
$standby->start;

# Open a repeatable-read transaction on the standby before the merge runs so
# its snapshot predates the merge's safemergexid.  A plain SELECT avoids
# leaving a buffer pin, which would produce a pin conflict rather than the
# snapshot conflict we intend to test.
my $standby_psql =
  $standby->background_psql('postgres', on_error_stop => 0);
my $result = $standby_psql->query_safe(q{
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	SELECT 1;
});
like($result, qr/^1$/m, 'standby snapshot established before merge');

merge_and_stress($node, 'streaming replication');
$node->wait_for_replay_catchup($standby);

# bt_index_check on the standby exercises the delta redo paths for MERGE,
# DEDUP, and SPLIT — the paths most likely to corrupt the MA block number.
$standby->safe_psql('postgres', $check_sql);
pass(
	'bt_index_check passes on standby after replaying merge WAL (full_page_writes off)'
);

# VACUUM clears the BTP_MERGED flags and writes a CLEAR_MERGE_FLAG record
# carrying safemergexid.  On replay, ResolveRecoveryConflictWithSnapshotFullXid
# cancels the standby transaction opened above.
my $log_offset = -s $standby->logfile;
$node->safe_psql('postgres', 'VACUUM merge_test');
$node->wait_for_replay_catchup($standby);

my $new_offset = $standby->wait_for_log(
	qr/User query might have needed to see row versions that must be removed/,
	$log_offset);
cmp_ok($new_offset, '>', $log_offset,
	'merge cleanup WAL canceled the old standby snapshot');

is($standby->safe_psql('postgres', q{
	SELECT confl_snapshot
	FROM pg_stat_database_conflicts
	WHERE datname = current_database()
}), '1', 'pg_stat_database_conflicts records one snapshot conflict');

$standby_psql->quit;
$standby->safe_psql('postgres', $check_sql);
pass('bt_index_check passes on standby after merge cleanup replay');

$standby->stop;
$node->stop;

done_testing();
