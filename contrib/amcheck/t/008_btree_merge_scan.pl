# Copyright (c) 2024-2026, PostgreSQL Global Development Group
#
# Tests for B-tree leaf page merge scan recovery.
#
# A scan is paused (via an injection point) right before it steps to the
# next leaf page.  While it is paused, another session merges (and
# optionally splits) the pages around that point.  The scan is then resumed
# and must return exactly the rows a heap scan returns for the same
# snapshot: nothing lost, nothing duplicated, correct order.
#
# Layouts used here are deterministic.  In each setup, the first leaf pair
# that gets merged is blocks (2, 4), which is why the injection points in
# _bt_readnextpage() are keyed on block 4 (forward) and block 2 (backward).
#
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init();
$node->start();

if (!$node->check_extension('injection_points'))
{
	$node->stop();
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres',
	'CREATE EXTENSION amcheck; CREATE EXTENSION pageinspect; CREATE EXTENSION injection_points;'
);

#
# Table layouts.  Each one leaves leaf pages 2 and 4 below the merge
# threshold used with it, so that bt_merge() merges them first.
#
my %setup = (
	# 500 distinct keys spread thinly over many leaf pages
	sparse_10k => q{
		DROP TABLE IF EXISTS merge_test CASCADE;
		CREATE TABLE merge_test (id int);
		ALTER TABLE merge_test SET (autovacuum_enabled = false);
		ALTER TABLE merge_test SET (parallel_workers = 2);
		INSERT INTO merge_test SELECT i FROM generate_series(1, 10000) i;
		CREATE INDEX merge_test_idx ON merge_test(id);
		DELETE FROM merge_test WHERE id % 20 != 0;
		VACUUM merge_test;
	},
	# two big holes, so only a few pages are merge candidates
	holes_5k => q{
		DROP TABLE IF EXISTS merge_test CASCADE;
		CREATE TABLE merge_test (id int);
		ALTER TABLE merge_test SET (autovacuum_enabled = false);
		ALTER TABLE merge_test SET (parallel_workers = 2);
		INSERT INTO merge_test SELECT generate_series(1, 5000);
		CREATE INDEX merge_test_idx ON merge_test(id);
		DELETE FROM merge_test WHERE id BETWEEN 375 AND 732;
		DELETE FROM merge_test WHERE id BETWEEN 755 AND 1088;
		VACUUM merge_test;
	},
	# ten heap rows per key, so leaf pages hold posting list tuples; the
	# merged page boundary is then a posting list tuple's max TID
	dups => q{
		DROP TABLE IF EXISTS merge_test CASCADE;
		CREATE TABLE merge_test (id int);
		ALTER TABLE merge_test SET (autovacuum_enabled = false);
		ALTER TABLE merge_test SET (parallel_workers = 2);
		INSERT INTO merge_test SELECT i / 10 FROM generate_series(1, 20000) i;
		CREATE INDEX merge_test_idx ON merge_test(id);
		DELETE FROM merge_test WHERE id % 20 NOT IN (0, 1);
		VACUUM merge_test;
	},
);

my $parallel_prefix = q{
	SET enable_seqscan = false;
	SET enable_bitmapscan = false;
	SET max_parallel_workers_per_gather = 2;
	SET min_parallel_table_scan_size = 0;
	SET min_parallel_index_scan_size = 0;
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
	SET cpu_operator_cost = 0;
};

my $serial_prefix = q{
	SET enable_seqscan = false;
	SET enable_bitmapscan = false;
	SET max_parallel_workers_per_gather = 0;
};

# Force a heap scan, ordered explicitly; used as ground truth.
my $heap_prefix = q{
	SET enable_indexscan = false;
	SET enable_indexonlyscan = false;
	SET enable_bitmapscan = false;
	SET max_parallel_workers_per_gather = 0;
};

sub reset_table
{
	my ($layout) = @_;
	$node->safe_psql('postgres', $setup{$layout});
}

# Ground truth: ids a heap scan returns right now for the given WHERE clause.
sub heap_rows
{
	my ($where, $desc) = @_;
	my $dir = $desc ? 'DESC' : 'ASC';
	my $out = $node->safe_psql('postgres',
		"$heap_prefix SELECT id FROM merge_test WHERE $where ORDER BY id $dir;");
	return [ grep { /^-?\d+$/ } split(/\n/, $out) ];
}

# Ids an index scan returns right now (no concurrency).
sub index_rows
{
	my ($where, $desc) = @_;
	my $dir = $desc ? 'DESC' : 'ASC';
	my $out = $node->safe_psql('postgres',
		"$serial_prefix SELECT id FROM merge_test WHERE $where ORDER BY id $dir;");
	return [ grep { /^-?\d+$/ } split(/\n/, $out) ];
}

sub amcheck_ok
{
	my ($name) = @_;
	my ($ret, $out, $err) = $node->psql('postgres',
		"SELECT bt_index_check('merge_test_idx', true); SELECT bt_index_parent_check('merge_test_idx', true);"
	);
	is($ret, 0, "$name: amcheck passes") or diag($err);
}

# Compare index scans in both directions against the heap.
sub index_matches_heap
{
	my ($name, $where) = @_;
	for my $desc (0, 1)
	{
		is_deeply(
			index_rows($where, $desc),
			heap_rows($where, $desc),
			"$name: index scan (@{[$desc ? 'backward' : 'forward']}) matches heap");
	}
}

# Run $scan_query in a background psql, pause it at $inj_point, run
# $merge_sql (+ optional $post_sql) in another session, resume, and compare
# the scan result to the heap result computed beforehand.
sub run_concurrent_scan
{
	my (%p) = @_;
	my $name       = $p{test_name};
	my $inj_point  = $p{injection_point};
	my $prefix     = $p{prefix};
	my $parallel   = $p{parallel} // 0;
	my $where      = $p{where};
	my $desc       = $p{desc} // 0;
	my $merge_args = $p{merge_args} // '10.0, 90.0, 10';
	my $post_sql   = $p{post_sql} // '';
	my $dir        = $desc ? 'DESC' : 'ASC';
	my $scan_query =
	  "SELECT id FROM merge_test WHERE $where ORDER BY id $dir;";

	# The plan must be what the test intends to exercise.
	my $plan = $node->safe_psql('postgres',
		"$prefix EXPLAIN (COSTS OFF) $scan_query");
	like($plan, qr/Index (Only )?Scan/, "$name: uses an index scan");
	if ($desc)
	{
		like($plan, qr/Index (Only )?Scan Backward/, "$name: scan is backward");
	}
	if ($parallel)
	{
		like($plan, qr/Parallel Index (Only )?Scan/, "$name: scan is parallel");
	}
	else
	{
		unlike($plan, qr/Gather/, "$name: scan is serial");
	}

	# Expected result, as seen by the scan's snapshot.  Anything session 2
	# inserts afterwards is invisible to that snapshot.
	my $expected = heap_rows($where, $desc);
	cmp_ok(scalar(@$expected), '>', 0, "$name: expected result is non-empty");

	$node->safe_psql('postgres',
		"SELECT injection_points_attach('$inj_point', 'wait');");

	my ($stdout, $stderr);
	my $h = IPC::Run::start(
		[
			$node->installed_command('psql'), '-X', '-A', '-t',
			'-p', $node->port, '-d', 'postgres',
			'-c', "$prefix $scan_query"
		],
		'>', \$stdout, '2>', \$stderr);

	$node->poll_query_until('postgres', qq[
		SELECT count(*) > 0 FROM pg_stat_activity
		WHERE wait_event_type = 'InjectionPoint' AND wait_event = '$inj_point';
	]) or die "timed out waiting for injection point $inj_point";

	# Session 2: merge (and optionally more) while the scan is paused.
	my $merges = $node->safe_psql('postgres',
		"SELECT * FROM bt_merge('merge_test_idx', $merge_args);");
	cmp_ok($merges, '>', 0, "$name: merge happened while scan was paused");
	$node->safe_psql('postgres', $post_sql) if $post_sql ne '';

	# Wake up all waiting processes (in case multiple workers hit the injection point)
	$node->safe_psql('postgres', qq[
		DO \$\$
		BEGIN
			FOR i IN 1..10 LOOP
				EXIT WHEN NOT EXISTS (
					SELECT 1 FROM pg_stat_activity
					WHERE wait_event_type = 'InjectionPoint' AND wait_event = '$inj_point'
				);
				BEGIN
					PERFORM injection_points_wakeup('$inj_point');
				EXCEPTION
					WHEN OTHERS THEN EXIT;
				END;
				PERFORM pg_sleep(0.01);
			END LOOP;
		END \$\$;
		SELECT injection_points_detach('$inj_point');
	]);

	$h->finish();
	is($h->result(0), 0, "$name: scan exited cleanly") or diag($stderr);

	my @got = grep { /^-?\d+$/ } split(/\n/, $stdout);
	is(scalar(@got), scalar(@$expected), "$name: row count matches heap");
	is_deeply(\@got, $expected,
		"$name: scan returned exactly the heap rows, in order (no loss, no duplicates)"
	);

	amcheck_ok($name);
}

# ==============================================================================
# SECTION 1: merge without concurrency, then VACUUM
# ==============================================================================
note("SECTION 1: basic merge and VACUUM reclamation");

for my $layout (qw(sparse_10k holes_5k dups))
{
	my $args = $layout eq 'dups' ? '30.0, 90.0, 10' : '10.0, 90.0, 10';
	reset_table($layout);

	my $cand = $node->safe_psql('postgres',
		"SELECT count(*) FROM bt_find_merge_candidates('merge_test_idx', " .
		  ($layout eq 'dups' ? '30.0' : '10.0') . ", 90.0, 10);");
	cmp_ok($cand, '>', 0, "$layout: merge candidates found");

	my $merges = $node->safe_psql('postgres',
		"SELECT * FROM bt_merge('merge_test_idx', $args);");
	cmp_ok($merges, '>', 0, "$layout: bt_merge merged pages");

	amcheck_ok("$layout after merge");
	index_matches_heap("$layout after merge", 'id >= 0');

	# merged-away tombstones must be reclaimable
	$node->safe_psql('postgres', q{
		INSERT INTO merge_test (id) VALUES (-1);
		DELETE FROM merge_test WHERE id = -1;
		VACUUM merge_test;
	});
	amcheck_ok("$layout after VACUUM");
	index_matches_heap("$layout after VACUUM", 'id >= 0');
}

# ==============================================================================
# SECTION 2: inserts into a merged page, including page splits
# ==============================================================================
note("SECTION 2: page split on a merged page");

reset_table('holes_5k');
$node->safe_psql('postgres',
	"SELECT * FROM bt_merge('merge_test_idx', 10.0, 90.0, 10);");
$node->safe_psql('postgres',
	"INSERT INTO merge_test SELECT generate_series(367, 367 + 500);");
amcheck_ok('split on merged page');
index_matches_heap('split on merged page', 'id >= 0');

# ==============================================================================
# SECTION 3/4: concurrent serial and parallel scans
# ==============================================================================
my $split_sql = q{INSERT INTO merge_test SELECT generate_series(367, 367 + 500);};

# Each case: scan paused before stepping to the page next to the merge pair;
# the merge happens; the scan resumes.  The forward hook pauses after the
# scan read the left page of the pair (so it must filter the merged page
# using the boundary stored in the tombstone); the backward hook pauses
# after the scan read the right page.
my @cases;
for my $mode ('serial', 'parallel')
{
	my $prefix = $mode eq 'serial' ? $serial_prefix : $parallel_prefix;
	my $par    = $mode eq 'parallel' ? 1 : 0;

	for my $layout (qw(sparse_10k holes_5k dups))
	{
		my $margs = $layout eq 'dups' ? '30.0, 90.0, 10' : '10.0, 90.0, 10';

		for my $desc (0, 1)
		{
			push @cases, {
				test_name       => ucfirst($mode) . ' ' . ($desc ? 'backward' : 'forward') . " scan, $layout: wait -> merge",
				layout          => $layout,
				injection_point => $desc ? 'before_read_prev_page' : 'before_read_next_page',
				prefix          => $prefix,
				parallel        => $par,
				where           => 'id >= 0',
				desc            => $desc,
				merge_args      => $margs,
			};

			# merge followed by a split of the merged page
			next if $layout ne 'holes_5k';
			push @cases, {
				%{ $cases[-1] },
				test_name => ucfirst($mode) . ' ' . ($desc ? 'backward' : 'forward') . " scan, $layout: wait -> merge -> split",
				post_sql  => $split_sql,
			};
		}
	}
}

# Also: scans bounded on one side, so the scan starts or stops inside the
# merged range.
for my $desc (0, 1)
{
	push @cases, {
		test_name       => 'Serial ' . ($desc ? 'backward' : 'forward') . ' bounded scan: wait -> merge',
		layout          => 'sparse_10k',
		injection_point => $desc ? 'before_read_prev_page' : 'before_read_next_page',
		prefix          => $serial_prefix,
		parallel        => 0,
		where           => 'id >= 1 AND id <= 3000',
		desc            => $desc,
		merge_args      => '10.0, 90.0, 10',
	};
}

for my $c (@cases)
{
	note($c->{test_name});
	reset_table($c->{layout});
	run_concurrent_scan(%$c);
}

$node->stop();
done_testing();
