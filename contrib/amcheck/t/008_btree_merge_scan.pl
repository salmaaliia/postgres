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
# Later sections cover the merge functions on an empty index, cursors that
# change direction, merge joins, parallel scans that start on a merged page,
# merges that race with VACUUM, a crash in the middle of merge group cleanup,
# and merges replayed on a standby.
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
$node->init(allows_streaming => 1, has_archiving => 1);
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start();

if (!$node->check_extension('injection_points'))
{
	$node->stop();
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres',
	'CREATE EXTENSION amcheck; CREATE EXTENSION pageinspect; CREATE EXTENSION injection_points; CREATE EXTENSION pg_walinspect;'
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

#
# Helpers for the sections below.  They take a node, so the same checks
# run on the primary, on a standby and on a recovered copy.
#

# Ids returned by $sql on $n, in order.
sub ids_of
{
	my ($n, $sql) = @_;
	my $out = $n->safe_psql('postgres', $sql);
	return [ grep { /^-?\d+$/ } split(/\n/, $out) ];
}

# Index scans of $col in $table, in both directions, against a heap scan.
sub table_matches_heap
{
	my ($n, $table, $col, $name) = @_;
	for my $dir ('ASC', 'DESC')
	{
		my $q = "SELECT $col FROM $table ORDER BY $col $dir";
		is_deeply(
			ids_of($n, "SET statement_timeout = '60s'; $serial_prefix $q"),
			ids_of($n, "$heap_prefix $q"),
			"$name: index scan ($dir) matches heap");
	}
}

sub index_ok
{
	my ($n, $index, $name) = @_;
	my $sql = "SELECT bt_index_check('$index', true);";

	# bt_index_parent_check needs a ShareLock, which recovery does not allow
	$sql .= " SELECT bt_index_parent_check('$index', true, true);"
	  if $n->safe_psql('postgres', 'SELECT pg_is_in_recovery()') eq 'f';
	my ($ret, $out, $err) = $n->psql('postgres', $sql);
	is($ret, 0, "$name: amcheck passes") or diag($err);
}

# Leaf pages that are merged-away tombstones whose right sibling is not a
# merged page, i.e. a tombstone left behind after its merged pages went away.
sub lone_tombstones
{
	my ($n, $index) = @_;
	return $n->safe_psql('postgres', qq{
		SELECT count(*)
		FROM bt_multi_page_stats('$index', 1, -1) l
		JOIN bt_multi_page_stats('$index', 1, -1) r ON r.blkno = l.btpo_next
		WHERE l.btpo_flags & 1024 <> 0 AND r.btpo_flags & 512 = 0;
	});
}

# Wait until backend $pid either finished its command or waits for a cleanup
# lock.
sub wait_idle_or_cleanup
{
	my ($pid) = @_;
	$node->poll_query_until('postgres', qq{
		SELECT state = 'idle' OR wait_event = 'BufferCleanup'
		FROM pg_stat_activity WHERE pid = $pid;
	}) or die "timed out waiting for backend $pid";
}

# ==============================================================================
# SECTION 5: merge functions on an empty index
# ==============================================================================
note("SECTION 5: merge functions on an empty index");

$node->safe_psql('postgres', q{
	CREATE TABLE merge_empty (id int);
	CREATE INDEX merge_empty_idx ON merge_empty (id);
});
is($node->safe_psql('postgres', "SELECT bt_merge('merge_empty_idx')"),
	'0', 'empty index: bt_merge merges nothing');
is( $node->safe_psql('postgres',
		"SELECT count(*) FROM bt_find_merge_candidates('merge_empty_idx')"),
	'0',
	'empty index: no merge candidates');

# ==============================================================================
# SECTION 6: a scrollable cursor changes direction after a merge
# ==============================================================================
#
# The cursor stops at different positions, the pages around it are merged,
# and it then moves backward and forward several times.  Each FETCH must
# return the rows next to the cursor in the heap order, whether the cursor
# sat on the left or the right page of the merge.
#
note("SECTION 6: scrollable cursor changes direction after a merge");

# Rows a FETCH sequence returns from @rows, given as [count, direction].
sub simulate_fetches
{
	my ($rows, $first, @steps) = @_;
	my $cur = $first - 1;
	my @out;
	for my $s (@steps)
	{
		my ($cnt, $dir) = @$s;
		my @got;
		for (1 .. $cnt)
		{
			$cur += $dir eq 'forward' ? 1 : -1;
			last if $cur < 0 || $cur > $#$rows;
			push @got, $rows->[$cur];
		}
		push @out, \@got;
	}
	return @out;
}

for my $dir ('ASC', 'DESC')
{
	for my $first (7, 15, 23, 31, 39)
	{
		my $name = "cursor $dir, merge after $first rows";
		$node->safe_psql('postgres', q{
			DROP TABLE IF EXISTS merge_cursor;
			CREATE TABLE merge_cursor (k int) WITH (autovacuum_enabled = off);
			INSERT INTO merge_cursor SELECT g FROM generate_series(1, 4000) g;
			CREATE INDEX merge_cursor_idx ON merge_cursor (k);
			DELETE FROM merge_cursor WHERE k % 50 <> 0;
			VACUUM (INDEX_CLEANUP ON) merge_cursor;
		});
		my $rows = ids_of($node,
			"$heap_prefix SELECT k FROM merge_cursor ORDER BY k $dir");
		my @steps = ([ 1, 'backward' ], [ 5, 'forward' ],
			[ 3, 'backward' ], [ 1000, 'forward' ]);
		my @expected = simulate_fetches($rows, $first, @steps);

		my $s = $node->background_psql('postgres');
		$s->query_safe(qq{
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SET LOCAL enable_seqscan = off;
			SET LOCAL enable_bitmapscan = off;
			SET LOCAL enable_indexonlyscan = off;
			SET LOCAL statement_timeout = '60s';
			DECLARE c SCROLL CURSOR FOR SELECT k FROM merge_cursor ORDER BY k $dir;
		});
		is($s->query_safe("FETCH $first FROM c"),
			join("\n", @$rows[ 0 .. $first - 1 ]),
			"$name: first FETCH");
		cmp_ok(
			$s->query_safe(
				"SELECT bt_merge('merge_cursor_idx', 10, 90, 1000)"),
			'>', 0, "$name: merge happened");
		for my $i (0 .. $#steps)
		{
			my ($cnt, $way) = @{ $steps[$i] };
			is($s->query_safe("FETCH $way $cnt FROM c"),
				join("\n", @{ $expected[$i] }),
				"$name: FETCH $way $cnt");
		}
		$s->query_safe('COMMIT');
		$s->quit;
		index_ok($node, 'merge_cursor_idx', $name);
	}
}

# ==============================================================================
# SECTION 7: merge join restores a mark after a merge
# ==============================================================================
#
# Duplicate outer keys make the merge join mark the inner index scan and
# restore it for the second outer row.  The rows that moved from the left
# page into the right page must be returned once per outer row.
#
note("SECTION 7: merge join mark and restore after a merge");

$node->safe_psql('postgres', q{
	CREATE TABLE merge_mj (k int, id int) WITH (autovacuum_enabled = off);
	INSERT INTO merge_mj SELECT g / 400, g FROM generate_series(1, 40000) g;
	CREATE INDEX merge_mj_idx ON merge_mj (k) WITH (deduplicate_items = off);
	DELETE FROM merge_mj WHERE id % 40 <> 0;
	VACUUM (INDEX_CLEANUP ON) merge_mj;
	CREATE TABLE merge_mj_outer (k int);
	INSERT INTO merge_mj_outer VALUES (1), (1);
});

{
	my $join_sql = q{SELECT m.id FROM merge_mj_outer o JOIN merge_mj m ON m.k = o.k
		WHERE m.k BETWEEN 1 AND 1 AND o.k BETWEEN 1 AND 1 ORDER BY o.k};
	my $expected = ids_of($node, qq{
		SET enable_indexscan = off; SET enable_indexonlyscan = off;
		SET enable_bitmapscan = off; SET enable_mergejoin = off;
		SET enable_nestloop = off;
		SELECT id FROM ($join_sql) x ORDER BY id;
	});

	my $s = $node->background_psql('postgres');
	$s->query_safe(qq{
		BEGIN;
		SET LOCAL enable_hashjoin = off; SET LOCAL enable_nestloop = off;
		SET LOCAL enable_material = off; SET LOCAL enable_seqscan = off;
		SET LOCAL enable_bitmapscan = off; SET LOCAL enable_indexonlyscan = off;
		SET LOCAL statement_timeout = '60s';
		DECLARE c CURSOR FOR $join_sql;
	});
	# The index scan must be the inner side, which the join marks and restores.
	like($s->query_safe("EXPLAIN (COSTS OFF) DECLARE x CURSOR FOR $join_sql"),
		qr/Merge Join.*->\s+Sort.*->\s+Index Scan using merge_mj_idx/s,
		'merge join: the index scan is the inner side');
	my @first = split /\n/, $s->query_safe('FETCH 9 FROM c');
	cmp_ok($s->query_safe("SELECT bt_merge('merge_mj_idx', 10, 90, 2)"),
		'>', 0, 'merge join: merge happened');
	my @rest = split /\n/, $s->query_safe('FETCH ALL FROM c');
	$s->query_safe('COMMIT');
	$s->quit;
	# the join is ordered by the outer key only, so compare sorted ids
	is_deeply([ sort { $a <=> $b } @first, @rest ], $expected,
		'merge join: rows match a hash join');
	index_ok($node, 'merge_mj_idx', 'merge join');
}

# ==============================================================================
# SECTION 8: parallel scans that start on a merged page
# ==============================================================================
#
# No concurrency: the merge happens first.  The parallel scan's first page is
# the merged page, so the scan must remember that it already covers the
# tombstone's rows.  The second case splits the merged page inside the old
# left key range and runs with the leader alone.
#
note("SECTION 8: parallel scan starting on a merged page");

$node->safe_psql('postgres', q{
	CREATE TABLE merge_par (id int) WITH (autovacuum_enabled = off, parallel_workers = 2);
	INSERT INTO merge_par SELECT generate_series(1, 20000);
	CREATE INDEX merge_par_idx ON merge_par (id);
	DELETE FROM merge_par WHERE id BETWEEN 4759 AND 5490 AND id % 20 <> 0
		AND id NOT IN (4759, 5124, 5125, 5490);
	VACUUM (FREEZE) merge_par;
});
cmp_ok($node->safe_psql('postgres', "SELECT bt_merge('merge_par_idx', 10, 90, 1000)"),
	'>', 0, 'parallel: merge happened');

{
	my $q = 'SELECT id FROM merge_par WHERE id <= 5124 ORDER BY id DESC';
	like($node->safe_psql('postgres', "$parallel_prefix EXPLAIN (COSTS OFF) $q"),
		qr/Parallel Index (Only )?Scan Backward/, 'parallel backward: plan is a parallel backward scan');
	is_deeply(
		ids_of($node, "$parallel_prefix SET statement_timeout = '60s'; $q"),
		ids_of($node, "$heap_prefix $q"),
		'parallel backward scan from the merged page matches heap');
}

$node->safe_psql('postgres', q{
	INSERT INTO merge_par SELECT g FROM generate_series(4759, 5275) g
		WHERE NOT EXISTS (SELECT 1 FROM merge_par WHERE id = g);
	VACUUM (FREEZE) merge_par;
});
{
	my $q = 'SELECT id FROM merge_par WHERE id >= 4759 ORDER BY id';
	my $leader_only = "$parallel_prefix SET max_parallel_workers = 0;";
	like($node->safe_psql('postgres', "$leader_only EXPLAIN (COSTS OFF) $q"),
		qr/Parallel Index (Only )?Scan/, 'parallel forward: plan is a parallel scan');
	is_deeply(
		ids_of($node, "$leader_only SET statement_timeout = '60s'; $q"),
		ids_of($node, "$heap_prefix $q"),
		'parallel forward scan after the merged page split matches heap');
	my $q2 = 'SELECT id FROM merge_par WHERE id <= 5124 ORDER BY id DESC';
	is_deeply(
		ids_of($node, "$parallel_prefix SET statement_timeout = '60s'; $q2"),
		ids_of($node, "$heap_prefix $q2"),
		'parallel backward scan after the merged page split matches heap');
}
index_ok($node, 'merge_par_idx', 'parallel');

# ==============================================================================
# SECTION 9: merges that race with VACUUM
# ==============================================================================
note("SECTION 9: merge while VACUUM runs");

# Leaf pages 6589..6954 and 6955..7320 of this table are thinned out, so they
# are the first merge pair.
my $vac_setup = q{
	DROP TABLE IF EXISTS merge_vac;
	CREATE TABLE merge_vac (id int) WITH (autovacuum_enabled = off);
	INSERT INTO merge_vac SELECT generate_series(1, 20000);
	CREATE INDEX merge_vac_idx ON merge_vac (id);
	VACUUM ANALYZE merge_vac;
	DELETE FROM merge_vac WHERE id BETWEEN 6589 AND 7320 AND id % 40 <> 0;
	VACUUM merge_vac;
};
my $ios_cursor = q{
	SET enable_seqscan = off; SET enable_bitmapscan = off;
	BEGIN ISOLATION LEVEL REPEATABLE READ;
	DECLARE c CURSOR FOR SELECT id FROM merge_vac WHERE id >= 6589 ORDER BY id;
	FETCH 1 FROM c;
};

# 9a: VACUUM waits for the cleanup lock on the left page while a merge runs.
# Whether the merge happens or backs off, VACUUM must not read the page as
# a normal leaf afterwards.
{
	my $name = 'VACUUM waiting on the left page';
	$node->safe_psql('postgres', $vac_setup . 'DELETE FROM merge_vac WHERE id = 15000;');

	my $cur = $node->background_psql('postgres');
	$cur->query_safe($ios_cursor);
	my $vac = $node->background_psql('postgres');
	my $vacpid = $vac->query_safe('SELECT pg_backend_pid()');
	$vac->query_until(qr/vacuum_started/,
		"\\echo vacuum_started\nVACUUM (INDEX_CLEANUP ON) merge_vac;\n");
	$node->poll_query_until('postgres',
		"SELECT wait_event = 'BufferCleanup' FROM pg_stat_activity WHERE pid = $vacpid")
	  or die "VACUUM did not wait for a cleanup lock";

	$node->safe_psql('postgres', "SELECT bt_merge('merge_vac_idx', 10, 90, 100)");
	$cur->query_safe('COMMIT');
	$cur->quit;
	my (undef, $ret) = $vac->query('SELECT 1');
	is($ret, 0, "$name: VACUUM finished without error");
	$vac->quit;

	is( $node->safe_psql('postgres',
			"SELECT reltuples::int = (SELECT count(*) FROM merge_vac) FROM pg_class WHERE relname = 'merge_vac_idx'"),
		't',
		"$name: index reltuples matches the heap");
	index_ok($node, 'merge_vac_idx', $name);
	table_matches_heap($node, 'merge_vac', 'id', $name);
}

# 9b: an index-only scan cursor still has the left page's items when the
# page is merged and VACUUM runs.  Rows deleted before the cursor's snapshot
# must not come back.
{
	my $name = 'index-only cursor across merge and VACUUM';
	$node->safe_psql('postgres',
		$vac_setup . 'DELETE FROM merge_vac WHERE id IN (6800, 6840, 6880, 6920);');
	my $expected = $node->safe_psql('postgres',
		"$heap_prefix SELECT id FROM merge_vac WHERE id > 6600 ORDER BY id LIMIT 12");

	my $cur = $node->background_psql('postgres');
	is($cur->query_safe($ios_cursor), '6600', "$name: cursor starts at 6600");
	$node->safe_psql('postgres', "SELECT bt_merge('merge_vac_idx', 10, 90, 100)");
	my $vac = $node->background_psql('postgres');
	my $vacpid = $vac->query_safe('SELECT pg_backend_pid()');
	$vac->query_until(qr/vacuum_started/, "\\echo vacuum_started\nVACUUM merge_vac;\n");
	wait_idle_or_cleanup($vacpid);

	is($cur->query_safe('FETCH 12 FROM c'), $expected,
		"$name: no deleted rows returned");
	$cur->query_safe('COMMIT');
	$cur->quit;
	my (undef, $ret) = $vac->query('SELECT 1');
	is($ret, 0, "$name: VACUUM finished without error");
	$vac->quit;
	index_ok($node, 'merge_vac_idx', $name);
}

# 9c: VACUUM has already passed the right page when the left page, at a
# higher block, is merged into it.  VACUUM must still remove the dead
# entries that moved, or they point at reused heap slots later.
{
	my $name = 'merge into a page VACUUM already passed';
	my $deleted = '13640, 13720, 13800, 13880';
	$node->safe_psql('postgres', qq{
		DROP TABLE IF EXISTS merge_vac;
		CREATE TABLE merge_vac (id int) WITH (autovacuum_enabled = off);
		INSERT INTO merge_vac SELECT 2 * g FROM generate_series(1, 20000) g;
		CREATE INDEX merge_vac_idx ON merge_vac (id);
		VACUUM ANALYZE merge_vac;
		-- split one leaf, so its right half lands on a high block number
		INSERT INTO merge_vac SELECT generate_series(13789, 13907, 2);
		DELETE FROM merge_vac WHERE id BETWEEN 13584 AND 14640 AND id % 40 <> 0;
		VACUUM merge_vac;
		DELETE FROM merge_vac WHERE id IN ($deleted);
	});

	# pin a leaf between the two merge pages, so VACUUM stops there
	my $cur = $node->background_psql('postgres');
	$cur->query_safe(q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		BEGIN;
		DECLARE c CURSOR FOR SELECT id FROM merge_vac WHERE id >= 27818 ORDER BY id;
		FETCH 1 FROM c;
	});
	my $vac = $node->background_psql('postgres');
	my $vacpid = $vac->query_safe('SELECT pg_backend_pid()');
	$vac->query_until(qr/vacuum_started/, "\\echo vacuum_started\nVACUUM merge_vac;\n");
	$node->poll_query_until('postgres',
		"SELECT wait_event = 'BufferCleanup' FROM pg_stat_activity WHERE pid = $vacpid")
	  or die "VACUUM did not wait for a cleanup lock";

	cmp_ok($node->safe_psql('postgres', "SELECT bt_merge('merge_vac_idx', 10, 90, 1000)"),
		'>', 0, "$name: merge happened");
	$cur->query_safe('COMMIT');
	$cur->quit;
	my (undef, $ret) = $vac->query('SELECT 1');
	is($ret, 0, "$name: VACUUM finished without error");
	$vac->quit;

	# reuse the freed heap slots
	$node->safe_psql('postgres',
		'INSERT INTO merge_vac SELECT 1000000 + g FROM generate_series(1, 3000) g');
	is( $node->safe_psql('postgres', qq{
			SET enable_seqscan = off; SET enable_bitmapscan = off;
			SET enable_indexonlyscan = off;
			SELECT count(*) FROM merge_vac WHERE id IN ($deleted);
		}),
		'0',
		"$name: index scan finds no deleted rows after heap slot reuse");
	index_ok($node, 'merge_vac_idx', $name);
	table_matches_heap($node, 'merge_vac', 'id', $name);
}

# 9d: VACUUM deletes an emptied merged page while an old snapshot keeps its
# tombstone alive.  Scans must step over the tombstone that has no merged
# page, before and after a second merge right next to it.
{
	my $name = 'tombstone whose merged page was deleted';
	$node->safe_psql('postgres', q{
		CREATE TABLE merge_del (id int) WITH (autovacuum_enabled = off);
		INSERT INTO merge_del SELECT generate_series(1, 20000);
		CREATE INDEX merge_del_idx ON merge_del (id);
		DELETE FROM merge_del WHERE id BETWEEN 4759 AND 6222 AND id % 20 <> 0
			AND id NOT IN (4759, 5124, 5125, 5490, 5491, 5856, 5857, 6222);
		VACUUM (FREEZE) merge_del;
		DELETE FROM merge_del WHERE id BETWEEN 4759 AND 5490;
	});
	cmp_ok($node->safe_psql('postgres', "SELECT bt_merge('merge_del_idx', 10, 90, 14)"),
		'>', 0, "$name: merge happened");

	my $snap = $node->background_psql('postgres');
	$snap->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1;');
	$node->safe_psql('postgres', 'VACUUM merge_del');
	cmp_ok(lone_tombstones($node, 'merge_del_idx'), '>', 0,
		"$name: a tombstone without merged pages exists");
	index_ok($node, 'merge_del_idx', $name);
	table_matches_heap($node, 'merge_del', 'id', $name);

	$node->safe_psql('postgres', "SELECT bt_merge('merge_del_idx', 10, 90, 1000)");
	index_ok($node, 'merge_del_idx', "$name, second merge");
	table_matches_heap($node, 'merge_del', 'id', "$name, second merge");
	$snap->query_safe('COMMIT');
	$snap->quit;
}

# ==============================================================================
# SECTION 10: crash between the two steps of merge group cleanup
# ==============================================================================
#
# VACUUM retires a merge group with two WAL records: one clears the merged
# flag on the right page, one turns the tombstone into a half-dead page.
# Recover a copy of the cluster to just before the second record, which is
# what a crash between them leaves on disk, and scan it.  Then merge the
# former merged page into its right sibling, which puts two tombstones next
# to each other, and scan again.
#
note("SECTION 10: crash between the two cleanup records");

$node->safe_psql('postgres', q{
	CREATE TABLE merge_pitr (k int) WITH (autovacuum_enabled = off);
	INSERT INTO merge_pitr SELECT g FROM generate_series(1, 4000) g;
	CREATE INDEX merge_pitr_idx ON merge_pitr (k);
	DELETE FROM merge_pitr WHERE k % 50 <> 0;
	VACUUM (INDEX_CLEANUP ON) merge_pitr;
});
cmp_ok($node->safe_psql('postgres', "SELECT bt_merge('merge_pitr_idx', 10, 90, 2)"),
	'>', 0, 'cleanup crash: merge happened');
$node->safe_psql('postgres', 'CHECKPOINT');
$node->backup('cleanup_crash');
my $start_lsn = $node->lsn('insert');

# a dead tuple, so VACUUM scans the index and retires the merge group
$node->safe_psql('postgres', q{
	DELETE FROM merge_pitr WHERE k = 4000;
	VACUUM (INDEX_CLEANUP ON) merge_pitr;
});
my $hd_lsn = $node->safe_psql('postgres', qq{
	SELECT start_lsn FROM pg_get_wal_records_info('$start_lsn', pg_current_wal_insert_lsn())
	WHERE resource_manager = 'Btree2' AND record_type = 'MARK_MA_AS_HD'
	ORDER BY start_lsn LIMIT 1;
});
isnt($hd_lsn, '', 'cleanup crash: VACUUM turned the tombstone into a half-dead page');
# Recovery must read the whole record at $hd_lsn, which can cross into the
# next segment, so wait for the segment that pg_switch_wal() closes.
my $last_wal = $node->safe_psql('postgres',
	'SELECT pg_walfile_name(pg_switch_wal())');
$node->poll_query_until('postgres',
	"SELECT last_archived_wal >= '$last_wal' FROM pg_stat_archiver")
  or die "timed out waiting for WAL archiving";

my $crashed = PostgreSQL::Test::Cluster->new('cleanup_crash');
$crashed->init_from_backup($node, 'cleanup_crash',
	has_restoring => 1, standby => 0);
$crashed->append_conf('postgresql.conf', qq{
recovery_target_lsn = '$hd_lsn'
recovery_target_inclusive = off
recovery_target_action = 'promote'
archive_mode = off
});
$crashed->start;
$crashed->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()')
  or die "timed out waiting for promotion";

cmp_ok(lone_tombstones($crashed, 'merge_pitr_idx'), '>', 0,
	'cleanup crash: tombstone next to a plain page after recovery');
index_ok($crashed, 'merge_pitr_idx', 'cleanup crash');
table_matches_heap($crashed, 'merge_pitr', 'k', 'cleanup crash');

$crashed->safe_psql('postgres', "SELECT bt_merge('merge_pitr_idx', 10, 90, 2)");
index_ok($crashed, 'merge_pitr_idx', 'cleanup crash, then merge next to the tombstone');
table_matches_heap($crashed, 'merge_pitr', 'k',
	'cleanup crash, then merge next to the tombstone');

# the next VACUUM finishes the cleanup
$crashed->safe_psql('postgres', q{
	DELETE FROM merge_pitr WHERE k = 3950;
	VACUUM (INDEX_CLEANUP ON) merge_pitr;
});
index_ok($crashed, 'merge_pitr_idx', 'cleanup crash, after VACUUM');
table_matches_heap($crashed, 'merge_pitr', 'k', 'cleanup crash, after VACUUM');
$crashed->stop;

# ==============================================================================
# SECTION 11: standby
# ==============================================================================
#
# bt_merge() must refuse to run during recovery.  VACUUM deletes an emptied
# merged page while its tombstone is still live; the standby and crash
# recovery must rebuild that page exactly as the primary has it, which
# wal_consistency_checking verifies on every replayed record.
#
note("SECTION 11: standby");

$node->safe_psql('postgres', q{
	ALTER SYSTEM SET wal_consistency_checking = 'btree,Btree2';
	SELECT pg_reload_conf();
});
$node->backup('for_standby');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($node, 'for_standby', has_streaming => 1);
$standby->start;

$node->safe_psql('postgres', q{
	CREATE TABLE merge_redo (id int) WITH (autovacuum_enabled = off);
	INSERT INTO merge_redo SELECT generate_series(1, 20000);
	CREATE INDEX merge_redo_idx ON merge_redo (id);
	DELETE FROM merge_redo WHERE id % 20 <> 0;
	VACUUM merge_redo;
	DELETE FROM merge_redo WHERE id <= 3000;
});
cmp_ok($node->safe_psql('postgres', "SELECT bt_merge('merge_redo_idx', 10, 90, 3)"),
	'>', 0, 'standby: merge happened');
# no XID is assigned after the merge, so its tombstones stay live
$node->safe_psql('postgres', 'VACUUM merge_redo');
$node->wait_for_replay_catchup($standby);

# Flags and the tombstone link stored in pd_prune_xid of every merged,
# merged-away or deleted page.
my $pages_sql = q{
	SELECT string_agg(s.blkno || ':' || s.btpo_flags || ':' || h.prune_xid, ' ' ORDER BY s.blkno)
	FROM generate_series(1, pg_relation_size('merge_redo_idx') / 8192 - 1) b,
		LATERAL bt_page_stats('merge_redo_idx', b::int) s,
		LATERAL page_header(get_raw_page('merge_redo_idx', b::int)) h
	WHERE s.btpo_flags & (512 | 1024) <> 0 OR s.type = 'd';
};
my $primary_pages = $node->safe_psql('postgres', $pages_sql);
is( $node->safe_psql('postgres', q{
		SELECT count(*) > 0 FROM bt_multi_page_stats('merge_redo_idx', 1, -1)
		WHERE btpo_flags & 512 <> 0 AND type = 'd';
	}),
	't',
	'standby: VACUUM deleted a merged page');
is($standby->safe_psql('postgres', $pages_sql),
	$primary_pages, 'standby: merged and deleted pages match the primary');
index_ok($standby, 'merge_redo_idx', 'standby');
table_matches_heap($standby, 'merge_redo', 'id', 'standby');

my ($ret, $out, $err) = $standby->psql('postgres',
	"SELECT bt_merge('merge_redo_idx', 10, 90, 3)");
isnt($ret, 0, 'standby: bt_merge fails during recovery');
like($err, qr/recovery is in progress/, 'standby: bt_merge reports recovery');
is($standby->safe_psql('postgres', 'SELECT 1'), '1',
	'standby: still accepts queries after bt_merge');
$standby->stop;

# crash recovery replays the same records on the primary
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', $pages_sql),
	$primary_pages, 'crash recovery: merged and deleted pages unchanged');
index_ok($node, 'merge_redo_idx', 'crash recovery');

$node->stop();
done_testing();
