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
# that gets merged is blocks (2, 4).  The injection point in
# _bt_readnextpage() is attached for merge_test_idx only, and stops a scan
# at its first step to another page, so each case chooses where the scan
# stops by choosing the page it starts on.
#
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

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

# Replace {L_MIN}, {R_MAX} and so on in $where with the smallest or largest
# key on a page of the merge pair: L is block 2, R its right sibling, and R2
# the page right of R.  A scan bounded by one of these keys starts on that
# page.
sub fill_bounds
{
	my ($where) = @_;
	return $where unless $where =~ /\{/;

	my %blk = (L => 2);
	$blk{R} = $node->safe_psql('postgres',
		"SELECT btpo_next FROM bt_page_stats('merge_test_idx', $blk{L});");
	$blk{R2} = $node->safe_psql('postgres',
		"SELECT btpo_next FROM bt_page_stats('merge_test_idx', $blk{R});");

	for my $page (keys %blk)
	{
		# Item 1 is the high key on every page except the rightmost.
		my ($min, $max) = split(/\|/, $node->safe_psql('postgres', qq{
			SELECT min(m.id), max(m.id)
			FROM bt_page_stats('merge_test_idx', $blk{$page}) s,
				bt_page_items('merge_test_idx', $blk{$page}) i
				JOIN merge_test m ON m.ctid = i.htid
			WHERE i.itemoffset > CASE WHEN s.btpo_next <> 0 THEN 1 ELSE 0 END;
		}));
		$where =~ s/\{${page}_MIN\}/$min/g;
		$where =~ s/\{${page}_MAX\}/$max/g;
	}
	return $where;
}

# Parallel workers launched so far in this database.  A query's leader
# reports its workers when the query ends, and its stats are flushed when the
# session exits.
sub workers_launched
{
	return $node->safe_psql('postgres',
		"SELECT parallel_workers_launched FROM pg_stat_database WHERE datname = 'postgres';"
	);
}

# Wait until more parallel workers than $before have been launched.
sub workers_ran
{
	my ($before, $name) = @_;
	ok( $node->poll_query_until('postgres', qq[
			SELECT parallel_workers_launched > $before
			FROM pg_stat_database WHERE datname = 'postgres';
		]),
		"$name: parallel workers ran");
}

# Run $scan_query in a background psql, pause it at its first step to
# another page, run bt_merge() (+ optional $post_sql) in another session,
# resume, and compare the scan result to the heap result computed beforehand.
sub run_concurrent_scan
{
	my (%p) = @_;
	my $name       = $p{test_name};
	my $inj_point  = 'nbtree-read-next-page';
	my $prefix     = $p{prefix};
	my $parallel   = $p{parallel} // 0;
	my $where      = fill_bounds($p{where});
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

	my $launched = workers_launched();

	# Only scans of merge_test_idx stop, so other sessions never wait here.
	$node->safe_psql('postgres',
		"SELECT injection_points_attach('$inj_point', 'wait', 'merge_test_idx');");

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
	is( $node->safe_psql('postgres',
			"SELECT btpo_flags & 1024 <> 0 FROM bt_page_stats('merge_test_idx', 2);"),
		't',
		"$name: block 2 was merged into its right sibling");
	$node->safe_psql('postgres', $post_sql) if $post_sql ne '';

	# Detach first, so the scan cannot stop there again (a backward scan can
	# reach the same page twice).  Then wake every process still waiting,
	# including parallel workers, until none has waited for a while.
	$node->safe_psql('postgres', qq[
		SELECT injection_points_detach('$inj_point');
		DO \$\$
		DECLARE
			quiet int := 0;
		BEGIN
			FOR i IN 1..1000 LOOP
				IF EXISTS (
					SELECT 1 FROM pg_stat_activity
					WHERE wait_event_type = 'InjectionPoint' AND wait_event = '$inj_point'
				) THEN
					quiet := 0;
					BEGIN
						PERFORM injection_points_wakeup('$inj_point');
					EXCEPTION
						WHEN OTHERS THEN NULL;
					END;
				ELSE
					quiet := quiet + 1;
					EXIT WHEN quiet >= 10;
				END IF;
				PERFORM pg_sleep(0.01);
			END LOOP;
		END \$\$;
	]);

	$h->finish();
	is($h->result(0), 0, "$name: scan exited cleanly") or diag($stderr);
	workers_ran($launched, $name) if $parallel;

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
# SECTION 2b: rescans across a merged page
# ==============================================================================
#
# A correlated subquery rescans the same index scan once for each outer row.
# Each scan stops early because of the LIMIT, often inside the merge group or
# in the middle of recovering L's tuples, and the next scan starts somewhere
# else.  No merge state may survive a rescan.
#
note("SECTION 2b: rescans across a merged page");

reset_table('sparse_10k');
{
	# Keys of the pages before the merge; afterwards block 2 is a tombstone.
	my $fwd_starts = fill_bounds('{L_MIN}, 0, {R_MIN}, {L_MAX}, {R2_MIN}, {L_MIN}');
	my $bwd_starts = fill_bounds('{R_MAX}, {R2_MAX}, {L_MAX}, {R_MIN}, {R2_MAX}');

	cmp_ok($node->safe_psql('postgres',
			"SELECT bt_merge('merge_test_idx', 10.0, 90.0, 10);"),
		'>', 0, 'rescan: merge happened');
	is( $node->safe_psql('postgres',
			"SELECT btpo_flags & 1024 <> 0 FROM bt_page_stats('merge_test_idx', 2);"),
		't',
		'rescan: block 2 was merged into its right sibling');

	for my $case ([ 'forward', '>=', 'ASC', $fwd_starts ],
		[ 'backward', '<=', 'DESC', $bwd_starts ])
	{
		my ($name, $op, $dir, $starts) = @$case;
		my $q = qq{
			SELECT g, (SELECT string_agg(id::text, ',')
					   FROM (SELECT id FROM merge_test WHERE id $op g
							 ORDER BY id $dir LIMIT 25) s)
			FROM unnest(ARRAY[$starts]) WITH ORDINALITY AS u(g, n)
			ORDER BY n;
		};

		my $plan = $node->safe_psql('postgres', "$serial_prefix EXPLAIN (COSTS OFF) $q");
		like($plan, qr/SubPlan/, "rescan $name: inner scan is a subplan");
		like($plan, qr/Index (Only )?Scan/, "rescan $name: inner scan uses the index");
		is( $node->safe_psql('postgres', "$serial_prefix $q"),
			$node->safe_psql('postgres', "$heap_prefix $q"),
			"rescan $name: every rescan matches heap");
	}
	amcheck_ok('rescan');
}

# ==============================================================================
# SECTION 3/4: concurrent serial and parallel scans
# ==============================================================================
my $split_sql = q{INSERT INTO merge_test SELECT generate_series(367, 367 + 500);};

# Each case: scan paused at its first step to another page; the merge
# happens; the scan resumes.  A serial forward scan starts on the left page
# of the pair, so it pauses after reading it and must filter the merged page
# using the boundary stored in the tombstone.  A serial backward scan starts
# on the right page, so it pauses after reading it.
#
# Parallel scans start one page outside the pair instead.  Under Gather
# Merge, a participant that read a page waits for the paused one while
# keeping its pin on that page, and bt_merge() skips a pinned pair.
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
				test_name  => ucfirst($mode) . ' ' . ($desc ? 'backward' : 'forward') . " scan, $layout: wait -> merge",
				layout     => $layout,
				prefix     => $prefix,
				parallel   => $par,
				where      => $par ? ($desc ? 'id <= {R2_MAX}' : 'id >= 0')
				  : ($desc ? 'id <= {R_MAX}' : 'id >= {L_MIN}'),
				desc       => $desc,
				merge_args => $margs,
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

# Also: scans bounded on both sides inside the merge pair, so the scan
# starts and stops inside the merged range.
for my $desc (0, 1)
{
	push @cases, {
		test_name  => 'Serial ' . ($desc ? 'backward' : 'forward') . ' bounded scan: wait -> merge',
		layout     => 'sparse_10k',
		prefix     => $serial_prefix,
		parallel   => 0,
		where      => 'id >= {L_MIN} AND id <= {R_MAX}',
		desc       => $desc,
		merge_args => '10.0, 90.0, 10',
	};
}

# Also: scans paused outside the pair.  A forward scan from the first page
# pauses before reading the left page, and a backward scan from the page
# right of the pair pauses before reading the right page.
for my $layout (qw(sparse_10k holes_5k))
{
	for my $desc (0, 1)
	{
		push @cases, {
			test_name  => 'Serial ' . ($desc ? 'backward' : 'forward') . " scan, $layout: wait " . ($desc ? 'right' : 'left') . ' of the pair -> merge',
			layout     => $layout,
			prefix     => $serial_prefix,
			parallel   => 0,
			where      => $desc ? 'id <= {R2_MAX}' : 'id >= 0',
			desc       => $desc,
			merge_args => '10.0, 90.0, 10',
		};
	}
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

# 6b: the cursor's bound stops the scan inside the merged page, where it
# still filters the rows that came from the left page.  The scan then ends,
# and the cursor turns around, which starts a new descent.  That descent
# must not reuse the old merge state.  The bounds are inside the left page's
# key range (4759..5124); the right page is 5125..5490.
{
	my $cur_setup = q{
		DROP TABLE IF EXISTS merge_turn;
		CREATE TABLE merge_turn (id int) WITH (autovacuum_enabled = off);
		INSERT INTO merge_turn SELECT generate_series(1, 20000);
		CREATE INDEX merge_turn_idx ON merge_turn (id);
		DELETE FROM merge_turn WHERE id BETWEEN 4759 AND 5490 AND id % 20 <> 0
			AND id NOT IN (4759, 5124, 5125, 5490);
		VACUUM (FREEZE) merge_turn;
	};
	# Turn at the far end, or go back right after the merge.
	my %orders = (
		far => [ [ 1000, 'forward' ], [ 1000, 'backward' ], [ 1000, 'forward' ] ],
		back => [ [ 1000, 'backward' ], [ 1000, 'forward' ], [ 1000, 'backward' ] ]);
	my @cases;
	for my $dir ('ASC', 'DESC')
	{
		for my $ios ('off', 'on')
		{
			for my $first (3, 20)
			{
				push @cases, [ $dir, $ios, $first, $_ ] for sort keys %orders;
			}
		}
	}

	my $merged = 0;
	for my $case (@cases)
	{
		my ($dir, $ios, $first, $order) = @$case;
		my @steps = @{ $orders{$order} };
		my $q = "SELECT id FROM merge_turn WHERE id > 4900 AND id < 5491 ORDER BY id $dir";
		my $name = "bounded cursor $dir, index-only $ios, merge after $first rows, turn $order";
		$node->safe_psql('postgres', $cur_setup);
		my $rows = ids_of($node, "$heap_prefix $q");
		my @expected = simulate_fetches($rows, $first, @steps);

		my $s = $node->background_psql('postgres');
		$s->query_safe(qq{
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SET LOCAL enable_seqscan = off;
			SET LOCAL enable_bitmapscan = off;
			SET LOCAL enable_indexonlyscan = $ios;
			SET LOCAL statement_timeout = '60s';
			DECLARE c SCROLL CURSOR FOR $q;
		});
		is($s->query_safe("FETCH $first FROM c"),
			join("\n", @$rows[ 0 .. $first - 1 ]),
			"$name: first FETCH");
		# An index-only scan pinning the left page makes the merge back off;
		# the result must be right either way.
		$merged++
		  if $s->query_safe(
			"SELECT bt_merge('merge_turn_idx', 10, 90, 1000)") > 0;
		for my $i (0 .. $#steps)
		{
			my ($cnt, $way) = @{ $steps[$i] };
			is($s->query_safe("FETCH $way $cnt FROM c"),
				join("\n", @{ $expected[$i] }),
				"$name: FETCH $way $cnt");
		}
		$s->query_safe('COMMIT');
		$s->quit;
		index_ok($node, 'merge_turn_idx', $name);
	}
	cmp_ok($merged, '>', 0, 'bounded cursor: merges happened');
}

# 6c: a posting list on the merged page holds TIDs from both sides of the
# boundary.  Key 500 has entries on both the left page (block 2) and the
# right page (block 4).  The index is built without deduplication, so they
# are plain tuples.  After the merge, another session turns deduplication on
# and fills the merged page with more rows of key 500, which the cursor
# cannot see.  The deduplication pass joins the entries from both pages into
# one posting list.  The scan must split that posting list at the boundary
# TID: forward it skips the left page's TIDs, backward it returns only
# them.  Rows are returned as id * 1000 + n, so every row of key 500 is
# different.
{
	my $post_setup = q{
		DROP TABLE IF EXISTS merge_post;
		CREATE TABLE merge_post (id int, n int) WITH (autovacuum_enabled = off);
		INSERT INTO merge_post SELECT g, 0 FROM generate_series(1, 2000) g;
		INSERT INTO merge_post SELECT 500, g FROM generate_series(1, 299) g;
		CREATE INDEX merge_post_idx ON merge_post (id)
			WITH (deduplicate_items = off);
		DELETE FROM merge_post WHERE id BETWEEN 367 AND 799
			AND NOT (id <> 500 AND id % 20 = 0)
			AND NOT (id = 500 AND n % 25 = 0);
		VACUUM (FREEZE) merge_post;
	};
	my $dedup_sql = q{
		ALTER INDEX merge_post_idx SET (deduplicate_items = on);
		INSERT INTO merge_post SELECT 500, g FROM generate_series(1000, 1999) g;
	};
	# Does a posting list on a merged page hold the TIDs of both rows?
	my $straddle_sql = q{
		WITH m AS MATERIALIZED (
			SELECT blkno FROM bt_multi_page_stats('merge_post_idx', 1, -1)
			WHERE type = 'l' AND btpo_flags & 512 <> 0)
		SELECT count(*) > 0
		FROM m, LATERAL bt_page_items('merge_post_idx', m.blkno) i
		WHERE i.tids @> ARRAY[
			(SELECT ctid FROM merge_post WHERE id = 500 AND n = %d),
			(SELECT ctid FROM merge_post WHERE id = 500 AND n = %d)];
	};
	my %orders = (
		far => [ [ 1000, 'forward' ], [ 1000, 'backward' ], [ 1000, 'forward' ] ],
		back => [ [ 1000, 'backward' ], [ 1000, 'forward' ], [ 1000, 'backward' ] ]);

	for my $dir ('ASC', 'DESC')
	{
		for my $first (3, 12)
		{
			for my $order (sort keys %orders)
			{
				my @steps = @{ $orders{$order} };
				my $name = "posting list cursor $dir, merge after $first rows, turn $order";
				my $q = "SELECT id * 1000 + n FROM merge_post"
				  . " WHERE id BETWEEN 367 AND 799 ORDER BY id $dir";
				$node->safe_psql('postgres', $post_setup);

				# Key 500 must be on both pages of the pair.
				my ($l_max, $r_min) = split(/\|/, $node->safe_psql('postgres', q{
					SELECT max(m.n) FILTER (WHERE s.blkno = 2),
						min(m.n) FILTER (WHERE s.blkno = 4)
					FROM (VALUES (2), (4)) s(blkno),
						LATERAL bt_page_items('merge_post_idx', s.blkno) i
						JOIN merge_post m ON m.ctid = i.htid
					WHERE m.id = 500;
				}));
				ok(defined $l_max && defined $r_min && $l_max ne '' && $r_min ne '',
					"$name: key 500 is on both pages");

				# Index order of key 500 is heap TID order.
				my $rows = ids_of($node,
					"$heap_prefix SELECT id * 1000 + n FROM merge_post"
					  . " WHERE id BETWEEN 367 AND 799 ORDER BY id $dir, ctid $dir");
				my @expected = simulate_fetches($rows, $first, @steps);

				my $s = $node->background_psql('postgres');
				$s->query_safe(qq{
					BEGIN ISOLATION LEVEL REPEATABLE READ;
					SET LOCAL enable_seqscan = off;
					SET LOCAL enable_bitmapscan = off;
					SET LOCAL statement_timeout = '60s';
					DECLARE c SCROLL CURSOR FOR $q;
				});
				is($s->query_safe("FETCH $first FROM c"),
					join("\n", @$rows[ 0 .. $first - 1 ]),
					"$name: first FETCH");
				cmp_ok($s->query_safe(
						"SELECT bt_merge('merge_post_idx', 10, 90, 1000)"),
					'>', 0, "$name: merge happened");
				is( $node->safe_psql('postgres',
						"SELECT btpo_flags & 1024 <> 0 FROM bt_page_stats('merge_post_idx', 2);"),
					't', "$name: block 2 was merged into its right sibling");

				$node->safe_psql('postgres', $dedup_sql);
				is( $node->safe_psql('postgres',
						sprintf($straddle_sql, $l_max, $r_min)),
					't',
					"$name: a posting list holds TIDs from both pages");

				for my $i (0 .. $#steps)
				{
					my ($cnt, $way) = @{ $steps[$i] };
					is($s->query_safe("FETCH $way $cnt FROM c"),
						join("\n", @{ $expected[$i] }),
						"$name: FETCH $way $cnt");
				}
				$s->query_safe('COMMIT');
				$s->quit;
				index_ok($node, 'merge_post_idx', $name);
			}
		}
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

# 7b: merge joins forward and backward, with a plain and an index-only inner
# scan.  The outer side has duplicate keys on both sides of the merge
# boundary, so the inner scan is marked on the left page or on the right
# page.  The merge happens after every possible number of fetched rows.
# When an index-only scan still pins the left page, the merge backs off; the
# result must be right either way.
{
	my $mj2_setup = q{
		DROP TABLE IF EXISTS merge_mj2;
		CREATE TABLE merge_mj2 (id int) WITH (autovacuum_enabled = off);
		INSERT INTO merge_mj2 SELECT generate_series(1, 20000);
		CREATE INDEX merge_mj2_idx ON merge_mj2 (id);
		DELETE FROM merge_mj2 WHERE id BETWEEN 4759 AND 5490 AND id % 20 <> 0
			AND id NOT IN (4759, 5124, 5125, 5490);
		VACUUM (FREEZE) merge_mj2;
	};
	$node->safe_psql('postgres', q{
		CREATE TABLE merge_mj2_outer (k int);
		INSERT INTO merge_mj2_outer VALUES (4700), (4760), (4760), (4780),
			(5100), (5124), (5124), (5125), (5125), (5140), (5300), (5300),
			(5491), (5491), (5600);
		-- with statistics, the planner keeps the small table on the outer side
		ANALYZE merge_mj2_outer;
	});

	for my $dir ('ASC', 'DESC')
	{
		for my $ios ('off', 'on')
		{
			my $name = "merge join $dir, index-only $ios";
			my $join_sql = "SELECT m.id FROM merge_mj2_outer o JOIN merge_mj2 m ON m.id = o.k ORDER BY o.k $dir";
			my $settings = qq{
				SET LOCAL enable_hashjoin = off; SET LOCAL enable_nestloop = off;
				SET LOCAL enable_material = off; SET LOCAL enable_seqscan = off;
				SET LOCAL enable_bitmapscan = off; SET LOCAL enable_indexonlyscan = $ios;
				SET LOCAL statement_timeout = '60s';
			};

			$node->safe_psql('postgres', $mj2_setup);
			my $expected = ids_of($node, qq{
				SET enable_indexscan = off; SET enable_indexonlyscan = off;
				SET enable_bitmapscan = off; SET enable_mergejoin = off;
				SET enable_nestloop = off;
				SELECT id FROM ($join_sql) x ORDER BY id;
			});
			my $total = scalar(@$expected);
			my $scan = $ios eq 'on' ? 'Index Only Scan' : 'Index Scan';
			$scan .= ' Backward' if $dir eq 'DESC';
			like(
				$node->safe_psql('postgres',
					"BEGIN; $settings EXPLAIN (COSTS OFF) DECLARE x CURSOR FOR $join_sql; COMMIT;"),
				qr/Merge Join.*->\s+Sort.*->\s+\Q$scan\E using merge_mj2_idx/s,
				"$name: the inner side is an \L$scan\E");

			my $merged = 0;
			for my $first (1 .. $total - 1)
			{
				$node->safe_psql('postgres', $mj2_setup) if $first > 1;
				my $s = $node->background_psql('postgres');
				$s->query_safe("BEGIN; $settings DECLARE c CURSOR FOR $join_sql;");
				my @got = split /\n/, $s->query_safe("FETCH $first FROM c");
				$merged++
				  if $s->query_safe(
					"SELECT bt_merge('merge_mj2_idx', 10, 90, 1000)") > 0;
				my ($rest, $ret) = $s->query('FETCH ALL FROM c');
				is($ret, 0, "$name, merge after $first rows: no error")
				  or diag($s->{stderr});
				push @got, split /\n/, $rest;
				$s->query('COMMIT');
				$s->quit;
				is_deeply([ sort { $a <=> $b } @got ], $expected,
					"$name, merge after $first rows: rows match a hash join");
			}
			cmp_ok($merged, '>', 0, "$name: merges happened");
			index_ok($node, 'merge_mj2_idx', $name);
		}
	}
}

# 7c: the mark is taken on a filtered merged page, and the boundary changes
# before the restore.  A DESC merge join reads the inner index on (k, s).
# Group 1 (L1 merged into R1) holds the start of the k = 1 run, so the join
# marks the inner scan on R1 while R1 is read with group 1's boundary.  R1 is
# also split by invisible inserts, so the scan needs that boundary for more
# than one page.  The k = 1 run continues left to group 2, which merges only
# after the scan has passed its right page, so the scan loads group 2's
# boundary.  When the second outer row restores the mark, group 1's boundary
# must come back from the copy saved with the mark.
{
	my $mr_setup = q{
		DROP TABLE IF EXISTS merge_mr;
		DROP TABLE IF EXISTS merge_mr_outer;
		CREATE TABLE merge_mr (k int, s int) WITH (autovacuum_enabled = off);
		INSERT INTO merge_mr SELECT 0, s FROM generate_series(1, 400) s;
		INSERT INTO merge_mr SELECT 1, s FROM generate_series(1, 4000) s;
		INSERT INTO merge_mr SELECT 2, s FROM generate_series(1, 800) s;
		INSERT INTO merge_mr SELECT 3, s FROM generate_series(1, 800) s;
		CREATE INDEX merge_mr_idx ON merge_mr (k, s);
		CREATE TABLE merge_mr_outer (k int);
		INSERT INTO merge_mr_outer VALUES (3), (2), (1), (1);
		ANALYZE merge_mr_outer;
	};

	# Leaf pages in key order, as [blkno, min k, min s, max k, max s].
	my $leaves = sub {
		my $out = $node->safe_psql('postgres', q{
			WITH RECURSIVE chain(blkno, nxt, n) AS (
				SELECT blkno, btpo_next, 0 FROM bt_multi_page_stats('merge_mr_idx', 1, -1)
				 WHERE type = 'l' AND btpo_prev = 0
				UNION ALL
				SELECT p.blkno, p.btpo_next, c.n + 1 FROM chain c
				  JOIN bt_multi_page_stats('merge_mr_idx', 1, -1) p ON p.blkno = c.nxt
			)
			SELECT c.blkno, min(m.k * 100000 + m.s), max(m.k * 100000 + m.s)
			  FROM chain c, LATERAL bt_page_items('merge_mr_idx', c.blkno) i
			  JOIN merge_mr m ON m.ctid = i.htid
			 WHERE NOT (c.nxt <> 0 AND i.itemoffset = 1)
			 GROUP BY c.blkno, c.n ORDER BY c.n});
		my @res;
		for my $line (split /\n/, $out)
		{
			my ($b, $lo, $hi) = split /\|/, $line;
			push @res, [ $b, int($lo / 100000), $lo % 100000,
				int($hi / 100000), $hi % 100000 ];
		}
		return \@res;
	};

	for my $ios ('off', 'on')
	{
		for my $gap (2, 3)
		{
			my $name = "merge join restore across two merges, index-only $ios, gap $gap";
			$node->safe_psql('postgres', $mr_setup);
			my $lv = $leaves->();

			# L1: last page whose highest key has k = 1; R1: its right sibling.
			my ($i1) = grep { $lv->[$_][3] == 1 } reverse 0 .. $#$lv;
			my ($L1, $R1) = @$lv[ $i1, $i1 + 1 ];
			my ($L2, $R2) = @$lv[ $i1 - $gap - 1, $i1 - $gap ];
			ok($L1->[1] == 1 && $R1->[3] == 2 && $L2->[1] == 1 && $R2->[3] == 1,
				"$name: expected page layout");
			my ($l1lo, $l1hi) = ($L1->[2], $L1->[4]);
			my ($r1lo, $r1hi) = ($R1->[1] == 2 ? $R1->[2] : 1, $R1->[4]);
			my ($l2lo, $r2hi) = ($L2->[2], $R2->[4]);

			# Thin out both pairs; group 1 below 4% full, group 2 below 15%.
			$node->safe_psql('postgres', qq{
				DELETE FROM merge_mr WHERE k = 1 AND s BETWEEN $l1lo AND $l1hi AND s % 40 <> 0;
				DELETE FROM merge_mr WHERE k = 1 AND s > $l1hi;
				DELETE FROM merge_mr WHERE k = 2 AND s BETWEEN $r1lo AND $r1hi AND s % 40 <> 0;
				DELETE FROM merge_mr WHERE k = 1 AND s BETWEEN $l2lo AND $r2hi AND s % 15 <> 0;
				VACUUM (FREEZE) merge_mr;
			});
			my ($r2now) = grep { $_->[0] == $R2->[0] } @{ $leaves->() };
			my $r2lo = $r2now->[2];

			my $join_sql = "SELECT m.k * 100000 + m.s FROM merge_mr_outer o JOIN merge_mr m ON m.k = o.k ORDER BY o.k DESC";
			my $settings = qq{
				SET LOCAL enable_hashjoin = off; SET LOCAL enable_nestloop = off;
				SET LOCAL enable_material = off; SET LOCAL enable_seqscan = off;
				SET LOCAL enable_bitmapscan = off; SET LOCAL enable_indexonlyscan = $ios;
				SET LOCAL statement_timeout = '60s';
			};
			my $scan = $ios eq 'on' ? 'Index Only Scan' : 'Index Scan';
			like(
				$node->safe_psql('postgres',
					"BEGIN; $settings EXPLAIN (COSTS OFF) DECLARE x CURSOR FOR $join_sql; COMMIT;"),
				qr/Merge Join.*->\s+Sort.*->\s+\Q$scan\E Backward using merge_mr_idx/s,
				"$name: the inner side is a backward \L$scan\E");
			my @expected = sort { $a <=> $b } @{ ids_of($node, qq{
				SET enable_indexscan = off; SET enable_indexonlyscan = off;
				SET enable_bitmapscan = off; SET enable_mergejoin = off;
				SET enable_nestloop = off; $join_sql}) };

			# Rows of k >= 2 bring the scan onto R1; rows of k = 1 down to
			# R2's lowest row bring it onto R2.
			my $to_r1 = $node->safe_psql('postgres',
				"SELECT count(*) FROM merge_mr WHERE k >= 2");
			my $to_r2 = $node->safe_psql('postgres',
				"SELECT count(*) FROM merge_mr WHERE k = 1 AND s >= $r2lo");

			my $s = $node->background_psql('postgres');
			$s->query_safe(qq{
				BEGIN ISOLATION LEVEL REPEATABLE READ;
				$settings
				DECLARE c CURSOR FOR $join_sql;
			});
			my @got = split /\n/, $s->query_safe("FETCH $to_r1 FROM c");
			is($s->query_safe("SELECT bt_merge('merge_mr_idx', 4, 90, 1000)"),
				'1', "$name: group 1 merged");
			# Split R1 inside L1's old range; the cursor cannot see these rows.
			$s->query_safe(qq{
				INSERT INTO merge_mr SELECT 1, s FROM generate_series($l1lo, $l1hi) s WHERE s % 40 <> 0;
				INSERT INTO merge_mr SELECT 1, s FROM generate_series($l1lo, $l1hi) s WHERE s % 40 <> 0;
			});
			push @got, split /\n/, $s->query_safe("FETCH $to_r2 FROM c");
			is($s->query_safe("SELECT bt_merge('merge_mr_idx', 15, 90, 1000)"),
				'1', "$name: group 2 merged");
			my ($rest, $ret) = $s->query('FETCH ALL FROM c');
			is($ret, 0, "$name: no error") or diag($s->{stderr});
			push @got, split /\n/, $rest;
			$s->query('COMMIT');
			$s->quit;
			is_deeply([ sort { $a <=> $b } @got ], \@expected,
				"$name: rows match a hash join");
			index_ok($node, 'merge_mr_idx', $name);
		}
	}
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
	my $launched = workers_launched();
	is_deeply(
		ids_of($node, "$parallel_prefix SET statement_timeout = '60s'; $q"),
		ids_of($node, "$heap_prefix $q"),
		'parallel backward scan from the merged page matches heap');
	workers_ran($launched, 'parallel backward scan from the merged page');
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

# 8b: parallel and serial scans while another session keeps merging.  The
# scanned rows (id % 20 = 0) never change.  Another session repeatedly fills
# a range with other rows, deletes them, runs VACUUM (which also cleans up
# merge groups) and merges the thinned pages.  Every scan must return the
# same rows, whatever it met on the way.
{
	$node->safe_psql('postgres', q{
		CREATE TABLE merge_churn (id int) WITH (autovacuum_enabled = off, parallel_workers = 2);
		INSERT INTO merge_churn SELECT g FROM generate_series(20, 2000000, 20) g;
		CREATE INDEX merge_churn_idx ON merge_churn (id);
		VACUUM (FREEZE, ANALYZE) merge_churn;
	});
	my $want = $node->safe_psql('postgres',
		'SELECT count(*) || \',\' || sum(id) FROM merge_churn');

	my %scans = (
		'parallel forward index-only' => [ $parallel_prefix, 'ASC', 'Parallel Index Only Scan' ],
		'parallel backward index-only' => [ $parallel_prefix, 'DESC', 'Parallel Index Only Scan Backward' ],
		'parallel forward' => [ "$parallel_prefix SET enable_indexonlyscan = off;", 'ASC', 'Parallel Index Scan' ],
		'parallel backward' => [ "$parallel_prefix SET enable_indexonlyscan = off;", 'DESC', 'Parallel Index Scan Backward' ],
		'serial backward' => [ "$serial_prefix SET enable_indexonlyscan = off;", 'DESC', 'Index Scan Backward' ]);
	for my $name (sort keys %scans)
	{
		my ($prefix, $dir, $plan) = @{ $scans{$name} };
		like($node->safe_psql('postgres',
				"$prefix EXPLAIN (COSTS OFF) SELECT id FROM merge_churn WHERE id % 20 = 0 ORDER BY id $dir"),
			qr/\Q$plan\E using merge_churn_idx/, "churn, $name: plan");
	}

	my $churn_sql = '';
	for my $i (0 .. 24)
	{
		my $lo = ($i * 77_003) % 1_980_000;
		$churn_sql .= qq{
			INSERT INTO merge_churn SELECT g FROM generate_series($lo, $lo + 20000) g WHERE g % 20 <> 0;
			DELETE FROM merge_churn WHERE id BETWEEN $lo AND $lo + 20000 AND id % 20 <> 0;
			VACUUM (INDEX_CLEANUP ON) merge_churn;
			SELECT 'merges ' || bt_merge('merge_churn_idx', 10, 90, 1000);
		};
	}
	my $churn_file = PostgreSQL::Test::Utils::tempdir() . '/churn.sql';
	PostgreSQL::Test::Utils::append_to_file($churn_file, $churn_sql);
	my ($churn_out, $churn_err) = ('', '');
	my $churn = IPC::Run::start(
		[
			$node->installed_command('psql'), '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1',
			'-p', $node->port, '-d', 'postgres', '-f', $churn_file
		],
		'>', \$churn_out, '2>', \$churn_err);

	my %bad;
	my $nscans = 0;
	my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
	while ($churn->pumpable)
	{
		if (time() > $deadline)
		{
			$churn->kill_kill;
			fail('churn: session did not finish in time');
			last;
		}
		for my $name (sort keys %scans)
		{
			my ($prefix, $dir) = @{ $scans{$name} };
			my $got = $node->safe_psql('postgres', qq{
				$prefix SET statement_timeout = '60s';
				SELECT count(*) || ',' || sum(id) FROM
				  (SELECT id FROM merge_churn WHERE id % 20 = 0 ORDER BY id $dir OFFSET 0) s});
			$nscans++;
			$bad{$name}++ if $got ne $want;
		}
		$churn->pump_nb;
	}
	$churn->finish;
	is($churn->result(0), 0, 'churn: session finished cleanly') or diag($churn_err);
	my $merges = 0;
	$merges += $1 while $churn_out =~ /merges (\d+)/g;
	cmp_ok($merges, '>', 0, "churn: merges happened ($merges merges, $nscans scans)");
	is_deeply(\%bad, {}, 'churn: every scan returned the unchanged rows');
	index_ok($node, 'merge_churn_idx', 'churn');
}

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
# cancel a standby query that blocks replay quickly
$standby->append_conf('postgresql.conf', q{
hot_standby_feedback = off
max_standby_streaming_delay = 1s
});
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

# An index-only cursor on the standby still has the left page's items when
# the primary merges that page and VACUUM removes the moved rows.  Replay of
# the merge must wait for the cursor's pin, so the cursor is either cancelled
# by a recovery conflict or returns no deleted rows.
{
	my $name = 'standby index-only cursor across merge and VACUUM';
	$node->safe_psql('postgres', q{
		CREATE TABLE merge_sb (id int) WITH (autovacuum_enabled = off);
		INSERT INTO merge_sb SELECT generate_series(1, 20000);
		CREATE INDEX merge_sb_idx ON merge_sb (id);
		VACUUM ANALYZE merge_sb;
		DELETE FROM merge_sb WHERE id BETWEEN 6589 AND 7320 AND id % 40 <> 0;
		VACUUM merge_sb;
		DELETE FROM merge_sb WHERE id IN (6800, 6840, 6880, 6920);
	});
	$node->wait_for_replay_catchup($standby);
	my $expected = $standby->safe_psql('postgres',
		"$heap_prefix SELECT id FROM merge_sb WHERE id > 6600 ORDER BY id LIMIT 12");
	my $log_offset = -s $standby->logfile;

	my $cur = $standby->background_psql('postgres');
	my $curpid = $cur->query_safe('SELECT pg_backend_pid()');
	like(
		$cur->query_safe(q{
			SET enable_seqscan = off; SET enable_bitmapscan = off;
			EXPLAIN (COSTS OFF) SELECT id FROM merge_sb WHERE id >= 6589 ORDER BY id;
		}),
		qr/Index Only Scan/,
		"$name: plan is an index-only scan");
	is( $cur->query_safe(q{
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			DECLARE c CURSOR FOR SELECT id FROM merge_sb WHERE id >= 6589 ORDER BY id;
			FETCH 1 FROM c;
		}),
		'6600',
		"$name: cursor starts at 6600");

	cmp_ok($node->safe_psql('postgres', "SELECT bt_merge('merge_sb_idx', 10, 90, 100)"),
		'>', 0, "$name: merge happened on the primary");
	$node->safe_psql('postgres', 'VACUUM merge_sb');
	$node->wait_for_replay_catchup($standby);

	# Replay has passed the merge, so the cursor's backend is either gone or
	# never blocked replay.  Give a cancelled backend a moment to exit.
	my $gone = 0;
	for (1 .. 100)
	{
		$gone = $standby->safe_psql('postgres',
			"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $curpid)") eq 't';
		last if $gone;
		usleep(100_000);
	}
	if ($gone)
	{
		ok($standby->log_contains(
				qr/User was holding shared buffer pin for too long/, $log_offset),
			"$name: cursor cancelled by a recovery conflict");
	}
	else
	{
		is($cur->query_safe('FETCH 12 FROM c'), $expected,
			"$name: no deleted rows returned");
	}
	eval { $cur->quit; };
	table_matches_heap($standby, 'merge_sb', 'id', $name);
}
$standby->stop;

# crash recovery replays the same records on the primary
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', $pages_sql),
	$primary_pages, 'crash recovery: merged and deleted pages unchanged');
index_ok($node, 'merge_redo_idx', 'crash recovery');

$node->stop();
done_testing();
