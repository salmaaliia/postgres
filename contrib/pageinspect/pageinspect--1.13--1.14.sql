/* contrib/pageinspect/pageinspect--1.13--1.14.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.14'" to load this file. \quit

--
-- bt_find_merge_candidates(relname, merge_candidate_max_pct,
--                          merge_destination_max_pct, num_pages)
--
-- Scan the B-tree leaf chain and return up to num_pages adjacent pairs that
-- are merge candidates (both pages <= merge_candidate_max_pct percent full,
-- and together <= merge_destination_max_pct percent of a page).
--
CREATE FUNCTION bt_find_merge_candidates(
    IN  relname           text,
    IN  merge_candidate_max_pct float8 DEFAULT 10.0,
    IN  merge_destination_max_pct float8 DEFAULT 90.0,
    IN  num_pages         int4   DEFAULT 1,
    OUT left_blkno        int8,
    OUT right_blkno       int8,
    OUT free_space_left   float8,
    OUT free_space_right  float8,
    OUT result_free_space  float8)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_find_merge_candidates'
LANGUAGE C PARALLEL RESTRICTED;

--
-- bt_merge_detail(relname, left_blkno)
--
-- Given the LEFT leaf block number, find its Right sibling and Parent via
-- a proper B-tree descent (O(log N)), then return one detail row for each
-- of the three pages: Parent, Left, Right.
--
CREATE FUNCTION bt_merge_detail(
    IN  relname     text,
    IN  left_blkno  int8,
    IN  show_tids   boolean,
    OUT page_role   text,
    OUT blkno       int8,
    OUT btpo_prev   int8,
    OUT btpo_next   int8,
    OUT btpo_level  int8,
    OUT btpo_flags  int4,
    OUT free_size   int4,
    OUT pct_free    float8,
    OUT num_tids    int4,
    OUT high_key    text,
    OUT first_val   text,
    OUT last_val    text,
    OUT tids        tid[]
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_merge_detail'
LANGUAGE C PARALLEL RESTRICTED;

CREATE FUNCTION bt_merge(
    IN  relname                   text,
    IN  merge_candidate_max_pct   float8 DEFAULT 10.0,
    IN  merge_destination_max_pct float8 DEFAULT 90.0,
    IN  num_pages                 int4   DEFAULT 1,
    OUT merges_performed          int4
)
RETURNS int4
AS 'MODULE_PATHNAME', 'bt_merge'
LANGUAGE C PARALLEL UNSAFE;
