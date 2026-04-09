/* contrib/pageinspect/pageinspect--1.12--1.13.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.13'" to load this file. \quit

-- Convert SQL functions to new style

CREATE OR REPLACE FUNCTION heap_page_item_attrs(
    IN page bytea,
    IN rel_oid regclass,
    IN do_detoast bool,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_attrs bytea[]
    )
RETURNS SETOF record
LANGUAGE SQL PARALLEL RESTRICTED
BEGIN ATOMIC
SELECT lp,
       lp_off,
       lp_flags,
       lp_len,
       t_xmin,
       t_xmax,
       t_field3,
       t_ctid,
       t_infomask2,
       t_infomask,
       t_hoff,
       t_bits,
       t_oid,
       tuple_data_split(
         rel_oid::oid,
         t_data,
         t_infomask,
         t_infomask2,
         t_bits,
         do_detoast)
         AS t_attrs
  FROM heap_page_items(page);
END;

CREATE OR REPLACE FUNCTION heap_page_item_attrs(IN page bytea, IN rel_oid regclass,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_attrs bytea[]
    )
RETURNS SETOF record
LANGUAGE SQL PARALLEL RESTRICTED
BEGIN ATOMIC
SELECT * FROM heap_page_item_attrs(page, rel_oid, false);
END;


--
-- bt_find_merge_candidates(relname, min_pct_threshold, num_pages)
--
-- Scan the B-tree leaf chain and return up to num_pages LEFT block numbers
-- of adjacent pairs that are merge candidates (both pages <= min_pct_threshold
-- percent full, and combined content fits within the target fillfactor).
--
CREATE FUNCTION bt_find_merge_candidates(
    IN  relname           text,
    IN  min_pct_threshold float8 DEFAULT 10.0,
    IN  num_pages         int4   DEFAULT 1,
    OUT left_blkno        int8,
    OUT right_blkno       int8,
    OUT free_space_left   float8,
    OUT free_space_right  float8,
    OUT total_free_space  float8)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_find_merge_candidates'
LANGUAGE C PARALLEL SAFE;

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
    OUT merge_id    int4,
    OUT tids        tid[]
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_merge_detail'
LANGUAGE C PARALLEL SAFE;
