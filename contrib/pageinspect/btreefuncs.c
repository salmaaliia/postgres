/*
 * contrib/pageinspect/btreefuncs.c
 *
 *
 * btreefuncs.c
 *
 * Copyright (c) 2006 Satoshi Nagayasu <nagayasus@nttdata.co.jp>
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose, without fee, and without a
 * written agreement is hereby granted, provided that the above
 * copyright notice and this paragraph and the following two
 * paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pageinspect.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/varlena.h"

PG_FUNCTION_INFO_V1(bt_metap);
PG_FUNCTION_INFO_V1(bt_page_items_1_9);
PG_FUNCTION_INFO_V1(bt_page_items);
PG_FUNCTION_INFO_V1(bt_page_items_bytea);
PG_FUNCTION_INFO_V1(bt_page_stats_1_9);
PG_FUNCTION_INFO_V1(bt_page_stats);
PG_FUNCTION_INFO_V1(bt_multi_page_stats);
PG_FUNCTION_INFO_V1(bt_find_merge_candidates);
PG_FUNCTION_INFO_V1(bt_merge_detail);
PG_FUNCTION_INFO_V1(bt_merge);

#define IS_INDEX(r) ((r)->rd_rel->relkind == RELKIND_INDEX)
#define IS_BTREE(r) ((r)->rd_rel->relam == BTREE_AM_OID)

#define BTREE_MERGE_THRESHOLD  0.20 /* page must be ≤20% full to be a
									 * candidate */
#define BTREE_TARGET_FILLFACTOR 0.90	/* merged result must stay ≤90% full */


/* ------------------------------------------------
 * structure for single btree page statistics
 * ------------------------------------------------
 */
typedef struct BTPageStat
{
	uint32		blkno;
	uint32		live_items;
	uint32		dead_items;
	uint32		page_size;
	uint32		max_avail;
	uint32		free_size;
	uint32		avg_item_size;
	char		type;

	/* opaque data */
	BlockNumber btpo_prev;
	BlockNumber btpo_next;
	uint32		btpo_level;
	uint16		btpo_flags;
	BTCycleId	btpo_cycleid;
} BTPageStat;

/*
 * cross-call data structure for SRF for page stats
 */
typedef struct ua_page_stats
{
	Oid			relid;
	int64		blkno;
	int64		blk_count;
	bool		allpages;
} ua_page_stats;

/*
 * cross-call data structure for SRF for page items
 */
typedef struct ua_page_items
{
	Page		page;
	OffsetNumber offset;
	bool		leafpage;
	bool		rightmost;
	TupleDesc	tupd;
} ua_page_items;

/**
 * cross-call data structure for SRF for merge candidates
 */
typedef struct ua_merge_candidates
{
	Oid			relid;			
	BlockNumber current_blkno;	
	TupleDesc	tupd;
	float8		merge_candidate_max_pct;		
	float8		merge_destination_max_pct;		
	int			num_pages;		
	int			returned;		

}			ua_merge_candidates;

/*
 * State for bt_merge_detail(): holds the three block numbers of the
 * Parent / Left / Right page trio across SRF calls.
 */
typedef struct ua_merge_detail
{
	Oid			relid;
	BlockNumber parent_blkno;
	BlockNumber left_blkno;
	BlockNumber right_blkno;
	int			call_cntr;		/* 0=parent, 1=left, 2=right */
	bool		show_tids;
	TupleDesc	tupd;
}			ua_merge_detail;

/* -------------------------------------------------
 * GetBTPageStatistics()
 *
 * Collect statistics of single b-tree page
 * -------------------------------------------------
 */
static void
GetBTPageStatistics(BlockNumber blkno, Buffer buffer, BTPageStat *stat)
{
	Page		page = BufferGetPage(buffer);
	PageHeader	phdr = (PageHeader) page;
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	BTPageOpaque opaque = BTPageGetOpaque(page);
	int			item_size = 0;
	int			off;

	stat->blkno = blkno;

	stat->max_avail = BLCKSZ - (BLCKSZ - phdr->pd_special + SizeOfPageHeaderData);

	stat->dead_items = stat->live_items = 0;

	stat->page_size = PageGetPageSize(page);

	/* page type (flags) */
	if (P_ISDELETED(opaque))
	{
		/* We divide deleted pages into leaf ('d') or internal ('D') */
		if (P_ISLEAF(opaque) || !P_HAS_FULLXID(opaque))
			stat->type = 'd';
		else
			stat->type = 'D';

		/*
		 * Report safexid in a deleted page.
		 *
		 * Handle pg_upgrade'd deleted pages that used the previous safexid
		 * representation in btpo_level field (this used to be a union type
		 * called "bpto").
		 */
		if (P_HAS_FULLXID(opaque))
		{
			FullTransactionId safexid = BTPageGetDeleteXid(page);

			elog(DEBUG2, "deleted page from block %u has safexid %u:%u",
				 blkno, EpochFromFullTransactionId(safexid),
				 XidFromFullTransactionId(safexid));
		}
		else
			elog(DEBUG2, "deleted page from block %u has safexid %u",
				 blkno, opaque->btpo_level);

		/* Don't interpret BTDeletedPageData as index tuples */
		maxoff = InvalidOffsetNumber;
	}
	else if (P_IGNORE(opaque))
		stat->type = 'e';
	else if(P_MERGED_AWAY(opaque))
		stat->type = 'm';
	else if (P_ISLEAF(opaque))
		stat->type = 'l';
	else if (P_ISROOT(opaque))
		stat->type = 'r';
	else
		stat->type = 'i';

	/* btpage opaque data */
	stat->btpo_prev = opaque->btpo_prev;
	stat->btpo_next = opaque->btpo_next;
	stat->btpo_level = opaque->btpo_level;
	stat->btpo_flags = opaque->btpo_flags;
	stat->btpo_cycleid = opaque->btpo_cycleid;

	/* count live and dead tuples, and free space */
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		IndexTuple	itup;

		ItemId		id = PageGetItemId(page, off);

		itup = (IndexTuple) PageGetItem(page, id);

		item_size += IndexTupleSize(itup);

		if (!ItemIdIsDead(id))
			stat->live_items++;
		else
			stat->dead_items++;
	}
	stat->free_size = PageGetFreeSpace(page);

	if ((stat->live_items + stat->dead_items) > 0)
		stat->avg_item_size = item_size / (stat->live_items + stat->dead_items);
	else
		stat->avg_item_size = 0;
}

/* -----------------------------------------------
 * check_relation_block_range()
 *
 * Verify that a block number (given as int64) is valid for the relation.
 * -----------------------------------------------
 */
static void
check_relation_block_range(Relation rel, int64 blkno)
{
	/* Ensure we can cast to BlockNumber */
	if (blkno < 0 || blkno > MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid block number %" PRId64, blkno)));

	if ((BlockNumber) (blkno) >= RelationGetNumberOfBlocks(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block number %" PRId64 " is out of range", blkno)));
}

/* -----------------------------------------------
 * bt_index_block_validate()
 *
 * Validate index type is btree and block number
 * is valid (and not the metapage).
 * -----------------------------------------------
 */
static void
bt_index_block_validate(Relation rel, int64 blkno)
{
	if (!IS_INDEX(rel) || !IS_BTREE(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a %s index",
						RelationGetRelationName(rel), "btree")));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	if (blkno == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block 0 is a meta page")));

	check_relation_block_range(rel, blkno);
}

/* -----------------------------------------------
 * bt_page_stats()
 *
 * Usage: SELECT * FROM bt_page_stats('t1_pkey', 1);
 * Arguments are index relation name and block number
 * -----------------------------------------------
 */
static Datum
bt_page_stats_internal(PG_FUNCTION_ARGS, enum pageinspect_version ext_version)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	int64		blkno = (ext_version == PAGEINSPECT_V1_8 ? PG_GETARG_UINT32(1) : PG_GETARG_INT64(1));
	Buffer		buffer;
	Relation	rel;
	RangeVar   *relrv;
	Datum		result;
	HeapTuple	tuple;
	TupleDesc	tupleDesc;
	int			j;
	char	   *values[11];
	BTPageStat	stat;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	bt_index_block_validate(rel, blkno);

	buffer = ReadBuffer(rel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	/* keep compiler quiet */
	stat.btpo_prev = stat.btpo_next = InvalidBlockNumber;
	stat.btpo_flags = stat.free_size = stat.avg_item_size = 0;

	GetBTPageStatistics(blkno, buffer, &stat);

	UnlockReleaseBuffer(buffer);
	relation_close(rel, AccessShareLock);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	j = 0;
	values[j++] = psprintf("%u", stat.blkno);
	values[j++] = psprintf("%c", stat.type);
	values[j++] = psprintf("%u", stat.live_items);
	values[j++] = psprintf("%u", stat.dead_items);
	values[j++] = psprintf("%u", stat.avg_item_size);
	values[j++] = psprintf("%u", stat.page_size);
	values[j++] = psprintf("%u", stat.free_size);
	values[j++] = psprintf("%u", stat.btpo_prev);
	values[j++] = psprintf("%u", stat.btpo_next);
	values[j++] = psprintf("%u", stat.btpo_level);
	values[j++] = psprintf("%d", stat.btpo_flags);

	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc),
								   values);

	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

Datum
bt_page_stats_1_9(PG_FUNCTION_ARGS)
{
	return bt_page_stats_internal(fcinfo, PAGEINSPECT_V1_9);
}

/* entry point for old extension version */
Datum
bt_page_stats(PG_FUNCTION_ARGS)
{
	return bt_page_stats_internal(fcinfo, PAGEINSPECT_V1_8);
}


/* -----------------------------------------------
 * bt_multi_page_stats()
 *
 * Usage: SELECT * FROM bt_page_stats('t1_pkey', 1, 2);
 * Arguments are index relation name, first block number, number of blocks
 * (but number of blocks can be negative to mean "read all the rest")
 * -----------------------------------------------
 */
Datum
bt_multi_page_stats(PG_FUNCTION_ARGS)
{
	Relation	rel;
	ua_page_stats *uargs;
	FuncCallContext *fctx;
	MemoryContext mctx;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));

	if (SRF_IS_FIRSTCALL())
	{
		text	   *relname = PG_GETARG_TEXT_PP(0);
		int64		blkno = PG_GETARG_INT64(1);
		int64		blk_count = PG_GETARG_INT64(2);
		RangeVar   *relrv;

		fctx = SRF_FIRSTCALL_INIT();

		relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
		rel = relation_openrv(relrv, AccessShareLock);

		/* Check that rel is a valid btree index and 1st block number is OK */
		bt_index_block_validate(rel, blkno);

		/*
		 * Check if upper bound of the specified range is valid. If only one
		 * page is requested, skip as we've already validated the page. (Also,
		 * it's important to skip this if blk_count is negative.)
		 */
		if (blk_count > 1)
			check_relation_block_range(rel, blkno + blk_count - 1);

		/* Save arguments for reuse */
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		uargs = palloc_object(ua_page_stats);

		uargs->relid = RelationGetRelid(rel);
		uargs->blkno = blkno;
		uargs->blk_count = blk_count;
		uargs->allpages = (blk_count < 0);

		fctx->user_fctx = uargs;

		MemoryContextSwitchTo(mctx);

		/*
		 * To avoid possibly leaking a relcache reference if the SRF isn't run
		 * to completion, we close and re-open the index rel each time
		 * through, using the index's OID for re-opens to ensure we get the
		 * same rel.  Keep the AccessShareLock though, to ensure it doesn't go
		 * away underneath us.
		 */
		relation_close(rel, NoLock);
	}

	fctx = SRF_PERCALL_SETUP();
	uargs = fctx->user_fctx;

	/* We should have lock already */
	rel = relation_open(uargs->relid, NoLock);

	/* In all-pages mode, recheck the index length each time */
	if (uargs->allpages)
		uargs->blk_count = RelationGetNumberOfBlocks(rel) - uargs->blkno;

	if (uargs->blk_count > 0)
	{
		/* We need to fetch next block statistics */
		Buffer		buffer;
		Datum		result;
		HeapTuple	tuple;
		int			j;
		char	   *values[11];
		BTPageStat	stat;
		TupleDesc	tupleDesc;

		buffer = ReadBuffer(rel, uargs->blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		/* keep compiler quiet */
		stat.btpo_prev = stat.btpo_next = InvalidBlockNumber;
		stat.btpo_flags = stat.free_size = stat.avg_item_size = 0;

		GetBTPageStatistics(uargs->blkno, buffer, &stat);

		UnlockReleaseBuffer(buffer);
		relation_close(rel, NoLock);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		j = 0;
		values[j++] = psprintf("%u", stat.blkno);
		values[j++] = psprintf("%c", stat.type);
		values[j++] = psprintf("%u", stat.live_items);
		values[j++] = psprintf("%u", stat.dead_items);
		values[j++] = psprintf("%u", stat.avg_item_size);
		values[j++] = psprintf("%u", stat.page_size);
		values[j++] = psprintf("%u", stat.free_size);
		values[j++] = psprintf("%u", stat.btpo_prev);
		values[j++] = psprintf("%u", stat.btpo_next);
		values[j++] = psprintf("%u", stat.btpo_level);
		values[j++] = psprintf("%d", stat.btpo_flags);

		/* Construct tuple to be returned */
		tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc),
									   values);

		result = HeapTupleGetDatum(tuple);

		/*
		 * Move to the next block number and decrement the number of blocks
		 * still to be fetched
		 */
		uargs->blkno++;
		uargs->blk_count--;

		SRF_RETURN_NEXT(fctx, result);
	}

	/* Done, so finally we can release the index lock */
	relation_close(rel, AccessShareLock);
	SRF_RETURN_DONE(fctx);
}

/*-------------------------------------------------------
 * bt_page_print_tuples()
 *
 * Form a tuple describing index tuple at a given offset
 * ------------------------------------------------------
 */
static Datum
bt_page_print_tuples(ua_page_items *uargs)
{
	Page		page = uargs->page;
	OffsetNumber offset = uargs->offset;
	bool		leafpage = uargs->leafpage;
	bool		rightmost = uargs->rightmost;
	bool		ispivottuple;
	Datum		values[9];
	bool		nulls[9];
	HeapTuple	tuple;
	ItemId		id;
	IndexTuple	itup;
	int			j;
	int			off;
	int			dlen;
	char	   *dump,
			   *datacstring;
	char	   *ptr;
	ItemPointer htid;

	id = PageGetItemId(page, offset);

	if (!ItemIdIsValid(id))
		elog(ERROR, "invalid ItemId");

	itup = (IndexTuple) PageGetItem(page, id);

	j = 0;
	memset(nulls, 0, sizeof(nulls));
	values[j++] = Int16GetDatum(offset);
	values[j++] = ItemPointerGetDatum(&itup->t_tid);
	values[j++] = Int16GetDatum(IndexTupleSize(itup));
	values[j++] = BoolGetDatum(IndexTupleHasNulls(itup));
	values[j++] = BoolGetDatum(IndexTupleHasVarwidths(itup));

	ptr = (char *) itup + IndexInfoFindDataOffset(itup->t_info);
	dlen = IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info);

	/*
	 * Make sure that "data" column does not include posting list or pivot
	 * tuple representation of heap TID(s).
	 *
	 * Note: BTreeTupleIsPivot() won't work reliably on !heapkeyspace indexes
	 * (those built before BTREE_VERSION 4), but we have no way of determining
	 * if this page came from a !heapkeyspace index.  We may only have a bytea
	 * nbtree page image to go on, so in general there is no metapage that we
	 * can check.
	 *
	 * That's okay here because BTreeTupleIsPivot() can only return false for
	 * a !heapkeyspace pivot, never true for a !heapkeyspace non-pivot.  Since
	 * heap TID isn't part of the keyspace in a !heapkeyspace index anyway,
	 * there cannot possibly be a pivot tuple heap TID representation that we
	 * fail to make an adjustment for.  A !heapkeyspace index can have
	 * BTreeTupleIsPivot() return true (due to things like suffix truncation
	 * for INCLUDE indexes in Postgres v11), but when that happens
	 * BTreeTupleGetHeapTID() can be trusted to work reliably (i.e. return
	 * NULL).
	 *
	 * Note: BTreeTupleIsPosting() always works reliably, even with
	 * !heapkeyspace indexes.
	 */
	if (BTreeTupleIsPosting(itup))
		dlen -= IndexTupleSize(itup) - BTreeTupleGetPostingOffset(itup);
	else if (BTreeTupleIsPivot(itup) && BTreeTupleGetHeapTID(itup) != NULL)
		dlen -= MAXALIGN(sizeof(ItemPointerData));

	if (dlen < 0 || dlen > INDEX_SIZE_MASK)
		elog(ERROR, "invalid tuple length %d for tuple at offset number %u",
			 dlen, offset);
	dump = palloc0(dlen * 3 + 1);
	datacstring = dump;
	for (off = 0; off < dlen; off++)
	{
		if (off > 0)
			*dump++ = ' ';
		sprintf(dump, "%02x", *(ptr + off) & 0xff);
		dump += 2;
	}
	values[j++] = CStringGetTextDatum(datacstring);
	pfree(datacstring);

	/*
	 * We need to work around the BTreeTupleIsPivot() !heapkeyspace limitation
	 * again.  Deduce whether or not tuple must be a pivot tuple based on
	 * whether or not the page is a leaf page, as well as the page offset
	 * number of the tuple.
	 */
	ispivottuple = (!leafpage || (!rightmost && offset == P_HIKEY));

	/* LP_DEAD bit can never be set for pivot tuples, so show a NULL there */
	if (!ispivottuple)
		values[j++] = BoolGetDatum(ItemIdIsDead(id));
	else
	{
		Assert(!ItemIdIsDead(id));
		nulls[j++] = true;
	}

	htid = BTreeTupleGetHeapTID(itup);
	if (ispivottuple && !BTreeTupleIsPivot(itup))
	{
		/* Don't show bogus heap TID in !heapkeyspace pivot tuple */
		htid = NULL;
	}

	if (htid)
		values[j++] = ItemPointerGetDatum(htid);
	else
		nulls[j++] = true;

	if (BTreeTupleIsPosting(itup))
	{
		/* Build an array of item pointers */
		ItemPointer tids;
		Datum	   *tids_datum;
		int			nposting;

		tids = BTreeTupleGetPosting(itup);
		nposting = BTreeTupleGetNPosting(itup);
		tids_datum = (Datum *) palloc(nposting * sizeof(Datum));
		for (int i = 0; i < nposting; i++)
			tids_datum[i] = ItemPointerGetDatum(&tids[i]);
		values[j++] = PointerGetDatum(construct_array_builtin(tids_datum, nposting, TIDOID));
		pfree(tids_datum);
	}
	else
		nulls[j++] = true;

	/* Build and return the result tuple */
	tuple = heap_form_tuple(uargs->tupd, values, nulls);

	return HeapTupleGetDatum(tuple);
}

/*-------------------------------------------------------
 * bt_page_items()
 *
 * Get IndexTupleData set in a btree page
 *
 * Usage: SELECT * FROM bt_page_items('t1_pkey', 1);
 *-------------------------------------------------------
 */
static Datum
bt_page_items_internal(PG_FUNCTION_ARGS, enum pageinspect_version ext_version)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	int64		blkno = (ext_version == PAGEINSPECT_V1_8 ? PG_GETARG_UINT32(1) : PG_GETARG_INT64(1));
	Datum		result;
	FuncCallContext *fctx;
	MemoryContext mctx;
	ua_page_items *uargs;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));

	if (SRF_IS_FIRSTCALL())
	{
		RangeVar   *relrv;
		Relation	rel;
		Buffer		buffer;
		BTPageOpaque opaque;
		TupleDesc	tupleDesc;

		fctx = SRF_FIRSTCALL_INIT();

		relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
		rel = relation_openrv(relrv, AccessShareLock);

		bt_index_block_validate(rel, blkno);

		buffer = ReadBuffer(rel, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		/*
		 * We copy the page into local storage to avoid holding pin on the
		 * buffer longer than we must, and possibly failing to release it at
		 * all if the calling query doesn't fetch all rows.
		 */
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		uargs = palloc_object(ua_page_items);

		uargs->page = palloc(BLCKSZ);
		memcpy(uargs->page, BufferGetPage(buffer), BLCKSZ);

		UnlockReleaseBuffer(buffer);
		relation_close(rel, AccessShareLock);

		uargs->offset = FirstOffsetNumber;

		opaque = BTPageGetOpaque(uargs->page);

		if (!P_ISDELETED(opaque))
			fctx->max_calls = PageGetMaxOffsetNumber(uargs->page);
		else
		{
			/* Don't interpret BTDeletedPageData as index tuples */
			elog(NOTICE, "page from block " INT64_FORMAT " is deleted", blkno);
			fctx->max_calls = 0;
		}
		uargs->leafpage = P_ISLEAF(opaque);
		uargs->rightmost = P_RIGHTMOST(opaque);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
		tupleDesc = BlessTupleDesc(tupleDesc);

		uargs->tupd = tupleDesc;

		fctx->user_fctx = uargs;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	uargs = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
		result = bt_page_print_tuples(uargs);
		uargs->offset++;
		SRF_RETURN_NEXT(fctx, result);
	}

	SRF_RETURN_DONE(fctx);
}

Datum
bt_page_items_1_9(PG_FUNCTION_ARGS)
{
	return bt_page_items_internal(fcinfo, PAGEINSPECT_V1_9);
}

/* entry point for old extension version */
Datum
bt_page_items(PG_FUNCTION_ARGS)
{
	return bt_page_items_internal(fcinfo, PAGEINSPECT_V1_8);
}

/*-------------------------------------------------------
 * bt_page_items_bytea()
 *
 * Get IndexTupleData set in a btree page
 *
 * Usage: SELECT * FROM bt_page_items(get_raw_page('t1_pkey', 1));
 *-------------------------------------------------------
 */

Datum
bt_page_items_bytea(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	Datum		result;
	FuncCallContext *fctx;
	ua_page_items *uargs;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	if (SRF_IS_FIRSTCALL())
	{
		BTPageOpaque opaque;
		MemoryContext mctx;
		TupleDesc	tupleDesc;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		uargs = palloc_object(ua_page_items);

		uargs->page = get_page_from_raw(raw_page);

		if (PageIsNew(uargs->page))
		{
			MemoryContextSwitchTo(mctx);
			PG_RETURN_NULL();
		}

		uargs->offset = FirstOffsetNumber;

		/* verify the special space has the expected size */
		if (PageGetSpecialSize(uargs->page) != MAXALIGN(sizeof(BTPageOpaqueData)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("input page is not a valid %s page", "btree"),
					 errdetail("Expected special size %d, got %d.",
							   (int) MAXALIGN(sizeof(BTPageOpaqueData)),
							   (int) PageGetSpecialSize(uargs->page))));

		opaque = BTPageGetOpaque(uargs->page);

		if (P_ISMETA(opaque))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("block is a meta page")));

		if (P_ISLEAF(opaque) && opaque->btpo_level != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("block is not a valid btree leaf page")));

		if (P_ISDELETED(opaque))
			elog(NOTICE, "page is deleted");

		if (!P_ISDELETED(opaque))
			fctx->max_calls = PageGetMaxOffsetNumber(uargs->page);
		else
		{
			/* Don't interpret BTDeletedPageData as index tuples */
			elog(NOTICE, "page from block is deleted");
			fctx->max_calls = 0;
		}
		uargs->leafpage = P_ISLEAF(opaque);
		uargs->rightmost = P_RIGHTMOST(opaque);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
		tupleDesc = BlessTupleDesc(tupleDesc);

		uargs->tupd = tupleDesc;

		fctx->user_fctx = uargs;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	uargs = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
		result = bt_page_print_tuples(uargs);
		uargs->offset++;
		SRF_RETURN_NEXT(fctx, result);
	}

	SRF_RETURN_DONE(fctx);
}

/* Number of output arguments (columns) for bt_metap() */
#define BT_METAP_COLS_V1_8		9

/* ------------------------------------------------
 * bt_metap()
 *
 * Get a btree's meta-page information
 *
 * Usage: SELECT * FROM bt_metap('t1_pkey')
 * ------------------------------------------------
 */
Datum
bt_metap(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	Datum		result;
	Relation	rel;
	RangeVar   *relrv;
	BTMetaPageData *metad;
	TupleDesc	tupleDesc;
	int			j;
	char	   *values[9];
	Buffer		buffer;
	Page		page;
	HeapTuple	tuple;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	if (!IS_INDEX(rel) || !IS_BTREE(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a %s index",
						RelationGetRelationName(rel), "btree")));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	buffer = ReadBuffer(rel, 0);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buffer);
	metad = BTPageGetMeta(page);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/*
	 * We need a kluge here to detect API versions prior to 1.8.  Earlier
	 * versions incorrectly used int4 for certain columns.
	 *
	 * There is no way to reliably avoid the problems created by the old
	 * function definition at this point, so insist that the user update the
	 * extension.
	 */
	if (tupleDesc->natts < BT_METAP_COLS_V1_8)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("function has wrong number of declared columns"),
				 errhint("To resolve the problem, update the \"pageinspect\" extension to the latest version.")));

	j = 0;
	values[j++] = psprintf("%d", metad->btm_magic);
	values[j++] = psprintf("%d", metad->btm_version);
	values[j++] = psprintf("%u", metad->btm_root);
	values[j++] = psprintf("%u", metad->btm_level);
	values[j++] = psprintf("%u", metad->btm_fastroot);
	values[j++] = psprintf("%u", metad->btm_fastlevel);

	/*
	 * Get values of extended metadata if available, use default values
	 * otherwise.  Note that we rely on the assumption that btm_allequalimage
	 * is initialized to zero with indexes that were built on versions prior
	 * to Postgres 13 (just like _bt_metaversion()).
	 */
	if (metad->btm_version >= BTREE_NOVAC_VERSION)
	{
		values[j++] = psprintf("%u", metad->btm_last_cleanup_num_delpages);
		values[j++] = psprintf("%f", metad->btm_last_cleanup_num_heap_tuples);
		values[j++] = metad->btm_allequalimage ? "t" : "f";
	}
	else
	{
		values[j++] = "0";
		values[j++] = "-1";
		values[j++] = "f";
	}

	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc),
								   values);

	result = HeapTupleGetDatum(tuple);

	UnlockReleaseBuffer(buffer);
	relation_close(rel, AccessShareLock);

	PG_RETURN_DATUM(result);
}


/*-------------------------------------------------------
 * bt_find_merge_candidates()
 *
 * Scan the B-tree leaf chain left-to-right and return up to num_pages
 * LEFT block numbers of adjacent leaf pairs where both pages are at most
 * min_pct_threshold percent full and their combined content fits within
 * the target fillfactor.  Only merge candidates are returned.
 *
 * Usage: SELECT * FROM bt_find_merge_candidates('t1_pkey', 50.0, 5);
 *-------------------------------------------------------
 */
Datum
bt_find_merge_candidates(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	float8		merge_candidate_max_pct = PG_GETARG_FLOAT8(1);
	float8		merge_destination_max_pct = PG_GETARG_FLOAT8(2);
	int32		num_pages = PG_GETARG_INT32(3);
	Datum		result;
	FuncCallContext *fctx;
	MemoryContext mctx;
	ua_merge_candidates *uargs;
	Buffer		left_buf,
				right_buf;
	Page		left_page,
				right_page;
	BTPageOpaque left_opaque,
				right_opaque;
	BlockNumber left_blkno,
				right_blkno;
	Size		left_free,
				right_free;
	Size		left_used,
				right_used;
	BTScanInsert scankey;
	int			j;
	Datum		values[5];
	bool		nulls[5];
	HeapTuple	tuple;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));

	if (SRF_IS_FIRSTCALL())
	{
		RangeVar   *relrv;
		Relation	rel;
		TupleDesc	tupleDesc;

		fctx = SRF_FIRSTCALL_INIT();

		relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
		rel = relation_openrv(relrv, AccessShareLock);

		if (!IS_INDEX(rel) || !IS_BTREE(rel))
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is not a %s index",
								   RelationGetRelationName(rel), "btree")));

		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);
		uargs = palloc_object(ua_merge_candidates);

		/*
		 * Start at the second leftmost leaf page.
		 */
		{
			Buffer		endpoint_buf = _bt_get_endpoint(rel, 0, false);
			Page temp_page = BufferGetPage(endpoint_buf);
		    BTPageOpaque temp_opaque = BTPageGetOpaque(temp_page);
			uargs->current_blkno = temp_opaque->btpo_next;
			// elog(WARNING, "Hello from bt_find_merge_candidates");
			UnlockReleaseBuffer(endpoint_buf);
		}

		uargs->relid = RelationGetRelid(rel);
		relation_close(rel, AccessShareLock);

		uargs->merge_candidate_max_pct = merge_candidate_max_pct / 100.0;
		uargs->merge_destination_max_pct = merge_destination_max_pct / 100.0;
		uargs->num_pages = num_pages;
		uargs->returned = 0;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
		tupleDesc = BlessTupleDesc(tupleDesc);

		uargs->tupd = tupleDesc;
		fctx->user_fctx = uargs;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	uargs = fctx->user_fctx;

	if (uargs->returned >= uargs->num_pages)
		SRF_RETURN_DONE(fctx);

	/*
	 * Reopen the relation for this call.  We open and close it every call so
	 * that a LIMIT early exit cannot leak a relcache reference.
	 */
	{
		Relation	rel = relation_open(uargs->relid, AccessShareLock);

		/*
		 * Walk the leaf chain looking for the next merge candidate. We skip
		 * non-candidates and stop at the rightmost leaf or end of chain.
		 */
		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			if (uargs->current_blkno == P_NONE)
			{
				relation_close(rel, AccessShareLock);
				SRF_RETURN_DONE(fctx);
			}

			left_blkno = uargs->current_blkno;
			left_buf = ReadBuffer(rel, left_blkno);
			LockBuffer(left_buf, BUFFER_LOCK_SHARE);
			left_page = BufferGetPage(left_buf);
			left_opaque = BTPageGetOpaque(left_page);

			if (P_RIGHTMOST(left_opaque))
			{
				UnlockReleaseBuffer(left_buf);
				relation_close(rel, AccessShareLock);
				SRF_RETURN_DONE(fctx);
			}

			/* Skip deleted, half-dead, and already-merged pages. */
			if (P_ISDELETED(left_opaque) || P_ISHALFDEAD(left_opaque)
					|| P_MERGED(left_opaque) || P_MERGED_AWAY(left_opaque))
			{
				uargs->current_blkno = left_opaque->btpo_next;
				UnlockReleaseBuffer(left_buf);
				continue;
			}

			Assert(P_ISLEAF(left_opaque));

			/* Lock couple: acquire right BEFORE releasing left */
			right_blkno = left_opaque->btpo_next;
			left_free = PageGetFreeSpace(left_page);
			right_buf = ReadBuffer(rel, right_blkno);
			LockBuffer(right_buf, BUFFER_LOCK_SHARE);
			right_page = BufferGetPage(right_buf);
			right_opaque = BTPageGetOpaque(right_page);
			right_free = PageGetFreeSpace(right_page);

			/* R is unusable; skip directly past it using its btpo_next. */
			if (P_ISDELETED(right_opaque) || P_ISHALFDEAD(right_opaque)
					|| P_MERGED(right_opaque) || P_MERGED_AWAY(right_opaque))
			{
				uargs->current_blkno = right_opaque->btpo_next;
				UnlockReleaseBuffer(right_buf);
				UnlockReleaseBuffer(left_buf);
				continue;
			}

			Assert(P_ISLEAF(right_opaque));

			scankey = NULL;
			{
				OffsetNumber first_data_off = P_FIRSTDATAKEY(left_opaque);

				if (first_data_off <= PageGetMaxOffsetNumber(left_page))
				{
					IndexTuple	first_itup = (IndexTuple) PageGetItem(left_page,
																	   PageGetItemId(left_page, first_data_off));

					scankey = _bt_mkscankey(rel, first_itup);
				}
			}

			/* L is examined; slide the window to R as the default next-left. */
			uargs->current_blkno = right_blkno;

			UnlockReleaseBuffer(right_buf);
			UnlockReleaseBuffer(left_buf);

			left_used = BLCKSZ - left_free;
			right_used = BLCKSZ - right_free;

			/*
			 * Size check: both pages must individually be below the threshold
			 * AND their combined content must fit within the target fillfactor.
			 */
			if ((float) left_used / BLCKSZ <= uargs->merge_candidate_max_pct &&
				(float) right_used / BLCKSZ <= uargs->merge_candidate_max_pct &&
				(float) (left_used + right_used) / BLCKSZ <= uargs->merge_destination_max_pct)
			{
				/*
				 * Parent check: left and right must share the same immediate
				 * parent with adjacent downlinks.
				 */
				if (scankey != NULL &&
					
				_bt_pages_share_parent(rel, left_blkno, right_blkno, scankey, NULL))
				{
					pfree(scankey);
					relation_close(rel, AccessShareLock);
					break;		/* found a valid candidate */
				}
			}

			if (scankey)
				pfree(scankey);
		}
	}


	/* Build and return the output row */
	uargs->returned++;
	memset(nulls, false, sizeof(nulls));
	j = 0;
	values[j++] = Int64GetDatum((int64) left_blkno);
	values[j++] = Int64GetDatum((int64) right_blkno);
	values[j++] = Float8GetDatum(100.0 * left_free / BLCKSZ);
	values[j++] = Float8GetDatum(100.0 * right_free / BLCKSZ);
	/* free space the merged page would have */
	values[j++] = Float8GetDatum(
								 100.0 * ((int64) left_free + (int64) right_free - (int64) BLCKSZ) / BLCKSZ);

	tuple = heap_form_tuple(uargs->tupd, values, nulls);
	result = HeapTupleGetDatum(tuple);
	SRF_RETURN_NEXT(fctx, result);
}

static char *
index_tuple_data(IndexTuple itup)
{
	char	   *ptr;
	int			dlen;
	char	   *dump;
	char	   *datacstring;

	ptr = (char *) itup + IndexInfoFindDataOffset(itup->t_info);
	dlen = IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info);

	if (BTreeTupleIsPosting(itup))
		dlen -= IndexTupleSize(itup) - BTreeTupleGetPostingOffset(itup);
	else if (BTreeTupleIsPivot(itup) && BTreeTupleGetHeapTID(itup) != NULL)
		dlen -= MAXALIGN(sizeof(ItemPointerData));

	if (dlen < 0 || dlen > INDEX_SIZE_MASK)
		elog(ERROR, "invalid tuple length %d", dlen);

	dump = palloc0(dlen * 3 + 1);
	datacstring = dump;
	for (int off = 0; off < dlen; off++)
	{
		if (off > 0)
			*dump++ = ' ';
		sprintf(dump, "%02x", *(ptr + off) & 0xff);
		dump += 2;
	}

	return datacstring;
}

/*-------------------------------------------------------
 * bt_merge_detail()
 *
 * Given a LEFT leaf block number, find its Right sibling and Parent page
 * using a proper O(log N) B-tree descent, then return one row of structural
 * details for each of the three pages in this order: Parent, Left, Right.
 *
 * Each row contains the page header fields and a tid[] array.  For leaf
 * pages the array holds heap TIDs (posting-list entries are expanded).
 * For the parent page the array holds the child downlinks encoded as TIDs.
 *
 * merge_id is reserved for future use and is always NULL.
 *
 * Usage: SELECT * FROM bt_merge_detail('t1_pkey', 5, false);
 *-------------------------------------------------------
 */
Datum
bt_merge_detail(PG_FUNCTION_ARGS)
{
	FuncCallContext *fctx;
	ua_merge_detail *uargs;

	if (SRF_IS_FIRSTCALL())
	{
		text	   *relname = PG_GETARG_TEXT_PP(0);
		int64		left_blkno_arg = PG_GETARG_INT64(1);
		MemoryContext mctx;
		TupleDesc	tupleDesc;
		Relation	rel;
		List	   *relname_list;
		RangeVar   *relrv;
		Buffer		left_buf;
		Page		left_page;
		BTPageOpaque left_opaque;
		OffsetNumber first_data_off;
		IndexTuple	first_itup;
		BTScanInsert scankey;
		Buffer		found_buf = InvalidBuffer;
		BTStack		stack;
		BlockNumber parent_blkno = InvalidBlockNumber;
		BlockNumber left_blkno;
		BlockNumber right_blkno;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		uargs = palloc_object(ua_merge_detail);

		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to use pageinspect functions")));

		/* Open and validate the relation */
		relname_list = textToQualifiedNameList(relname);
		relrv = makeRangeVarFromNameList(relname_list);
		rel = relation_openrv(relrv, AccessShareLock);

		if (!IS_INDEX(rel) || !IS_BTREE(rel))
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is not a btree index",
								   RelationGetRelationName(rel))));

		left_blkno = (BlockNumber) left_blkno_arg;

		/*
		 * Step 1: Read the left leaf page to get right_blkno and the first
		 * data tuple, which we will use to build the scan key for the
		 * descent.
		 */
		left_buf = ReadBuffer(rel, left_blkno);
		LockBuffer(left_buf, BUFFER_LOCK_SHARE);
		left_page = BufferGetPage(left_buf);
		left_opaque = BTPageGetOpaque(left_page);

		if (!P_ISLEAF(left_opaque))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("block %u is not a leaf page", left_blkno)));

		right_blkno = left_opaque->btpo_next;

		if (P_RIGHTMOST(left_opaque))
			ereport(
					ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("block %u is the rightmost leaf it has no right sibling",
							left_blkno)));

		/*
		 * P_FIRSTDATAKEY returns the offset of the first real data tuple,
		 * skipping the high-key slot on non-leftmost pages.
		 */
		first_data_off = P_FIRSTDATAKEY(left_opaque);
		if (first_data_off > PageGetMaxOffsetNumber(left_page))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("leaf page %u is empty cannot determine parent",
								   left_blkno)));

		first_itup = (IndexTuple) PageGetItem(
											  left_page, PageGetItemId(left_page, first_data_off));

		/*
		 * Build a typed scan key from the tuple using the relation schema.
		 * _bt_mkscankey reads the TupleDesc and operator classes from rel, so
		 * our extension does not need to know the column data types.
		 */
		scankey = _bt_mkscankey(rel, first_itup);

		UnlockReleaseBuffer(left_buf);	/* release left before the descent */

		/*
		 * Step 2: Descend from the root in O(log N) reads. _bt_search
		 * returns: - found_buf: the leaf buffer (locked); we release it
		 * immediately. - stack: linked list from root to parent;
		 * stack->bts_blkno is the block number of the immediate parent
		 * (level-1 page).
		 */
		stack = _bt_search(rel, NULL, scankey, &found_buf, BT_READ);
		if (stack != NULL)
			parent_blkno = stack->bts_blkno;

		if (BufferIsValid(found_buf))
			UnlockReleaseBuffer(found_buf);
		if (stack != NULL)
			_bt_freestack(stack);
		pfree(scankey);

		if (parent_blkno == InvalidBlockNumber)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("could not find parent page for leaf block %u",
								   left_blkno)));

		/*
		 * Step 3: Verify that Right is also a child of the same parent.
		 *
		 * Adjacent leaf siblings do NOT always share the same parent — they
		 * can straddle a parent-page boundary after a split.  Instead of a
		 * second O(log N) descent, we simply scan the parent page we already
		 * have: if right_blkno does not appear as a downlink there, the two
		 * leaves have different parents and cannot be merged with a single
		 * parent update.
		 */
		{
			Buffer		parent_buf;
			Page		parent_page;
			OffsetNumber off,
						maxoff;
			bool		found_right = false;

			parent_buf = ReadBuffer(rel, parent_blkno);
			LockBuffer(parent_buf, BUFFER_LOCK_SHARE);
			parent_page = BufferGetPage(parent_buf);
			maxoff = PageGetMaxOffsetNumber(parent_page);

			/*
			 * For non-rightmost pages, offset 1 is the HIGH KEY — its t_tid
			 * holds the separator key's downlink to the adjacent page, NOT a
			 * child of this page.  Start from offset 2 to scan only real
			 * downlinks.
			 */
			{
				BTPageOpaque pOpaque = BTPageGetOpaque(parent_page);
				OffsetNumber scan_start = P_RIGHTMOST(pOpaque)
					? FirstOffsetNumber
					: OffsetNumberNext(P_HIKEY);

				for (off = scan_start; off <= maxoff; off++)
				{
					IndexTuple	itup = (IndexTuple) PageGetItem(
																parent_page, PageGetItemId(parent_page, off));

					if (ItemPointerGetBlockNumberNoCheck(&itup->t_tid) == right_blkno)
					{
						found_right = true;
						break;
					}
				}
			}

			UnlockReleaseBuffer(parent_buf);

			if (!found_right)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("pages cannot be merged: left block %u and right block "
								"%u span a parent boundary",
								left_blkno, right_blkno),
						 errhint("Parent block %u does not contain the right block.",
								 parent_blkno)));
			}
		}

		/*
		 * Store only the relation Oid so we can safely reopen it on every
		 * per-call invocation without leaking a relcache reference on LIMIT.
		 */
		uargs->relid = RelationGetRelid(rel);
		relation_close(rel, AccessShareLock);

		uargs->parent_blkno = parent_blkno;
		uargs->left_blkno = left_blkno;
		uargs->right_blkno = right_blkno;
		uargs->call_cntr = 0;
		uargs->show_tids = PG_GETARG_BOOL(2);

		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
		tupleDesc = BlessTupleDesc(tupleDesc);

		uargs->tupd = tupleDesc;
		fctx->user_fctx = uargs;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	uargs = fctx->user_fctx;

	/* We return exactly 3 rows: 0=parent, 1=left, 2=right */
	if (uargs->call_cntr >= 3)
		SRF_RETURN_DONE(fctx);

	{
		Relation	rel = relation_open(uargs->relid, AccessShareLock);
		BlockNumber blkno;
		BlockNumber btpo_prev_val;
		BlockNumber btpo_next_val;
		uint32		btpo_flags_val;
		uint32		btpo_level_val;
		Size		free_size_val;
		const char *role;
		Buffer		buf;
		Page		page;
		BTPageOpaque opaque;
		OffsetNumber off,
					maxoff,
					first_off;
		char	   *high_key_hex = NULL;
		char	   *first_val_hex = NULL;
		char	   *last_val_hex = NULL;
		int			max_tids;
		ItemPointerData *tids_buf;
		int			ntids = 0;
		Datum	   *tids_datum;
		Datum		values[14];
		bool		nulls[14];
		int			j;
		HeapTuple	tuple;
		Datum		result;

		switch (uargs->call_cntr)
		{
			case 0:
				blkno = uargs->parent_blkno;
				role = "parent";
				break;
			case 1:
				blkno = uargs->left_blkno;
				role = "left";
				break;
			default:
				blkno = uargs->right_blkno;
				role = "right";
				break;
		}

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		btpo_prev_val = opaque->btpo_prev;
		btpo_next_val = opaque->btpo_next;
		btpo_flags_val = opaque->btpo_flags;
		btpo_level_val = opaque->btpo_level;
		free_size_val = PageGetFreeSpace(page);

		/*
		 * Collect TIDs / downlinks from the page.
		 *
		 * Leaf pages: iterate data tuples (skip high key via P_FIRSTDATAKEY).
		 * - Posting-list tuples: expand with BTreeTupleGetPosting(). - Normal
		 * tuples: take t_tid directly.
		 *
		 * Parent page: all items are pivot tuples; the block-number part of
		 * t_tid is the child downlink.  Skip minus-infinity entries that have
		 * no real downlink (InvalidBlockNumber).
		 *
		 * Upper bound: 2*BLCKSZ/sizeof(ItemPointerData) covers any real page.
		 */
		max_tids = 2 * BLCKSZ / sizeof(ItemPointerData);
		tids_buf = (ItemPointerData *) palloc(max_tids * sizeof(ItemPointerData));

		if (P_ISLEAF(opaque))
		{
			first_off = P_FIRSTDATAKEY(opaque);
			for (off = first_off; off <= maxoff; off++)
			{
				IndexTuple	itup =
					(IndexTuple) PageGetItem(page, PageGetItemId(page, off));

				if (BTreeTupleIsPosting(itup))
				{
					int			nposting = BTreeTupleGetNPosting(itup);
					ItemPointer posting = BTreeTupleGetPosting(itup);

					for (int i = 0; i < nposting && ntids < max_tids; i++)
						tids_buf[ntids++] = posting[i];
				}
				else
				{
					if (ntids < max_tids)
						tids_buf[ntids++] = itup->t_tid;
				}
			}

			/*
			 * High key
			 */
			if (!P_RIGHTMOST(opaque))
			{
				IndexTuple	hikey =
					(IndexTuple) PageGetItem(page, PageGetItemId(page, P_HIKEY));

				high_key_hex = index_tuple_data(hikey);
			}

			/*
			 * First and last data tuples
			 */
			if (first_off <= maxoff)
			{
				IndexTuple	first_itup =
					(IndexTuple) PageGetItem(page, PageGetItemId(page, first_off));
				IndexTuple	last_itup =
					(IndexTuple) PageGetItem(page, PageGetItemId(page, maxoff));

				first_val_hex = index_tuple_data(first_itup);
				last_val_hex = index_tuple_data(last_itup);
			}
		}
		else
		{
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				IndexTuple	itup =
					(IndexTuple) PageGetItem(page, PageGetItemId(page, off));

				if (BTreeTupleIsPivot(itup) &&
					ItemPointerGetBlockNumberNoCheck(&itup->t_tid) !=
					InvalidBlockNumber)
				{
					if (ntids < max_tids)
						tids_buf[ntids++] = itup->t_tid;
				}
			}
		}

		UnlockReleaseBuffer(buf);
		relation_close(rel, AccessShareLock);

		/* Convert tids_buf into a Datum array for construct_array_builtin */
		tids_datum = (Datum *) palloc(ntids * sizeof(Datum));
		for (int i = 0; i < ntids; i++)
			tids_datum[i] = ItemPointerGetDatum(&tids_buf[i]);

		/* Build output tuple */
		memset(nulls, false, sizeof(nulls));
		j = 0;
		values[j++] = CStringGetTextDatum(role);
		values[j++] = Int64GetDatum((int64) blkno);
		values[j++] = Int64GetDatum((int64) btpo_prev_val);
		values[j++] = Int64GetDatum((int64) btpo_next_val);
		values[j++] = Int64GetDatum((int64) btpo_level_val);
		values[j++] = Int32GetDatum((int32) btpo_flags_val);
		values[j++] = Int32GetDatum((int32) free_size_val);
		values[j++] = Float8GetDatum(100.0 * free_size_val / BLCKSZ);
		values[j++] = Int32GetDatum((int32) ntids);
		if (high_key_hex)
			values[j++] = CStringGetTextDatum(high_key_hex);
		else
			nulls[j++] = true;
		if (first_val_hex)
			values[j++] = CStringGetTextDatum(first_val_hex);
		else
			nulls[j++] = true;
		if (last_val_hex)
			values[j++] = CStringGetTextDatum(last_val_hex);
		else
			nulls[j++] = true;
		nulls[j++] = true;		/* merge_id: RFU, always NULL */

		if (uargs->show_tids)
			values[j++] = PointerGetDatum(construct_array_builtin(tids_datum, ntids, TIDOID));
		else
			nulls[j++] = true;

		tuple = heap_form_tuple(uargs->tupd, values, nulls);
		result = HeapTupleGetDatum(tuple);

		uargs->call_cntr++;
		SRF_RETURN_NEXT(fctx, result);
	}
}

Datum
bt_merge(PG_FUNCTION_ARGS){
	text	   *relname = PG_GETARG_TEXT_PP(0);
	float8		merge_candidate_max_pct = PG_GETARG_FLOAT8(1);
	float8		merge_destination_max_pct = PG_GETARG_FLOAT8(2);
	int32		num_pages = PG_GETARG_INT32(3);
	Relation	rel;
	RangeVar   *relrv;
	int32 		merges_performed;

	
    if (!superuser())
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				errmsg("must be superuser to use pageinspect functions")));

    relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	if (!IS_INDEX(rel) || !IS_BTREE(rel))
		ereport(ERROR,
			(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("\"%s\" is not a %s index",
					RelationGetRelationName(rel), "btree")));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));
	

	merges_performed = _bt_merge_index(rel, merge_candidate_max_pct, merge_destination_max_pct, num_pages);

    relation_close(rel, AccessShareLock);
	
    
	PG_RETURN_INT32(merges_performed);

}
