/*-------------------------------------------------------------------------
 *
 * nbtmerge.c
 *	  Merge two adjacent underutilized leaf pages into one to reduce bloat.
 *
 * The merge scans leaf pages left-to-right and, when a consecutive pair both
 * fall below a minimum fill threshold and their combined data fits within the
 * target fill factor, copies all tuples from the left page (L) into the right
 * page (R), redirects L's parent downlink to R, removes R's now-redundant
 * downlink from the parent, marks L as BTP_MERGED_AWAY (tombstone), and marks
 * R as BTP_MERGED.  VACUUM is responsible for later reclaiming tombstone pages.
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtmerge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/tableam.h"
#include "access/nbtxlog.h"
#include "access/xloginsert.h"
#include "common/int.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/injection_point.h"
#include "storage/predicate.h"
#include "miscadmin.h"

typedef struct BTMergeState
{

	Relation	rel;
	float8		min_threshold;	/* minimum fill fraction to qualify for merge */
	float8		fillfactor;		/* maximum combined fill fraction after merge */
	BlockNumber left_blkno;
	BlockNumber right_blkno;
	BTStack		stack;
}			BTMergeState;


static int32 _bt_mergescan(Relation rel, float8 min_threshold, float8 fillfactor, int pages_limit);
static bool _bt_mergepage(BTMergeState mstate);
static BTScanInsert _bt_merge_mkscankey(Relation rel, Page page, BTPageOpaque opaque);
static bool _bt_pages_mergeable(Page leftpage, Page rightpage, float8 min_threshold, float8 fillfactor);


/*
 * _bt_merge_index() -- Entry point: merge underutilized leaf pages.
 *
 * min_pct is the per-page threshold (0..100); pages below this are candidates.
 * dest_pct is the target combined fill (0..100); the merged page must fit.
 * num_pages caps how many leaf pairs are examined in one call.
 *
 * Returns the number of merges actually performed.
 */
int32
_bt_merge_index(Relation rel, float8 min_pct, float8 dest_pct, int32 num_pages)
{
	return _bt_mergescan(rel, min_pct, dest_pct, num_pages);
}


/*
 * _bt_merge_mkscankey() -- Build a BTScanInsert key from the first data tuple
 * on a leaf page.  Returns NULL if the page carries no data tuples.  Caller
 * must pfree the returned key.
 */
static BTScanInsert
_bt_merge_mkscankey(Relation rel, Page page, BTPageOpaque opaque)
{
	OffsetNumber first_off = P_FIRSTDATAKEY(opaque);

	if (first_off > PageGetMaxOffsetNumber(page))
		return NULL;

	return _bt_mkscankey(rel,
						 (IndexTuple) PageGetItem(page,
												  PageGetItemId(page, first_off)));
}

/*
 * _bt_pages_mergeable() -- Return true when the pair qualifies for merging.
 *
 * Both pages must individually be below min_threshold and their combined used
 * space must fit within fillfactor.  Thresholds are fractions (0.0 - 1.0).
 */
static bool
_bt_pages_mergeable(Page leftpage, Page rightpage,
					float8 min_threshold, float8 fillfactor)
{
	BTPageOpaque leftopaque = BTPageGetOpaque(leftpage);
	Size		left_used = BLCKSZ - PageGetFreeSpace(leftpage);
	Size		right_used = BLCKSZ - PageGetFreeSpace(rightpage);
	Size		bytes_needed = 0;
	OffsetNumber maxoff_left = PageGetMaxOffsetNumber(leftpage);
	OffsetNumber first_left = P_FIRSTDATAKEY(leftopaque);

	/* Check individual threshold qualifications first */
	if ((float8) left_used / BLCKSZ > min_threshold ||
		(float8) right_used / BLCKSZ > min_threshold)
		return false;

	/* Compute exact bytes needed for all data tuples transferred from L */
	for (OffsetNumber off = first_left; off <= maxoff_left; off++)
	{
		ItemId		itemid = PageGetItemId(leftpage, off);
		IndexTuple	itup = (IndexTuple) PageGetItem(leftpage, itemid);

		bytes_needed += MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);
	}

	/* Ensure R has enough physical free space to hold all transferred tuples */
	if (PageGetFreeSpace(rightpage) < bytes_needed)
		return false;

	/* Ensure total resulting size fits within the specified fillfactor */
	if ((float8) (right_used + bytes_needed) / BLCKSZ > fillfactor)
		return false;

	return true;
}


/*
 * _bt_mergescan() -- Walk leaf pages left-to-right looking for merge
 * candidates.
 *
 * Scans leaf pages starting from the second leftmost.  For each
 * candidate pair (L, R), verifies they share a parent, then calls
 * _bt_mergepage() to perform the actual merge under exclusive locks.
 *
 * Stops after pages_limit leaf pages have been examined or the rightmost leaf
 * is reached.  Returns the number of merges performed.
 */
static int32
_bt_mergescan(Relation rel, float8 min_threshold, float8 fillfactor, int pages_limit)
{
	Buffer		leftbuf,
				rightbuf;
	Page		leftpage,
				rightpage,
				temp_page;
	BTPageOpaque leftopaque,
				rightopaque,
				temp_opaque;
	BlockNumber left_blkno,
				right_blkno,
				current_blkno,
				r_right_blkno;
	int32		merges_performed = 0;
	int			num_pages = 0;
	BTScanInsert scankey;
	BTMergeState mstate;
	BTStack		stack;

	mstate.rel = rel;
	mstate.min_threshold = min_threshold / 100.0;
	mstate.fillfactor = fillfactor / 100.0;

	/* Start from the second leftmost leaf page. */
	{
		Buffer		endpoint_buf = _bt_get_endpoint(rel, 0, false);

		current_blkno = BufferGetBlockNumber(endpoint_buf);
		temp_page = BufferGetPage(endpoint_buf);
		temp_opaque = BTPageGetOpaque(temp_page);
		current_blkno = temp_opaque->btpo_next;
		UnlockReleaseBuffer(endpoint_buf);
	}

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (num_pages >= pages_limit || current_blkno == P_NONE)
			return merges_performed;

		/* Pin and share-lock the left candidate. */
		left_blkno = current_blkno;
		leftbuf = ReadBuffer(rel, left_blkno);
		LockBuffer(leftbuf, BUFFER_LOCK_SHARE);
		leftpage = BufferGetPage(leftbuf);
		leftopaque = BTPageGetOpaque(leftpage);

		if (P_RIGHTMOST(leftopaque))
		{
			UnlockReleaseBuffer(leftbuf);
			return merges_performed;
		}

		if (P_ISDELETED(leftopaque) || P_ISHALFDEAD(leftopaque)
			|| P_ISMERGED(leftopaque) || P_ISMERGEDAWAY(leftopaque))
		{
			current_blkno = leftopaque->btpo_next;
			UnlockReleaseBuffer(leftbuf);
			continue;
		}

		Assert(P_ISLEAF(leftopaque));

		/* Pin and share-lock the right candidate. */
		right_blkno = leftopaque->btpo_next;
		rightbuf = ReadBuffer(rel, right_blkno);
		LockBuffer(rightbuf, BUFFER_LOCK_SHARE);
		rightpage = BufferGetPage(rightbuf);
		rightopaque = BTPageGetOpaque(rightpage);

		if (P_ISDELETED(rightopaque) || P_ISHALFDEAD(rightopaque)
			|| P_ISMERGED(rightopaque) || P_ISMERGEDAWAY(rightopaque))
		{
			current_blkno = rightopaque->btpo_next;
			UnlockReleaseBuffer(rightbuf);
			UnlockReleaseBuffer(leftbuf);
			continue;
		}

		Assert(P_ISLEAF(rightopaque));

		/* Save R's right sibling while we still hold the share lock on R. */
		r_right_blkno = rightopaque->btpo_next;

		/* L is examined; slide the window to R as the default next-left. */
		num_pages++;
		current_blkno = right_blkno;

		scankey = _bt_merge_mkscankey(rel, leftpage, leftopaque);

		if (_bt_pages_mergeable(leftpage, rightpage,
								mstate.min_threshold, mstate.fillfactor) &&
			scankey != NULL)
		{
			/*
			 * R is also consumed; advance past it.
			 *
			 * Drop share locks before calling _bt_pages_share_parent.
			 * _bt_search descends to the left leaf and will try to lock it,
			 * which would fail an assertion if we already hold a lock on it.
			 * The scankey is a palloc'd copy so releasing here is safe.
			 */
			num_pages++;
			current_blkno = r_right_blkno;
			UnlockReleaseBuffer(rightbuf);
			UnlockReleaseBuffer(leftbuf);

			if (_bt_pages_share_parent(rel, left_blkno, right_blkno,
									   scankey, &stack))
			{
				mstate.left_blkno = left_blkno;
				mstate.right_blkno = right_blkno;
				mstate.stack = stack;

				if (_bt_mergepage(mstate))
					merges_performed++;

				_bt_freestack(stack);
			}

			pfree(scankey);
			continue;
		}

		/* Pages don't qualify; current_blkno already points at R. */
		UnlockReleaseBuffer(rightbuf);
		UnlockReleaseBuffer(leftbuf);
		if (scankey)
			pfree(scankey);
	}
}


/*
 * _bt_mergepage() -- Perform one leaf-page merge.
 *
 * Re-acquires exclusive locks on L, R, and their parent, re-verifies all
 * preconditions under those exclusive locks, then:
 *
 *   1. Saves R's high key and all data tuples to palloc'd memory.
 *   2. Re-initializes R's page, restoring its opaque header and high key.
 *   3. Copies all data tuples from L into R, followed by R's original tuples.
 *   4. Redirects L's downlink in the parent to point to R, then deletes R's
 *      now-redundant downlink entry from the parent.
 *   5. Marks R as BTP_MERGED, recording L's block number in R's pd_prune_xid
 *      field so backward scans can identify the merge group tombstone.
 *   6. Marks L as BTP_MERGED_AWAY, recording a safemergexid so VACUUM can
 *      determine when the tombstone page is safe to reclaim.
 *
 * Returns true if the merge completed, false if any precondition failed.
 */
static bool
_bt_mergepage(BTMergeState mstate)
{
	Relation	rel = mstate.rel;
	BlockNumber parent_blkno;
	Buffer		leftbuf,
				rightbuf,
				parentbuf = InvalidBuffer;
	Page		leftpage,
				rightpage,
				parentpage;
	BTPageOpaque leftopaque,
				rightopaque,
				popaque;
	BTStack		stack = mstate.stack;
	ItemId		itemid;
	IndexTuple	itup,
				left_itup,
				r_hikey = NULL;
	Size		r_hikey_size = 0,
				sz;
	OffsetNumber next_off;
	int			n_right;
	IndexTuple *r_tuples = NULL;
	Size	   *r_sizes = NULL;
	BTPageOpaqueData saved_opaque;
	FullTransactionId safemergexid;
	bool		merged = false;
	XLogRecPtr	recptr;

	parent_blkno = stack->bts_blkno;

	leftbuf = ReadBuffer(rel, mstate.left_blkno);
	LockBuffer(leftbuf, BT_WRITE);
	leftpage = BufferGetPage(leftbuf);
	leftopaque = BTPageGetOpaque(leftpage);
	Assert(P_ISLEAF(leftopaque));

	INJECTION_POINT("after_left_lock", NULL);

	rightbuf = ReadBuffer(rel, mstate.right_blkno);
	LockBuffer(rightbuf, BT_WRITE);
	rightpage = BufferGetPage(rightbuf);
	rightopaque = BTPageGetOpaque(rightpage);
	Assert(P_ISLEAF(rightopaque));

	/* Re-verify left & right leaf pages under exclusive lock. */
	if (P_ISDELETED(leftopaque) || P_ISHALFDEAD(leftopaque) ||
		P_ISMERGED(leftopaque) || P_ISMERGEDAWAY(leftopaque) ||
		P_INCOMPLETE_SPLIT(leftopaque) ||
		leftopaque->btpo_next != mstate.right_blkno)
		goto unlock_leaf_bufs;

	if (P_ISDELETED(rightopaque) || P_ISHALFDEAD(rightopaque) ||
		P_ISMERGED(rightopaque) || P_ISMERGEDAWAY(rightopaque) ||
		P_INCOMPLETE_SPLIT(rightopaque))
		goto unlock_leaf_bufs;

	if (!_bt_pages_mergeable(leftpage, rightpage,
							 mstate.min_threshold, mstate.fillfactor))
		goto unlock_leaf_bufs;

	/*
	 * Lock the parent and confirm that downlinks at bts_offset and
	 * bts_offset+1 still point to L and R respectively.
	 */
	parentbuf = ReadBuffer(rel, parent_blkno);
	LockBuffer(parentbuf, BT_WRITE);
	parentpage = BufferGetPage(parentbuf);
	popaque = BTPageGetOpaque(parentpage);

	if (P_ISDELETED(popaque) || P_ISHALFDEAD(popaque) ||
		popaque->btpo_level != leftopaque->btpo_level + 1)
		goto unlock_all_bufs;

	next_off = OffsetNumberNext(stack->bts_offset);
	if (stack->bts_offset < P_FIRSTDATAKEY(popaque) ||
		next_off > PageGetMaxOffsetNumber(parentpage))
		goto unlock_all_bufs;

	/* Verify L's downlink */
	itemid = PageGetItemId(parentpage, stack->bts_offset);
	if (!ItemIdIsNormal(itemid))
		goto unlock_all_bufs;
	left_itup = (IndexTuple) PageGetItem(parentpage, itemid);
	if (BTreeTupleGetDownLink(left_itup) != mstate.left_blkno)
		goto unlock_all_bufs;

	/* Verify R's downlink */
	itemid = PageGetItemId(parentpage, next_off);
	if (!ItemIdIsNormal(itemid))
		goto unlock_all_bufs;
	itup = (IndexTuple) PageGetItem(parentpage, itemid);
	if (BTreeTupleGetDownLink(itup) != mstate.right_blkno)
		goto unlock_all_bufs;

	/* Save R's high key (if not rightmost). */
	if (!P_RIGHTMOST(rightopaque))
	{
		ItemId		hikey_id = PageGetItemId(rightpage, P_HIKEY);

		r_hikey_size = ItemIdGetLength(hikey_id);
		r_hikey = (IndexTuple) palloc(r_hikey_size);
		memcpy(r_hikey, PageGetItem(rightpage, hikey_id), r_hikey_size);
	}

	/* Save all of R's data tuples into temporary memory. */
	{
		OffsetNumber r_start = P_FIRSTDATAKEY(rightopaque);
		OffsetNumber r_maxoff = PageGetMaxOffsetNumber(rightpage);

		n_right = (r_maxoff >= r_start) ? (r_maxoff - r_start + 1) : 0;
		r_tuples = palloc_array(IndexTuple, n_right);
		r_sizes = palloc_array(Size, n_right);

		for (int i = 0; i < n_right; i++)
		{
			itemid = PageGetItemId(rightpage, r_start + i);
			sz = ItemIdGetLength(itemid);
			itup = (IndexTuple) PageGetItem(rightpage, itemid);
			r_tuples[i] = (IndexTuple) palloc(sz);
			memcpy(r_tuples[i], itup, sz);
			r_sizes[i] = sz;
		}
	}

	safemergexid = ReadNextFullTransactionId();

	/*
	 * Any failure across the three-buffer update (L becomes a tombstone, R
	 * absorbs all tuples, parent loses R's downlink) would leave the index in
	 * an inconsistent state.  Perform all three modifications as an atomic
	 * unit inside a critical section so that any error panics the server
	 * rather than aborting with a partially updated index. 
	 */
	START_CRIT_SECTION();

	/* Reinitialize R, preserving its opaque header. */
	saved_opaque = *rightopaque;
	PageInit(rightpage, BufferGetPageSize(rightbuf), sizeof(BTPageOpaqueData));
	*BTPageGetOpaque(rightpage) = saved_opaque;

	if (r_hikey != NULL)
	{
		if (PageAddItem(rightpage, r_hikey, r_hikey_size,
						P_HIKEY, false, false) == InvalidOffsetNumber)
			elog(PANIC, "failed to restore high key to merged page");
	}

	/* Copy L's data tuples into R. */
	for (OffsetNumber off = P_FIRSTDATAKEY(leftopaque);
		 off <= PageGetMaxOffsetNumber(leftpage);
		 off++)
	{
		itemid = PageGetItemId(leftpage, off);
		sz = ItemIdGetLength(itemid);
		itup = (IndexTuple) PageGetItem(leftpage, itemid);

		if (PageAddItem(rightpage, itup, sz,
						InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(PANIC, "failed to copy left tuple to merged page");
	}

	/* Append R's original data tuples after L's. */
	for (int i = 0; i < n_right; i++)
	{
		if (PageAddItem(rightpage, r_tuples[i], r_sizes[i],
						InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(PANIC, "failed to copy right tuple to merged page");
	}

	/* Redirect L's downlink to R and delete R's downlink entry. */
	BTreeTupleSetDownLink(left_itup, mstate.right_blkno);
	PageIndexTupleDelete(parentpage, next_off);

	BTPageSetMerged(rightpage);
	BTMergedPageSetMABlkno(rightpage, mstate.left_blkno);
	BTPageSetMergedAway(leftpage, safemergexid);

	MarkBufferDirty(leftbuf);
	MarkBufferDirty(rightbuf);
	MarkBufferDirty(parentbuf);

	if(RelationNeedsWAL(rel))
	{
		xl_btree_merge xlrec;

		xlrec.left_prev = leftopaque->btpo_prev;
		xlrec.left_next = mstate.right_blkno;
		xlrec.poffset = stack->bts_offset;
		xlrec.safemergexid = safemergexid;
		xlrec.action = XLOG_BTREE_MERGE_PAGES;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfBtreeMerge);

		XLogRegisterBuffer(0, leftbuf, REGBUF_WILL_INIT);
		XLogRegisterBuffer(1, rightbuf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
		XLogRegisterBuffer(2, parentbuf, REGBUF_STANDARD);

		recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_MERGE);
	}
	else
		recptr = XLogGetFakeLSN(rel);

	PageSetLSN(leftpage,   recptr);
    PageSetLSN(rightpage,  recptr);
    PageSetLSN(parentpage, recptr);


	END_CRIT_SECTION();

	/* Free temporary memory. */
	if (r_hikey != NULL)
		pfree(r_hikey);
	for (int i = 0; i < n_right; i++)
		pfree(r_tuples[i]);
	pfree(r_tuples);
	pfree(r_sizes);

	PredicateLockPageCombine(rel, mstate.left_blkno, mstate.right_blkno);
	merged = true;

unlock_all_bufs:
	UnlockReleaseBuffer(parentbuf);

unlock_leaf_bufs:
	UnlockReleaseBuffer(rightbuf);
	UnlockReleaseBuffer(leftbuf);

	return merged;
}
