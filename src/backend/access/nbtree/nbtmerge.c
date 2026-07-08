/*-------------------------------------------------------------------------
 *
 * nbtmerge.c
 *	  Merge two adjacent underutilized leaf pages into one to reduce bloat.
 *
 * The merge scans leaf pages left-to-right and, when a consecutive pair both
 * fall below a minimum fill threshold and their combined data fits within the
 * target fill factor, copies all tuples from the left page into the right,
 * marks the left page half-dead, and removes the left page's downlink from
 * the parent.
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtmerge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/tableam.h"
#include "common/int.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/injection_point.h"


typedef struct BTMergeState
{

	Relation	rel;
	float8		min_threshold;	/* minimum fill fraction to qualify for merge */
	float8		fillfactor;		/* maximum combined fill fraction after merge */
	BlockNumber left_blkno;
	BlockNumber right_blkno;
	BTStack		stack;
} BTMergeState;


static int32 _bt_mergescan(Relation rel, float8 min_threshold, float8 fillfactor, int pages_limit);
static bool _bt_mergepage(BTMergeState mstate);
static BTScanInsert _bt_merge_mkscankey(Relation rel, Page page, BTPageOpaque opaque);
static bool _bt_pages_mergeable(Page left_page, Page right_page, float8 min_threshold, float8 fillfactor);


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
_bt_pages_mergeable(Page left_page, Page right_page,
					float8 min_threshold, float8 fillfactor)
{
	Size		left_used = BLCKSZ - PageGetFreeSpace(left_page);
	Size		right_used = BLCKSZ - PageGetFreeSpace(right_page);

	return ((float8) left_used / BLCKSZ <= min_threshold &&
			(float8) right_used / BLCKSZ <= min_threshold &&
			(float8) (left_used + right_used) / BLCKSZ <= fillfactor);
}


/*
 * _bt_mergescan() -- Walk leaf pages left-to-right looking for merge
 * candidates.
 *
 * Stops after pages_limit pairs have been examined or the rightmost leaf is
 * reached.  Returns the number of merges performed.
 */
static int32
_bt_mergescan(Relation rel, float8 min_threshold, float8 fillfactor, int pages_limit)
{
	Buffer		left_buf,
				right_buf;
	Page		left_page,
				right_page,
				temp_page;
	BTPageOpaque left_opaque,
				right_opaque,
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
		left_buf = ReadBuffer(rel, left_blkno);
		LockBuffer(left_buf, BUFFER_LOCK_SHARE);
		left_page = BufferGetPage(left_buf);
		left_opaque = BTPageGetOpaque(left_page);

		if (P_RIGHTMOST(left_opaque))
		{
			UnlockReleaseBuffer(left_buf);
			return merges_performed;
		}

		if (P_ISDELETED(left_opaque) || P_ISHALFDEAD(left_opaque)
				|| P_MERGED(left_opaque) || P_MERGED_AWAY(left_opaque))
		{
			current_blkno = left_opaque->btpo_next;
			UnlockReleaseBuffer(left_buf);
			continue;
		}

		Assert(P_ISLEAF(left_opaque));

		/* Pin and share-lock the right candidate. */
		right_blkno = left_opaque->btpo_next;
		right_buf = ReadBuffer(rel, right_blkno);
		LockBuffer(right_buf, BUFFER_LOCK_SHARE);
		right_page = BufferGetPage(right_buf);
		right_opaque = BTPageGetOpaque(right_page);

		if (P_ISDELETED(right_opaque) || P_ISHALFDEAD(right_opaque)
				|| P_MERGED(right_opaque) || P_MERGED_AWAY(right_opaque))
		{
			current_blkno = right_opaque->btpo_next;
			UnlockReleaseBuffer(right_buf);
			UnlockReleaseBuffer(left_buf);
			continue;
		}

		Assert(P_ISLEAF(right_opaque));

		/* Save R's right sibling while we still hold the share lock on R. */
		r_right_blkno = right_opaque->btpo_next;

		/* L is examined; slide the window to R as the default next-left. */
		num_pages++;
		current_blkno = right_blkno;

		scankey = _bt_merge_mkscankey(rel, left_page, left_opaque);

		if (_bt_pages_mergeable(left_page, right_page,
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
			UnlockReleaseBuffer(right_buf);
			UnlockReleaseBuffer(left_buf);

			if (_bt_pages_share_parent(rel, left_blkno, right_blkno,
									   scankey, &stack))
			{
				mstate.left_blkno = left_blkno;
				mstate.right_blkno = right_blkno;
				mstate.stack = stack;

				if (_bt_mergepage(mstate))
					merges_performed++;
			}

			pfree(scankey);
			continue;
		}

		/* Pages don't qualify; current_blkno already points at R. */
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		if (scankey)
			pfree(scankey);
	}
}


/*
 * _bt_mergepage() -- Perform one leaf-page merge.
 *
 * Re-acquires exclusive locks on both pages and their parent, re-verifies all
 * preconditions under those locks, then:
 *
 *   1. Saves R's high key and all data tuples to palloc'd memory.
 *   2. Re-initializes R's page, restoring its opaque header and high key.
 *   3. Copies all data tuples from L into R, followed by R's original tuples.
 *   4. Redirects L's parent downlink to R and removes R's downlink entry.
 *   5. Marks L half-dead so concurrent scans skip it.
 *
 * Returns true if the merge completed, false if any precondition failed.
 */
static bool
_bt_mergepage(BTMergeState mstate)
{
	BlockNumber parent_blkno;
	Buffer		left_buf,
				right_buf,
				parent_buf;
	Page		left_page,
				right_page,
				parent_page;
	BTPageOpaque left_opaque,
				right_opaque;
	BTStack		stack;
	ItemId		itemid;
	IndexTuple	itup,
				left_itup;
	BlockNumber child;
	OffsetNumber next_off;
	IndexTuple	r_hikey = NULL;
	Size		r_hikey_size = 0;
	OffsetNumber r_start;
	OffsetNumber r_maxoff;
	int			n_right;
	IndexTuple *r_tuples;
	Size	   *r_sizes;
	BTPageOpaqueData saved_opaque;
	OffsetNumber l_start;
	OffsetNumber l_maxoff;
	ItemId		hikey_id;
	Size		sz;

	stack = mstate.stack;
	parent_blkno = stack->bts_blkno;

	left_buf = ReadBuffer(mstate.rel, mstate.left_blkno);
	LockBuffer(left_buf, BT_WRITE);
	left_page = BufferGetPage(left_buf);
	left_opaque = BTPageGetOpaque(left_page);
	Assert(P_ISLEAF(left_opaque));

	INJECTION_POINT("after_left_lock", NULL);

	right_buf = ReadBuffer(mstate.rel, mstate.right_blkno);
	LockBuffer(right_buf, BT_WRITE);
	right_page = BufferGetPage(right_buf);
	right_opaque = BTPageGetOpaque(right_page);
	Assert(P_ISLEAF(right_opaque));

	/* Re-verify left under exclusive lock. */
	if (P_ISDELETED(left_opaque) || P_ISHALFDEAD(left_opaque)
			|| P_MERGED(left_opaque) || P_MERGED_AWAY(left_opaque))
	{
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	/* A split between scan and merge would have changed left's right link. */
	if (left_opaque->btpo_next != mstate.right_blkno)
	{
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	/* Re-verify right under exclusive lock. */
	if (P_ISDELETED(right_opaque) || P_ISHALFDEAD(right_opaque)
			|| P_MERGED(right_opaque) || P_MERGED_AWAY(right_opaque))
	{
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	/* Re-check size criteria now that we hold exclusive locks. */
	if (!_bt_pages_mergeable(left_page, right_page,
							 mstate.min_threshold, mstate.fillfactor))
	{
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	/*
	 * Lock the parent and confirm that the downlinks at bts_offset and
	 * bts_offset+1 still point to L and R respectively.  If the tree was
	 * modified between our scan and now, we bail out rather than risk
	 * corrupting the index.
	 */
	parent_buf = ReadBuffer(mstate.rel, parent_blkno);
	LockBuffer(parent_buf, BT_WRITE);
	parent_page = BufferGetPage(parent_buf);

	itemid = PageGetItemId(parent_page, stack->bts_offset);
	itup = (IndexTuple) PageGetItem(parent_page, itemid);
	child = ItemPointerGetBlockNumberNoCheck(&itup->t_tid);

	if (child != mstate.left_blkno)
	{
		UnlockReleaseBuffer(parent_buf);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	left_itup = itup;

	next_off = OffsetNumberNext(stack->bts_offset);
	itemid = PageGetItemId(parent_page, next_off);
	itup = (IndexTuple) PageGetItem(parent_page, itemid);
	child = ItemPointerGetBlockNumberNoCheck(&itup->t_tid);

	if (child != mstate.right_blkno)
	{
		UnlockReleaseBuffer(parent_buf);
		UnlockReleaseBuffer(right_buf);
		UnlockReleaseBuffer(left_buf);
		return false;
	}

	/*
	 * Save R's high key so we can restore it after reinitializing the page.
	 */
	if (!P_RIGHTMOST(right_opaque))
	{
		hikey_id = PageGetItemId(right_page, P_HIKEY);
		r_hikey_size = ItemIdGetLength(hikey_id);
		r_hikey = (IndexTuple) palloc(r_hikey_size);
		memcpy(r_hikey, PageGetItem(right_page, hikey_id), r_hikey_size);
	}

	/* Save all of R's data tuples. */
	r_start = P_FIRSTDATAKEY(right_opaque);
	r_maxoff = PageGetMaxOffsetNumber(right_page);
	n_right = (r_maxoff >= r_start) ? (r_maxoff - r_start + 1) : 0;

	r_tuples = palloc_array(IndexTuple, n_right);
	r_sizes = palloc_array(Size, n_right);


	for (int i = 0; i < n_right; i++)
	{
		itemid = PageGetItemId(right_page, r_start + i);
		sz = ItemIdGetLength(itemid);
		itup = (IndexTuple) PageGetItem(right_page, itemid);
		r_tuples[i] = (IndexTuple) palloc(sz);
		memcpy(r_tuples[i], itup, sz);
		r_sizes[i] = sz;
	}

	/* Reinitialize R, preserving its opaque header. */
	saved_opaque = *right_opaque;
	PageInit(right_page, BLCKSZ, sizeof(BTPageOpaqueData));
	*BTPageGetOpaque(right_page) = saved_opaque;

	if (r_hikey != NULL)
	{
		if (PageAddItem(right_page, r_hikey, r_hikey_size,
						P_HIKEY, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to restore high key to merged page");
		pfree(r_hikey);
	}

	/* Copy L's data tuples into R. */
	l_start = P_FIRSTDATAKEY(left_opaque);
	l_maxoff = PageGetMaxOffsetNumber(left_page);

	for (OffsetNumber off = l_start; off <= l_maxoff; off++)
	{
		itemid = PageGetItemId(left_page, off);
		sz = ItemIdGetLength(itemid);
		itup = (IndexTuple) PageGetItem(left_page, itemid);

		if (PageAddItem(right_page, itup, sz,
						InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to copy left tuple to merged page");
	}

	/* Append R's original data tuples after L's. */
	for (int i = 0; i < n_right; i++)
	{
		if (PageAddItem(right_page, r_tuples[i], r_sizes[i],
						InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to copy right tuple to merged page");
		pfree(r_tuples[i]);
	}
	pfree(r_tuples);
	pfree(r_sizes);

	/*
	 * Update the parent: redirect L's downlink to R, then delete R's
	 * now-redundant downlink entry.
	 */
	BTreeTupleSetDownLink(left_itup, mstate.right_blkno);
	PageIndexTupleDelete(parent_page, next_off);
	
	BTPageSetMerged(right_page);
	/*
	 * Record the block number of the MERGED_AWAY (tombstone) page on every
	 * MERGED page.  The backward scan uses this to verify it has found the
	 * correct tombstone for this merge group, which is needed to detect a
	 * race where a new merge replaces the original tombstone while the scan
	 * is between the MERGED page and the old tombstone.
	 *
	 * We store it in pd_prune_xid, which is unused in index pages (see the
	 * BTMergedPageSetMABlkno macro in nbtree.h for full justification).
	 */
	BTMergedPageSetMABlkno(right_page, mstate.left_blkno);
	BTPageSetMergedAway(left_page);


	MarkBufferDirty(left_buf);
	MarkBufferDirty(right_buf);
	MarkBufferDirty(parent_buf);

	UnlockReleaseBuffer(parent_buf);
	UnlockReleaseBuffer(right_buf);
	UnlockReleaseBuffer(left_buf);

	return true;
}
