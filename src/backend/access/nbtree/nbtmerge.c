#include "postgres.h"

#include "access/nbtree.h"
#include "access/tableam.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "common/int.h"

typedef struct BTMergeState
{
    /* Passed to _bt_mergepage */
    Relation    rel;
    float8      min_threshold;  /* for re-verification inside mergepage */
    float8      fillfactor;     /* for re-verification inside mergepage */
    BlockNumber left_blkno;
    BlockNumber right_blkno;
} BTMergeState;


static int32 _bt_mergescan(Relation rel, float8 min_threshold, float8 fillfactor, int pages_limit);
static bool _bt_mergepage(BTMergeState mstate);
static BTScanInsert _bt_merge_mkscankey(Relation rel, Page page, BTPageOpaque opaque);
static bool _bt_pages_mergeable(Page left_page, Page right_page, float8 min_threshold, float8 fillfactor);

int32
_bt_merge_index(Relation rel, float8 min_pct, float8 dest_pct, int32 num_pages)
{
    return _bt_mergescan(rel, min_pct, dest_pct, num_pages);
    
}


/*
 * Build a BTScanInsert key from the first data tuple on a leaf page.
 * Returns NULL if the page is empty (no data tuples).
 * Caller is responsible for pfree'ing the returned scankey.
 */
static BTScanInsert
_bt_merge_mkscankey(Relation rel, Page page, BTPageOpaque opaque)
{
    OffsetNumber first_off = P_FIRSTDATAKEY(opaque);

    if (first_off > PageGetMaxOffsetNumber(page))
        return NULL;    /* page is empty */

    return _bt_mkscankey(rel, (IndexTuple) PageGetItem(page, PageGetItemId(page, first_off)));
}

/*
 * Return true if the pair (left, right) meets the merge size criteria:
 * both pages individually below min_threshold, and combined used space
 * fits within fillfactor. Thresholds are already expressed as fractions
 * (0.0 - 1.0), not percentages.
 */
static bool
_bt_pages_mergeable(Page left_page, Page right_page, float8 min_threshold, float8 fillfactor)
{
    Size left_used  = BLCKSZ - PageGetFreeSpace(left_page);
    Size right_used = BLCKSZ - PageGetFreeSpace(right_page);

    return ((float8) left_used  / BLCKSZ <= min_threshold &&
            (float8) right_used / BLCKSZ <= min_threshold &&
            (float8) (left_used + right_used) / BLCKSZ <= fillfactor);
}



static int32 
_bt_mergescan(Relation rel, float8 min_threshold, float8 fillfactor, int pages_limit){
    Buffer		left_buf,
				right_buf;
	Page		left_page,
				right_page;
	BTPageOpaque left_opaque,
                right_opaque;
	BlockNumber left_blkno,
				right_blkno,
                current_blkno,
        // this is just temporary till we add the MERGED flag!
                potential_blkno;
    int32 merges_performed = 0;
    int num_pages = 0;
    BTScanInsert scankey;
    BTMergeState mstate;

    mstate.rel = rel;
    mstate.min_threshold = min_threshold / 100.0;
    mstate.fillfactor = fillfactor / 100.0;



    // should I take rel or oid as a parameter? rel
    // how the parameters are going to be passed to the function? how it is going to be called? we have to make a sql function to be 
    // able to call this function from outside
    // Which lock to take for the relation

    // get the leftmost leaf page to start from there from _bt_get_endpoint
    {
		Buffer endpoint_buf = _bt_get_endpoint(rel, 0, false);

		current_blkno = BufferGetBlockNumber(endpoint_buf);
		UnlockReleaseBuffer(endpoint_buf);
	}
    // a loop to stop if it reached the limit pages OR reached the rightmost leaf page in the tree
    // call _bt_mergepage()
    for(;;){

        CHECK_FOR_INTERRUPTS();
        // return if reahed the page limit
        if(num_pages >= pages_limit){
            return merges_performed;
        }

        if(current_blkno == P_NONE){
            return merges_performed;
        }

        // get the left page
        left_blkno = current_blkno;
        left_buf = ReadBuffer(rel, left_blkno);
        LockBuffer(left_buf, BUFFER_LOCK_SHARE);
        left_page = BufferGetPage(left_buf);
        left_opaque = BTPageGetOpaque(left_page);

        if(P_RIGHTMOST(left_opaque)){
            UnlockReleaseBuffer(left_buf);
            // should return 
            return merges_performed;
        }

        if (P_ISDELETED(left_opaque) || !P_ISLEAF(left_opaque)){
            current_blkno = left_opaque->btpo_next;
            UnlockReleaseBuffer(left_buf);
			continue;
        }

        // get right page 
        right_blkno = left_opaque->btpo_next;
        right_buf = ReadBuffer(rel, right_blkno);
        LockBuffer(right_buf, BUFFER_LOCK_SHARE);
        right_page = BufferGetPage(right_buf);
        right_opaque = BTPageGetOpaque(right_page);


        current_blkno = right_blkno;
        // temporary
        potential_blkno = right_opaque->btpo_next;
        num_pages++;



        // 
        scankey = NULL;
        scankey = _bt_merge_mkscankey(rel, left_page, left_opaque);

        /*
         * 1- both pages must be below min_threshold and combined used
         *    space must fit within fillfactor.
         */
        if (_bt_pages_mergeable(left_page, right_page,
                                mstate.min_threshold, mstate.fillfactor))
        {
            if (scankey != NULL)
            {
                /*
                 * Release SHARE locks BEFORE calling _bt_pages_share_parent.
                 * _bt_search inside it descends to the left leaf and tries to
                 * lock it — assertion fails if we already hold a lock on it.
                 * The scankey is a standalone palloc'd copy so releasing here
                 * is safe.
                 */
                UnlockReleaseBuffer(right_buf);
                UnlockReleaseBuffer(left_buf);

                /* 2- must share the same parent */
                if (_bt_pages_share_parent(rel, left_blkno, right_blkno, scankey))
                {
                    mstate.left_blkno  = left_blkno;
                    mstate.right_blkno = right_blkno;

                    if (_bt_mergepage(mstate))
                    {
                        current_blkno = potential_blkno;
                        num_pages++;
                        merges_performed++;
                    }
                }

                pfree(scankey);
                continue;
            }
        }
        /* not a candidate — release and move on */
        UnlockReleaseBuffer(right_buf);
        UnlockReleaseBuffer(left_buf);
        if (scankey)
            pfree(scankey);
    }


}

static bool 
_bt_mergepage(BTMergeState mstate){
    Buffer		left_buf,
				right_buf;
	Page		left_page,
				right_page;
	BTPageOpaque left_opaque,
                right_opaque;
    
    // lock the relation --> locked in the caller
    // lock the left page  --> need exclusive look for now
    left_buf = ReadBuffer(mstate.rel, mstate.left_blkno);
    LockBuffer(left_buf, BT_WRITE);
    left_page = BufferGetPage(left_buf);
    left_opaque = BTPageGetOpaque(left_page);

    // lock right page --> exclusive look
    right_buf = ReadBuffer(mstate.rel, mstate.right_blkno);
    LockBuffer(right_buf, BT_WRITE);
    right_page = BufferGetPage(right_buf);
    right_opaque = BTPageGetOpaque(right_page);

    /* Re-verify left is still valid */
    if (P_ISDELETED(left_opaque) || !P_ISLEAF(left_opaque)){
        UnlockReleaseBuffer(right_buf);
        UnlockReleaseBuffer(left_buf);
        return false;
    }

    // what if left's right link is not the same as right? --> a split happened
    // SO SHOULD I JUST APPORT the merge 

    /* detect split between scan and merge */
    if(left_opaque->btpo_next != mstate.right_blkno){
        UnlockReleaseBuffer(right_buf);
        UnlockReleaseBuffer(left_buf);
        return false;
    }

    /* re-verify right */
    if (P_ISDELETED(right_opaque) || !P_ISLEAF(right_opaque))
    {
        UnlockReleaseBuffer(right_buf);
        UnlockReleaseBuffer(left_buf);
        return false;
    }

    
    // recheck the condition again --> size 


    if(_bt_pages_mergeable(left_page, right_page, mstate.min_threshold, mstate.fillfactor)){
        
        // Do the actual merge

        /**
         *  1. Read all tuples from R —> save them in memory
            2. Clear R's tuple area
            3. Write all L tuples into R first
            4. Write all original R tuples after the L tuples
            5. Mark L as half-dead
            6. R' is now complete
         */

        /* Save the high key of R */
        IndexTuple r_hikey = NULL;
        Size r_hikey_size = 0;
        OffsetNumber r_start;
        OffsetNumber r_maxoff;
        int          n_right;
        IndexTuple  *r_tuples;
        Size        *r_sizes;
        BTPageOpaqueData saved_opaque;
        OffsetNumber l_start;
        OffsetNumber l_maxoff;



        
        if(!P_RIGHTMOST(right_opaque)){
            ItemId hikey_id = PageGetItemId(right_page, P_HIKEY);
            r_hikey_size = ItemIdGetLength(hikey_id);
            r_hikey = (IndexTuple) palloc(r_hikey_size);
            memcpy(r_hikey, PageGetItem(right_page, hikey_id), r_hikey_size);
        }

        // Save R's Data in Memory
        r_start = P_FIRSTDATAKEY(right_opaque);
        r_maxoff = PageGetMaxOffsetNumber(right_page);
        n_right = (r_maxoff >= r_start)? (r_maxoff - r_start + 1): 0;

        r_tuples = palloc(n_right * sizeof(IndexTuple)); /* allocates array of pointers*/
        r_sizes = palloc(n_right * sizeof(Size));

        for(int i = 0; i < n_right; i++){
            ItemId itemid = PageGetItemId(right_page, r_start + i);
            Size sz = ItemIdGetLength(itemid);
            IndexTuple itup = (IndexTuple) PageGetItem(right_page, itemid);

            r_tuples[i] = (IndexTuple) palloc(sz);
            memcpy(r_tuples[i], itup, sz);
            r_sizes[i] = sz;
        }


        ///// I need to recheck this part
        saved_opaque = *right_opaque;

        PageInit(right_page, BLCKSZ, sizeof(BTPageOpaqueData));

        *BTPageGetOpaque(right_page) = saved_opaque;

        // re add the high key
        if(r_hikey != NULL){
            if(PageAddItem(right_page, r_hikey, r_hikey_size, P_HIKEY, false, false) == InvalidOffsetNumber)
                elog(ERROR, "failed to add high key to merged page");

            pfree(r_hikey);
            
        }

        // copy L's data to R
        l_start = P_FIRSTDATAKEY(left_opaque);
        l_maxoff = PageGetMaxOffsetNumber(left_page);

        for(OffsetNumber off = l_start; off <= l_maxoff; off++){
            ItemId itemid = PageGetItemId(left_page, off);
            Size sz = ItemIdGetLength(itemid);
            IndexTuple itup = PageGetItem(left_page, itemid);

            if(PageAddItem(right_page, itup, sz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
                elog(ERROR, "failed to add high key to merged page");
        }

        // Re-copy R data

        for(int i = 0; i < n_right; i++){
            if(PageAddItem(right_page, r_tuples[i], r_sizes[i], InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
                elog(ERROR, "failed to copy right tuple to merged page");
            pfree(r_tuples[i]);
        }
        pfree(r_tuples);
        pfree(r_sizes);

        MarkBufferDirty(right_buf);

        UnlockReleaseBuffer(right_buf);

        /**
         * TODO:
         * 1-  unlink L from parent and assign its key space to R. 
         * 2- test if this test amcheck
         * 3- select count(a) from tbl where a>= 4 and a<=9;
         * 4- EXPLAIN SELECT count(1) FROM merge_test WHERE id =199;
         * 5- Scan the code to find where BTP_HALF_DEAD is set 
         * 6- Review the amckec code for the message it says it failing
         */

        /* Mark L half-dead so scans skip it */
        left_opaque->btpo_flags |= BTP_HALF_DEAD;

        MarkBufferDirty(left_buf);
        UnlockReleaseBuffer(left_buf);
        return true;
    }


    return false;
}