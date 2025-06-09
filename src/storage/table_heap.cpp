#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // Check if the tuple itself is too large to fit onto any page.
  if (row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW) {
    LOG(WARNING) << "Tuple too large to fit in any page. Serialized size: " << row.GetSerializedSize(schema_);
    return false;
  }

  page_id_t current_page_id = first_page_id_;
  TablePage *table_page_obj = nullptr;

  // Case 1: The heap is currently empty. Create the first page.
  if (current_page_id == INVALID_PAGE_ID) {
    page_id_t new_first_page_id;
    Page* raw_page = buffer_pool_manager_->NewPage(new_first_page_id);
    if (raw_page == nullptr || new_first_page_id == INVALID_PAGE_ID) {
      return false; // Failed to create the first page (BPM full or disk full).
    }
    first_page_id_ = new_first_page_id; // Update heap's first page ID.
    current_page_id = first_page_id_;   // This is now the page to try.

    table_page_obj = reinterpret_cast<TablePage*>(raw_page);
    table_page_obj->Init(current_page_id, INVALID_PAGE_ID, log_manager_, txn);
    // The page is currently pinned by NewPage. It will be unpinned after the insert attempt.
  }

  // Case 2: Heap is not empty, or first page was just created. Iterate to find space.
  while (current_page_id != INVALID_PAGE_ID) {
    // Fetch the current page if not already available (e.g., from the first page creation step)
    if (table_page_obj == nullptr || table_page_obj->GetTablePageId() != current_page_id) {
        table_page_obj = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    }

    if (table_page_obj == nullptr) {
      LOG(ERROR) << "InsertTuple: Failed to fetch page " << current_page_id << ". Aborting insert.";
      return false;
    }

    // Try to insert the tuple into the current page.
    // TablePage::InsertTuple will set row.rid_ if successful.
    if (table_page_obj->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(current_page_id, true); // Page is dirty due to insert.
      return true;
    }

    // If insertion failed on this page (likely no space), try the next page.
    page_id_t next_page_id_in_chain = table_page_obj->GetNextPageId();
    buffer_pool_manager_->UnpinPage(current_page_id, false); // Unpin current page (not dirtied by failed insert).
    current_page_id = next_page_id_in_chain;
    table_page_obj = nullptr; // Nullify to ensure re-fetch for the next page in loop.
  }

  // Case 3: All existing pages are full. Need to allocate a new page and append it.
  // current_page_id is now INVALID_PAGE_ID. We need the ID of the actual last page in the chain.
  page_id_t actual_last_page_id = INVALID_PAGE_ID;
  TablePage* actual_last_page_obj_pinned = nullptr; // Pointer to the pinned last page

  // Re-traverse to find the last page (if any, first_page_id_ should be valid now).
  // This part is crucial and needs to correctly identify the last page if the heap wasn't initially empty.
  if (first_page_id_ == INVALID_PAGE_ID) {
      // This state should not be reached if empty heap was handled by creating a first page and failing there.
      // If it implies the very first page creation also failed to hold the tuple, this indicates an issue.
      LOG(ERROR) << "InsertTuple: Heap is unexpectedly still marked empty before allocating a new page at the end.";
      return false;
  }

  page_id_t iter_pid = first_page_id_;
  while(iter_pid != INVALID_PAGE_ID) {
      actual_last_page_obj_pinned = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(iter_pid));
      if (actual_last_page_obj_pinned == nullptr) {
           LOG(ERROR) << "InsertTuple: Failed to fetch page " << iter_pid << " while finding chain end.";
           return false; // Should not happen in a consistent heap
      }
      actual_last_page_id = iter_pid;
      page_id_t next_pid_in_chain = actual_last_page_obj_pinned->GetNextPageId();
      if (next_pid_in_chain == INVALID_PAGE_ID) { // This is the last page in the chain
          break; // Keep actual_last_page_obj_pinned
      }
      buffer_pool_manager_->UnpinPage(iter_pid, false); // Unpin intermediate pages
      iter_pid = next_pid_in_chain;
  }
  ASSERT(actual_last_page_obj_pinned != nullptr && actual_last_page_id != INVALID_PAGE_ID,
         "Failed to find the last page to append a new page.");

  // Allocate a new page from BPM.
  page_id_t new_page_disk_id;
  Page* raw_page_bpm = buffer_pool_manager_->NewPage(new_page_disk_id);
  if (raw_page_bpm == nullptr || new_page_disk_id == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(actual_last_page_id, false); // Unpin the previously fetched last page.
    return false; // BPM full or disk full.
  }
  TablePage* new_table_page = reinterpret_cast<TablePage*>(raw_page_bpm);

  // Initialize the new page, linking its PrevPageId to the previous actual_last_page_id.
  new_table_page->Init(new_page_disk_id, actual_last_page_id, log_manager_, txn);

  // Update the NextPageId of the previous last page to point to this new page.
  actual_last_page_obj_pinned->SetNextPageId(new_page_disk_id);
  buffer_pool_manager_->UnpinPage(actual_last_page_id, true); // Dirtied by SetNextPageId.

  // Insert the tuple into the new page. This should succeed given prior size checks.
  bool inserted_in_new = new_table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(new_page_disk_id, true); // Dirtied by Init and Insert.

  return inserted_in_new;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &new_row, const RowId &rid, Txn *txn) {
  if (rid.GetPageId() == INVALID_PAGE_ID) {
    return false; // Cannot update a tuple with an invalid RowId.
  }

  // Fetch the page where the old tuple resides.
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false; // Page not found in buffer pool.
  }

  // TablePage::UpdateTuple attempts an in-place update.
  // It expects 'old_row_data_for_page' as an output parameter where it can store the *previous* version of the row.
  Row old_row_data_for_page;
  // Note: TablePage::UpdateTuple must check if the tuple at 'rid' exists and is not deleted.
  bool updated_in_place = page->UpdateTuple(new_row, &old_row_data_for_page, schema_, txn, lock_manager_, log_manager_);

  if (updated_in_place) {
    new_row.SetRowId(rid); // If updated in place, RowId remains the same.
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true); // Page is dirty.
    return true;
  }

  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);

  // To robustly handle DEL+INS, first verify the old tuple actually exists if TablePage::UpdateTuple doesn't guarantee this check.
  // A simple GetTuple call can do this.
  Row check_old_row;
  check_old_row.SetRowId(rid);
  if (!GetTuple(&check_old_row, txn)) { // GetTuple fetches, checks existence (incl. delete mask), and unpins.
      // LOG(WARNING) << "UpdateTuple: Original tuple at RID " << rid.Get() << " not found or already deleted before DEL+INS.";
      return false; // Original tuple doesn't exist or is marked deleted.
  }

  // Check if new_row is too large for *any* page before attempting DEL+INS.
  if (new_row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW) {
    LOG(WARNING) << "UpdateTuple: New row is too large to fit in any page.";
    return false;
  }

  // Perform Delete (MarkDelete) + Insert.
  if (!MarkDelete(rid, txn)) {
    // MarkDelete might fail if another transaction interfered, or if the tuple state changed.
    LOG(WARNING) << "UpdateTuple: MarkDelete failed for RID " << rid.Get() << " during DEL+INS.";
    return false;
  }

  // Try to insert the new version of the tuple. InsertTuple will find/create a page
  // and crucially set new_row.rid_ to its new RowId.
  if (InsertTuple(new_row, txn)) {
    return true;
  } else {
    // InsertTuple failed (e.g., BPM full, disk full). This is a critical state.
    // We must roll back the MarkDelete to maintain data consistency.
    LOG(ERROR) << "UpdateTuple: InsertTuple failed after MarkDelete for old RID " << rid.Get()
               << ". Rolling back the delete.";
    RollbackDelete(rid, txn); // Attempt to revert the MarkDelete.
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  if (rid.GetPageId() == INVALID_PAGE_ID) {
    return; // Cannot apply delete to an invalid RID.
  }
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(WARNING) << "ApplyDelete: Page " << rid.GetPageId() << " not found. Cannot apply delete for tuple in slot " << rid.GetSlotNum();
    return;
  }

  // TablePage::ApplyDelete will physically remove the tuple data and update slot information.
  // It should also handle page latching.
  page->ApplyDelete(rid, txn, log_manager_);

  // The page structure has been modified.
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  if (row == nullptr) {
    return false;
  }
  RowId rid = row->GetRowId(); // Caller must set row->rid_ to the RID of the tuple they want.
  if (rid.GetPageId() == INVALID_PAGE_ID) {
    return false; // Invalid RID.
  }

  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    // Page not found in buffer pool, so tuple cannot be retrieved from memory.
    return false;
  }

  bool found = page->GetTuple(row, schema_, txn, lock_manager_);

  // Unpin the page. It was a read operation, so the page is not marked dirty by this GetTuple call.
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return found;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t current_page_id = first_page_id_;
  RowId first_valid_rid(INVALID_PAGE_ID, 0); // Use INVALID_ROWID from common/rowid.h

  if (current_page_id == INVALID_PAGE_ID) {
    return End(); // Heap is empty.
  }

  // Iterate through pages starting from the first page to find the first valid tuple.
  while (current_page_id != INVALID_PAGE_ID) {
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page == nullptr) {
      LOG(ERROR) << "TableHeap::Begin: Failed to fetch page " << current_page_id << ".";
      return End(); // Error fetching page, return end iterator.
    }

    // TablePage::GetFirstTupleRid should find the RID of the first non-deleted tuple on this page.
    if (page->GetFirstTupleRid(&first_valid_rid)) {
      // Found the first tuple in the heap.
      buffer_pool_manager_->UnpinPage(current_page_id, false); // Unpin; read-only for finding RID.
      // The TableIterator constructor will call GetTuple, which will re-fetch/pin this page
      // and load the row data.
      return TableIterator(this, first_valid_rid, txn);
    }

    // No valid (non-deleted) tuples on this page, try the next page in the chain.
    page_id_t next_page_id_in_chain = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(current_page_id, false); // Unpin this page.
    current_page_id = next_page_id_in_chain;
  }

  // If the loop finishes, it means all pages were checked and no valid tuples were found.
  return End(); // Return an end iterator.
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  // The end iterator is conventionally represented by an invalid RowId.
  return TableIterator(this, INVALID_ROWID, nullptr);
}
