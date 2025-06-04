#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) :
    table_heap_(table_heap), current_rid_(rid), txn_(txn) {
  // If the iterator is initialized with a valid TableHeap and a potentially valid starting RowId
  // (i.e., not explicitly an end-iterator marker like INVALID_ROWID passed for rid),
  // attempt to pre-fetch the current row's data.
  if (table_heap_ != nullptr && current_rid_.GetPageId() != INVALID_PAGE_ID) {
    current_row_.SetRowId(current_rid_); // Set the RID for the row buffer
    // Try to fetch the tuple. If it fails (e.g., tuple is deleted, RID is invalid beyond heap bounds),
    // this iterator should be considered invalid or at an "end" state.
    if (!table_heap_->GetTuple(&current_row_, txn_)) {
      // LOG(WARNING) << "TableIterator: GetTuple failed for initial RID: " << current_rid_.GetPageId() << "," << current_rid_.GetSlotNum();
      current_rid_.Set(INVALID_PAGE_ID, 0); // Mark as an end/invalid iterator
    }
  }
  // If table_heap_ is nullptr or rid is INVALID_PAGE_ID, it's an end iterator by construction.
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  current_rid_ = other.current_rid_;
  txn_ = other.txn_; // Transaction pointer is shallow copied.
  current_row_ = other.current_row_; // Row's copy constructor handles deep copy of fields.
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  // Iterators are equal if they belong to the same heap and point to the same RowId.
  return table_heap_ == itr.table_heap_ && current_rid_ == itr.current_rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  // Ensure the iterator is valid (not an end iterator and heap exists).
  ASSERT(table_heap_ != nullptr && current_rid_.GetPageId() != INVALID_PAGE_ID,
         "Dereferencing an invalid or end TableIterator.");
  // The current_row_ is assumed to be populated correctly by constructor or operator++.
  return current_row_;
}

Row *TableIterator::operator->() {
  ASSERT(table_heap_ != nullptr && current_rid_.GetPageId() != INVALID_PAGE_ID,
         "Dereferencing an invalid or end TableIterator with -> operator.");
  return &current_row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this == &itr) { // Handle self-assignment.
    return *this;
  }
  table_heap_ = itr.table_heap_;
  current_rid_ = itr.current_rid_;
  txn_ = itr.txn_;
  current_row_ = itr.current_row_; // Row's assignment operator handles deep copy.
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // If already at the end or associated with no heap, do nothing.
  if (table_heap_ == nullptr || current_rid_.GetPageId() == INVALID_PAGE_ID) {
    current_rid_.Set(INVALID_PAGE_ID, 0); // Ensure it's definitively an end iterator.
    return *this;
  }

  ASSERT(table_heap_->buffer_pool_manager_ != nullptr, "BufferPoolManager is null in TableHeap via iterator.");

  page_id_t page_id_of_curr_tuple = current_rid_.GetPageId();
  TablePage *current_page_obj = reinterpret_cast<TablePage *>(
      table_heap_->buffer_pool_manager_->FetchPage(page_id_of_curr_tuple));

  if (current_page_obj == nullptr) {
    LOG(ERROR) << "Iterator operator++: Failed to fetch current page " << page_id_of_curr_tuple;
    current_rid_.Set(INVALID_PAGE_ID, 0); // Critical error, cannot proceed.
    return *this;
  }

  RowId next_rid_candidate;
  // Try to find the next tuple on the current page.
  // TablePage::GetNextTupleRid is expected to skip logically deleted tuples.
  if (current_page_obj->GetNextTupleRid(current_rid_, &next_rid_candidate)) {
    current_rid_ = next_rid_candidate;
    current_row_.SetRowId(current_rid_); // Prepare current_row_ for GetTuple
    if (!table_heap_->GetTuple(&current_row_, txn_)) {
      // This implies the tuple found by GetNextTupleRid was not retrievable (e.g. deleted concurrently).
      // To be robust, the iterator should try to find the *next* valid one,
      // which means effectively calling ++ recursively or iterating further.
      // For simplicity in this step, we might mark as end or re-attempt ++.
      // A simple approach is to mark as end if GetTuple fails.
      // LOG(WARNING) << "Iterator operator++: GetTuple failed for RID " << current_rid_.Get() << " on same page.";
      current_rid_.Set(INVALID_PAGE_ID, 0); // Simplistic: fail and become end iterator.
    }
    table_heap_->buffer_pool_manager_->UnpinPage(page_id_of_curr_tuple, false); // Unpin, read-only for navigation.
    return *this;
  }

  // No more tuples on the current page. Unpin it and try to move to the next page.
  table_heap_->buffer_pool_manager_->UnpinPage(page_id_of_curr_tuple, false);
  page_id_t next_page_id_in_chain = current_page_obj->GetNextPageId();

  while (next_page_id_in_chain != INVALID_PAGE_ID) {
    current_page_obj = reinterpret_cast<TablePage *>(
        table_heap_->buffer_pool_manager_->FetchPage(next_page_id_in_chain));

    if (current_page_obj == nullptr) {
      LOG(ERROR) << "Iterator operator++: Failed to fetch next page " << next_page_id_in_chain;
      current_rid_.Set(INVALID_PAGE_ID, 0); // Critical error.
      return *this;
    }

    // Try to find the first valid tuple on this new page.
    if (current_page_obj->GetFirstTupleRid(&next_rid_candidate)) {
      current_rid_ = next_rid_candidate;
      current_row_.SetRowId(current_rid_);
      if (!table_heap_->GetTuple(&current_row_, txn_)) {
        // LOG(WARNING) << "Iterator operator++: GetTuple failed for first RID on new page " << current_rid_.Get();
        current_rid_.Set(INVALID_PAGE_ID, 0);
      }
      table_heap_->buffer_pool_manager_->UnpinPage(next_page_id_in_chain, false);
      return *this;
    }

    // This new page is also empty (or contains only deleted tuples). Unpin and get next page ID.
    page_id_t page_to_unpin = next_page_id_in_chain;
    next_page_id_in_chain = current_page_obj->GetNextPageId();
    table_heap_->buffer_pool_manager_->UnpinPage(page_to_unpin, false);
  }

  // Reached the end of all pages in the chain.
  current_rid_.Set(INVALID_PAGE_ID, 0); // Mark as end iterator.
  current_row_.destroy(); // Clear fields of the buffered row, as it's now an end iterator.
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this); // Create a copy of the iterator's state BEFORE incrementing.
  ++(*this);                 // Call the prefix increment operator on the original iterator.
  return TableIterator(temp);               // Return the copy (the state before it was incremented).
}