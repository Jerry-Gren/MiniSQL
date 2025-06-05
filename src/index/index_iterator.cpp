#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

/**
 * TODO: Student Implement
 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return std::make_pair(page->KeyAt(item_index), page->ValueAt(item_index));
}

/**
 * TODO: Student Implement
 */
IndexIterator &IndexIterator::operator++() {
  if (current_page_id == INVALID_PAGE_ID) return *this;
  if (item_index + 1 < page->GetSize()) item_index++;
  else {
    page_id_t next = page->GetNextPageId();
    if (next != INVALID_PAGE_ID) {
      buffer_pool_manager->UnpinPage(page->GetPageId(), false);
      page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(next)->GetData());
    }
    else page = nullptr;
    item_index = 0;
    current_page_id = next;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}