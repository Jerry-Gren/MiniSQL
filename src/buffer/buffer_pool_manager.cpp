#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

// static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

frame_id_t BufferPoolManager::TryToFindFreePage() {
  frame_id_t frame_id = INVALID_FRAME_ID;

  // 1. Try to get a frame from the free list
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // The page in this frame should already be reset or was never used.
    // Its pin_count should be 0.
    return frame_id;
  }

  // 2. If free list is empty, try to get a victim from the replacer
  if (replacer_->Victim(&frame_id)) {
    // A victim frame was found
    Page* victim_page = &pages_[frame_id];
    page_id_t old_page_id = victim_page->GetPageId();

    // If the victim page is dirty, write it back to disk
    if (victim_page->IsDirty()) {
      disk_manager_->WritePage(old_page_id, victim_page->GetData());
      // Note: is_dirty_ will be reset below or by the caller
    }

    // Remove the mapping of the old page from the page table
    page_table_.erase(old_page_id);

    // Reset the victim page's metadata for reuse
    // Pin count is already 0 (otherwise it wouldn't be in replacer)
    victim_page->page_id_ = INVALID_PAGE_ID;
    victim_page->pin_count_ = 0;
    victim_page->is_dirty_ = false;
    victim_page->ResetMemory(); // Zero out the page data

    return frame_id;
  }

  // 3. No frame available from free list or replacer
  return INVALID_FRAME_ID; // Indicates no frame could be found
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return nullptr; // Cannot fetch an invalid page ID
  }

  // 1. Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 1.1 If P exists, pin it and return it immediately.
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    // If the page was in the replacer (pin_count was 0), Pin it to remove.
    // LRUReplacer's Pin handles the case where it might not be in the replacer.
    replacer_->Pin(frame_id);
    return page;
  }

  // 1.2 If P does not exist, find a replacement frame (R_frame_id)
  frame_id_t R_frame_id = TryToFindFreePage();

  if (R_frame_id == INVALID_FRAME_ID) { // Check for invalid frame_id
    // No frame is available (free list empty and no victim found)
    return nullptr;
  }

  // A frame R_frame_id is now available.
  // Old page in R_frame_id (if any) was handled by TryToFindFreePage (flushed, removed from page_table, metadata reset).
  Page *page_in_frame = &pages_[R_frame_id];

  // 3. Update page table for the new page P using frame R_frame_id.
  page_table_[page_id] = R_frame_id;

  // 4. Update P's metadata, read in the page content from disk.
  page_in_frame->page_id_ = page_id;
  page_in_frame->pin_count_ = 1;    // Fetched and pinned
  page_in_frame->is_dirty_ = false; // Freshly read from disk
  // page_in_frame->ResetMemory(); // TryToFindFreePage already does this if victimizing

  disk_manager_->ReadPage(page_id, page_in_frame->GetData());

  // The frame is now pinned, so ensure it's not in the replacer.
  replacer_->Pin(R_frame_id);

  return page_in_frame;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id_out) { // Renamed param for clarity
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  // 2. Pick a victim frame P_frame_id from either the free list or the replacer.
  frame_id_t P_frame_id = TryToFindFreePage();

  // 1. If all the pages in the buffer pool are pinned (no frame found), return nullptr.
  if (P_frame_id == INVALID_FRAME_ID) { // Check for invalid frame_id
    page_id_out = INVALID_PAGE_ID;
    return nullptr;
  }

  // A frame P_frame_id is available.
  Page *new_page_in_frame = &pages_[P_frame_id];
  // Old page in P_frame_id (if any) was handled by TryToFindFreePage.

  // 0. Make sure you call AllocatePage! (BPM's private AllocatePage)
  page_id_t new_logical_page_id = AllocatePage();

  if (new_logical_page_id == INVALID_PAGE_ID) {
    // DiskManager could not allocate a new page (e.g., disk full).
    // The frame P_frame_id needs to be returned to a usable state.
    // Since it was prepared by TryToFindFreePage, it's clean and unmapped.
    // Add it back to the free_list.
    new_page_in_frame->page_id_ = INVALID_PAGE_ID; // Ensure it's reset
    new_page_in_frame->pin_count_ = 0;
    new_page_in_frame->is_dirty_ = false;
    // new_page_in_frame->ResetMemory(); // Already done by TryToFindFreePage if victim
    free_list_.push_back(P_frame_id); // Return to free list

    page_id_out = INVALID_PAGE_ID;
    return nullptr;
  }

  // 3. Update P's metadata, zero out memory and add P to the page table.
  page_table_[new_logical_page_id] = P_frame_id;

  new_page_in_frame->page_id_ = new_logical_page_id;
  new_page_in_frame->pin_count_ = 1;    // New page is immediately pinned
  new_page_in_frame->is_dirty_ = true;  // New content (zeros) is different from uninitialized disk page
  new_page_in_frame->ResetMemory();     // Zero out the page data as per hint

  // 4. Set the page ID output parameter.
  page_id_out = new_logical_page_id;

  // The frame is now pinned, ensure it's not in the replacer.
  replacer_->Pin(P_frame_id);

  return new_page_in_frame;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return true;
  }

  // 1. Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);

  if (it == page_table_.end()) {
    // 1.1 If P does not exist in buffer pool, still try to deallocate from disk.
    DeallocatePage(page_id); // Call BPM's private DeallocatePage
    return true;
  }

  // P exists in the buffer pool.
  frame_id_t frame_id = it->second;
  Page *page_to_delete = &pages_[frame_id];

  // 2. If P exists, but has a non-zero pin-count, return false.
  if (page_to_delete->GetPinCount() > 0) {
    return false; // Someone is using the page.
  }

  // 3. Otherwise, P can be deleted. (pin_count is 0)
  // Remove P from the page table.
  page_table_.erase(page_id);

  // Since pin_count is 0, it should be in the replacer. Remove it.
  // Pinning it in replacer effectively removes it from candidate list.
  replacer_->Pin(frame_id);

  // Reset its metadata.
  page_to_delete->page_id_ = INVALID_PAGE_ID;
  page_to_delete->pin_count_ = 0; // Should already be 0
  page_to_delete->is_dirty_ = false; // Content is being discarded, no need to flush
  page_to_delete->ResetMemory();

  // Return it to the free list.
  free_list_.push_back(frame_id);

  // 0. Make sure you call DeallocatePage! (BPM's private DeallocatePage)
  DeallocatePage(page_id);

  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // Page not found in buffer pool.
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (page->GetPinCount() <= 0) {
    // Cannot unpin a page with pin_count <= 0.
    return false;
  }

  page->pin_count_--;

  if (is_dirty) {
    page->is_dirty_ = true;
  }
  // Alternative: page->is_dirty_ = page->is_dirty_ || is_dirty; (safer if page could already be dirty)
  // The above line `page->is_dirty_ = true;` if is_dirty is true implies `is_dirty` sets it regardless of prior state.
  // Let's use the safer ORing:
  // page->is_dirty_ = page->is_dirty_ || is_dirty;
  // However, typical use is `UnpinPage(page_id, true)` means "I dirtied it".
  // `UnpinPage(page_id, false)` means "I read it, didn't dirty it".
  // So `if (is_dirty) page->is_dirty_ = true;` is fine. If it was already dirty, it remains dirty.
  // If it was clean and is_dirty is true, it becomes dirty.
  // If it was clean and is_dirty is false, it remains clean.

  if (page->GetPinCount() == 0) {
    // If pin_count reaches 0, the page becomes a candidate for replacement.
    replacer_->Unpin(frame_id);
  }

  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false; // Cannot flush an invalid page ID.
  }

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // Page not found in buffer pool.
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  // Write the page content to disk using DiskManager.
  disk_manager_->WritePage(page->GetPageId(), page->GetData());

  // After flushing, the page is no longer dirty.
  page->is_dirty_ = false;

  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}