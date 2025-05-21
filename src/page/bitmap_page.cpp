#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >= GetMaxSupportedSize()) {
    return false;  // No free pages left
  }

  uint32_t current_search_page = next_free_page_;
  // Loop 1: Search from next_free_page_ to the end
  for (; current_search_page < GetMaxSupportedSize(); ++current_search_page) {
    if (IsPageFree(current_search_page)) {
      uint32_t byte_index = current_search_page / 8;
      uint8_t bit_index = current_search_page % 8;
      bytes[byte_index] |= (1U << bit_index);
      page_allocated_++;
      page_offset = current_search_page;

      // Update next_free_page_ to the next actual free page
      if (page_allocated_ == GetMaxSupportedSize()) {
        next_free_page_ = GetMaxSupportedSize();
      } else {
        bool found_next = false;
        // Search from current_search_page + 1 to end
        for (uint32_t i = current_search_page + 1; i < GetMaxSupportedSize(); ++i) {
          if (IsPageFree(i)) {
            next_free_page_ = i;
            found_next = true;
            break;
          }
        }
        if (!found_next) {
          // Search from 0 to current_search_page
          for (uint32_t i = 0; i < current_search_page; ++i) {
            if (IsPageFree(i)) {
              next_free_page_ = i;
              found_next = true;
              break;
            }
          }
        }
        // If still not found (should not happen if page_allocated_ < GetMaxSupportedSize())
        // If all pages are full, the initial check handles it.
        // If there's at least one free page, one of the loops above *must* find it.
      }
      return true;
    }
  }

  // Loop 2: Search from 0 to next_free_page_ (if not found in Loop 1)
  for (current_search_page = 0; current_search_page < next_free_page_; ++current_search_page) {
    if (IsPageFree(current_search_page)) {
      uint32_t byte_index = current_search_page / 8;
      uint8_t bit_index = current_search_page % 8;
      bytes[byte_index] |= (1U << bit_index);
      page_allocated_++;
      page_offset = current_search_page;

      // Update next_free_page_ to the actual free page
      if (page_allocated_ == GetMaxSupportedSize()) {
        next_free_page_ = GetMaxSupportedSize();
      } else {
        bool found_next = false;
        for (uint32_t i = current_search_page + 1; i < GetMaxSupportedSize(); ++i) {
          if (IsPageFree(i)) {
            next_free_page_ = i;
            found_next = true;
            break;
          }
        }
        if (!found_next) {
          for (uint32_t i = 0; i < current_search_page; ++i) {
            if (IsPageFree(i)) {
              next_free_page_ = i;
              found_next = true;
              break;
            }
          }
        }
      }
      return true;
    }
  }
  return false;  // Should not be reached if page_allocated_ < GetMaxSupportedSize()
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;  // Page offset out of bounds
  }
  if (IsPageFree(page_offset)) {
    return false;  // Page is already free
  }

  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  bytes[byte_index] &= ~(1U << bit_index);  // Clear the bit to mark as free
  page_allocated_--;

  // A small optimization: if the deallocated page is earlier than the current hint,
  // it becomes the new best hint.
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }

  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;  // Consider out-of-bounds pages as not free
  }
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (bit_index >= 8) {
    // LOG(ERROR) << "IsPageFreeLow called with invalid bit_index: " << bit_index;
    return false;
  }
  // Check if the specific bit is 0 (free)
  // (1U << bit_index) creates a mask with the bit_index-th bit set.
  // If (bytes[byte_index] & mask) is 0, then that bit in bytes[byte_index] was 0.
  return (bytes[byte_index] & (1U << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;