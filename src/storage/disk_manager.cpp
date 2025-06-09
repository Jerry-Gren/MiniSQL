#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  if (meta_page->GetAllocatedPages() >= MAX_VALID_PAGE_ID) {
    LOG(WARNING) << "Cannot allocate page. Database is full. Allocated pages: "
                 << meta_page->GetAllocatedPages() << ", Max valid pages: " << MAX_VALID_PAGE_ID;
    return INVALID_PAGE_ID;
  }

  // Try to find a free page in existing extents
  for (uint32_t extent_id = 0; extent_id < meta_page->GetExtentNums(); ++extent_id) {
    if (meta_page->GetExtentUsedPage(extent_id) < BITMAP_SIZE) {
      page_id_t bitmap_physical_page_id = 1 + extent_id * (1 + BITMAP_SIZE);
      char page_buffer[PAGE_SIZE];
      ReadPhysicalPage(bitmap_physical_page_id, page_buffer);
      auto bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_buffer);

      uint32_t page_offset_in_bitmap;
      if (bitmap->AllocatePage(page_offset_in_bitmap)) {
        WritePhysicalPage(bitmap_physical_page_id, page_buffer);
        meta_page->num_allocated_pages_++;
        meta_page->extent_used_page_[extent_id]++;
        // WritePhysicalPage(META_PAGE_ID, meta_data_); // MetaData is flushed on Close()
        return static_cast<page_id_t>(extent_id * BITMAP_SIZE + page_offset_in_bitmap);
      }
    }
  }

  // If no space in existing extents, try to create a new extent
  uint32_t current_num_extents = meta_page->GetExtentNums();
  // Calculate max extents DiskFileMetaPage can hold
  // offsetof(DiskFileMetaPage, extent_used_page_) is typically 8 bytes for the two uint32_t members before it.
  uint32_t max_extents_possible = (PAGE_SIZE - offsetof(DiskFileMetaPage, extent_used_page_)) / sizeof(uint32_t);


  if (current_num_extents < max_extents_possible) {
    uint32_t new_extent_id = current_num_extents;
    page_id_t bitmap_physical_page_id = 1 + new_extent_id * (1 + BITMAP_SIZE);

    char page_buffer[PAGE_SIZE];
    std::memset(page_buffer, 0, PAGE_SIZE); // Initialize new bitmap page to all zeros (all free)
    auto bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_buffer);
    
    uint32_t page_offset_in_bitmap;
    if (bitmap->AllocatePage(page_offset_in_bitmap)) { // Should allocate the first page (offset 0)
      WritePhysicalPage(bitmap_physical_page_id, page_buffer);

      meta_page->num_extents_++;
      meta_page->num_allocated_pages_++;
      meta_page->extent_used_page_[new_extent_id] = 1;
      // WritePhysicalPage(META_PAGE_ID, meta_data_); // MetaData is flushed on Close()
      return static_cast<page_id_t>(new_extent_id * BITMAP_SIZE + page_offset_in_bitmap);
    } else {
      LOG(ERROR) << "Failed to allocate page in a brand new bitmap page. This should not happen.";
      return INVALID_PAGE_ID;
    }
  } else {
    LOG(WARNING) << "Cannot allocate page. DiskFileMetaPage cannot hold more extents. Current extents: "
                 << current_num_extents << ", Max extents: " << max_extents_possible;
  }

  return INVALID_PAGE_ID; // No page could be allocated
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  if (logical_page_id < 0) { // Or >= MAX_VALID_PAGE_ID potentially, but IsPageFree should handle state
    LOG(ERROR) << "Attempting to deallocate invalid logical_page_id: " << logical_page_id;
    return;
  }

  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset_in_bitmap = logical_page_id % BITMAP_SIZE;

  if (extent_id >= meta_page->GetExtentNums()) {
    LOG(ERROR) << "Attempting to deallocate page " << logical_page_id
               << " from non-existent extent_id " << extent_id;
    return;
  }

  page_id_t bitmap_physical_page_id = 1 + extent_id * (1 + BITMAP_SIZE);
  char page_buffer[PAGE_SIZE];
  ReadPhysicalPage(bitmap_physical_page_id, page_buffer);
  auto bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_buffer);

  if (bitmap->DeAllocatePage(page_offset_in_bitmap)) {
    WritePhysicalPage(bitmap_physical_page_id, page_buffer);
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[extent_id]--;
    // WritePhysicalPage(META_PAGE_ID, meta_data_); // MetaData is flushed on Close()
  } else {
    LOG(ERROR) << "Failed to deallocate logical page " << logical_page_id
               << " (offset " << page_offset_in_bitmap << " in extent " << extent_id
               << "). Page might have already been free or other error.";
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_); // Read operation, but involves disk read
  auto meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  if (logical_page_id < 0) return false; // Invalid pages are not "free" in a usable sense

  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset_in_bitmap = logical_page_id % BITMAP_SIZE;

  if (extent_id >= meta_page->GetExtentNums()) {
    // This logical page belongs to an extent that hasn't been created yet.
    // Therefore, it's not allocated, so it's considered free.
    return true;
  }

  page_id_t bitmap_physical_page_id = 1 + extent_id * (1 + BITMAP_SIZE);
  char page_buffer[PAGE_SIZE];
  ReadPhysicalPage(bitmap_physical_page_id, page_buffer);
  auto bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(page_buffer);

  return bitmap->IsPageFree(page_offset_in_bitmap);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  // Page 0: Disk Meta Page
  // Extent k (0-indexed):
  //   Bitmap Page: Physical Page 1 + k * (1 + BITMAP_SIZE)
  //   Data Pages Start: Physical Page 1 + k * (1 + BITMAP_SIZE) + 1

  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset_in_logical_extent = logical_page_id % BITMAP_SIZE;

  page_id_t physical_page_id = 1 + extent_id * (1 + BITMAP_SIZE) + 1 + page_offset_in_logical_extent;
  // This simplifies to:
  // physical_page_id = extent_id * (BITMAP_SIZE + 1) + 2 + page_offset_in_logical_extent;

  return physical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
