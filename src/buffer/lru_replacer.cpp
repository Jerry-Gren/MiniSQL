#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (lru_list_.empty()) {
    // No frames are currently available for replacement.
    return false;
  }

  // The least recently used frame is at the back of the list.
  *frame_id = lru_list_.back();

  // Remove the victim frame from the map and the list.
  lru_map_.erase(*frame_id);
  lru_list_.pop_back();

  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto map_iterator = lru_map_.find(frame_id);
  if (map_iterator != lru_map_.end()) {
    // Frame is found in the replacer (i.e., it was unpinned).
    // Remove it from the list of replaceable frames.
    lru_list_.erase(map_iterator->second); // map_iterator->second is the std::list<frame_id_t>::iterator.
    lru_map_.erase(map_iterator);          // Erase from map using the map iterator for efficiency.
  }
  // If frame_id is not in lru_map_, it implies it was already pinned
  // or has never been unpinned into the replacer; no action is needed.
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // Check if the frame is already in the replacer (i.e., already unpinned).
  if (lru_map_.count(frame_id)) {
    // According to the test's expected behavior for the initial victims (1,2,3),
    // unpinning an already unpinned frame should NOT change its LRU status.
    // Therefore, if it's already in the map, we do nothing.
    return;
  }

  // If the frame is not in the replacer, it means its pin count just became zero.
  // Add it to the MRU position (front of the list).
  lru_list_.push_front(frame_id);
  // Store the iterator to this new element in the map.
  lru_map_[frame_id] = lru_list_.begin();
}

/**
 * TODO: Student Implement
 * @return the number of frames in the LRUReplacer that can be victimized.
 */
size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(latch_);
  // The size is the number of frames currently in our lru_list_.
  return lru_list_.size();
}