#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {
  // Initialize the clock hand to the beginning of the list.
  // Since the list is initially empty, it will be an end-iterator.
  clock_hand_ = clock_list.begin();
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  // If the replacer is empty, there are no frames to evict.
  if (clock_list.empty()) {
    return false;
  }

  // Loop indefinitely until a victim is found.
  while (true) {
    // If the clock hand reaches the end, wrap it around to the beginning.
    if (clock_hand_ == clock_list.end()) {
      clock_hand_ = clock_list.begin();
    }

    // Check the reference bit of the frame pointed to by the clock hand.
    // The value in clock_status map acts as the reference bit (1 for referenced, 0 for not).
    if (clock_status[*clock_hand_] == 1) {
      // If the reference bit is 1, set it to 0 and advance the hand.
      clock_status[*clock_hand_] = 0;
      clock_hand_++;
    } else {
      // If the reference bit is 0, this is our victim.
      // Set the output parameter to the victim's frame_id.
      *frame_id = *clock_hand_;
      // Remove the victim frame from the status map.
      clock_status.erase(*frame_id);
      // Erase the frame from the list. The erase method returns an iterator
      // to the next element, which becomes the new position for the clock hand.
      clock_hand_ = clock_list.erase(clock_hand_);
      return true;
    }
  }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  // Check if the frame is currently a candidate for replacement.
  if (clock_status.count(frame_id) == 0) {
    return; // Not in the replacer, so nothing to do.
  }

  // Find the iterator for the frame to be pinned.
  // This is an O(n) operation on std::list.
  for (auto it = clock_list.begin(); it != clock_list.end(); ++it) {
    if (*it == frame_id) {
      // If the clock hand is pointing to the frame we are about to pin,
      // we must advance the clock hand before erasing the element to avoid invalidating it.
      if (clock_hand_ == it) {
        // Erase the element and update the hand to point to the next element.
        clock_hand_ = clock_list.erase(it);
      } else {
        // If the hand is not pointing here, just erase the element.
        clock_list.erase(it);
      }
      // Since we found and erased the element, we can break the loop.
      break;
    }
  }

  // Remove the frame from the status map.
  clock_status.erase(frame_id);
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  // If the frame is already in the replacer, its reference bit should be set to 1.
  if (clock_status.count(frame_id)) {
    clock_status[frame_id] = 1;
    return;
  }

  // If the replacer is full, we don't add the new frame.
  // The Buffer Pool Manager is expected to call Victim() first.
  if (clock_list.size() >= capacity) {
    return;
  }

  // Add the new frame to the back of the list.
  clock_list.push_back(frame_id);
  // Mark it as present in the replacer and set its reference bit to 1 (recently unpinned).
  clock_status[frame_id] = 1;

  // If this is the first element added, initialize the clock hand to point to it.
  if (clock_list.size() == 1) {
      clock_hand_ = clock_list.begin();
  }
}

size_t CLOCKReplacer::Size() {
  // The size of the replacer is the number of frames it currently holds.
  return clock_list.size();
}