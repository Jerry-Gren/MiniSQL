#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  Page* root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  if (leaf_max_size == UNDEFINED_SIZE) leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(RowId));
  if (internal_max_size == UNDEFINED_SIZE) internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t));
  if (leaf_max_size_ <= 0) leaf_max_size_ = 1;
  if (internal_max_size_ <= 1) internal_max_size_ = 2;
  IndexRootsPage *root = reinterpret_cast<IndexRootsPage *>(root_page->GetData());
  root->GetRootId(index_id, &root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID) return true;
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()) return false;
  Page *leaf = FindLeafPage(key, root_page_id_, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage*>(leaf->GetData());
  RowId temp;
  bool found = leaf_page->Lookup(key, temp, processor_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  if (found) {
    result.push_back(temp);
  }
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t new_page_id;
  Page *root = buffer_pool_manager_->NewPage(new_page_id);
  if (root == nullptr) {
    throw ("Out of memory");
  }
  LeafPage *root_leaf = reinterpret_cast<LeafPage*>(root->GetData());
  int key_size = processor_.GetKeySize();
  root_leaf->Init(new_page_id, INVALID_PAGE_ID, key_size, leaf_max_size_);
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);
  root_leaf->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(new_page_id, false);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  Page *page = FindLeafPage(key, root_page_id_, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage*>(page->GetData());

  page_id_t leaf_page_id = leaf_page->GetPageId();
  int old_key_size = leaf_page->GetKeySize();
  int new_key_size  = leaf_page->Insert(key, value, processor_);
  if (new_key_size == old_key_size) {
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return false;
  }
  if (leaf_page->GetSize() > leaf_page->GetMaxSize() - 1) {
    LeafPage *new_page = Split(leaf_page, transaction);
    leaf_page->SetNextPageId(new_page->GetPageId());
    InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) throw("Out of memory");
  InternalPage *new_internal_page = reinterpret_cast<InternalPage*>(new_page->GetData());
  int size = processor_.GetKeySize();
  new_internal_page->Init(new_page_id, node->GetParentPageId(), size, internal_max_size_);
  node->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  return new_internal_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) throw("Out of memory");
  LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  int size = processor_.GetKeySize();
  new_leaf_page->Init(new_page_id, node->GetParentPageId(), size, leaf_max_size_);
  node->MoveHalfTo(new_leaf_page);
  return new_leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
//有问题
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {
    Page *new_root = buffer_pool_manager_->NewPage(root_page_id_);
    if (new_root == nullptr) throw("Out of memory");
    InternalPage *new_page = reinterpret_cast<InternalPage *>(new_root->GetData());
    int size = processor_.GetKeySize();
    new_page->Init(root_page_id_, INVALID_PAGE_ID, size, internal_max_size_);
    new_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    UpdateRootPageId(0);
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  page_id_t parent_root_id = old_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_root_id);
  InternalPage *new_page = reinterpret_cast<InternalPage*>(parent_page->GetData());
  int size = new_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (size > new_page->GetMaxSize()) {
    InternalPage *new_parent_sibling = Split(new_page, transaction);
    InsertIntoParent(new_page, new_parent_sibling->KeyAt(0), new_parent_sibling, transaction);
    buffer_pool_manager_->UnpinPage(new_parent_sibling->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {//还要改
  if (IsEmpty()) return;
  Page *leaf = FindLeafPage(key, root_page_id_, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage*>(leaf->GetData());
  page_id_t leaf_page_id = leaf_page->GetPageId();

  bool is_delete = false;
  int index = leaf_page->KeyIndex(key, processor_);
  int new_size = leaf_page->RemoveAndDeleteRecord(key, processor_);

  if (new_size < leaf_page->GetMaxSize()) {
    is_delete = CoalesceOrRedistribute<LeafPage>(leaf_page, transaction);
  }
  else if (!index) {
    GenericKey *new_key = leaf_page->KeyAt(0); // New key to replace the old one.
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId())->GetData());
    page_id_t page_id = leaf_page->GetPageId();

    if(parent_page!=nullptr){
      while (!parent_page->IsRootPage() && parent_page->ValueIndex(page_id) == 0) {
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
        page_id=parent_page->GetPageId();
        parent_page = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(parent_page->GetParentPageId())->GetData()
        );
      }
      int tmp = parent_page->ValueIndex(page_id);
      if (tmp != 0 && processor_.CompareKeys(parent_page->KeyAt(tmp), new_key) != 0) {
        parent_page->SetKeyAt(tmp, new_key);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      }
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  if(is_delete) buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (node->GetSize() >= node->GetMinSize()) return false;
  if (node->IsRootPage()) return AdjustRoot(node);

  page_id_t parent_page_id = node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  if (parent_page == nullptr) return false;
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

  int index = parent_node->ValueIndex(node->GetPageId());

  page_id_t neighbor_id;
  if (index == 0) neighbor_id = parent_node->ValueAt(1);
  else neighbor_id = parent_node->ValueAt(index - 1);
  Page* neighbor_page = buffer_pool_manager_->FetchPage(neighbor_id);
  N* neighbor = reinterpret_cast<N*>(neighbor_page->GetData());

  bool flag = false;
  if (neighbor->GetSize() + node->GetSize() <= neighbor->GetMaxSize()) {
    flag = Coalesce(neighbor, node, parent_node, index, transaction);
    buffer_pool_manager_->DeletePage(node->GetPageId());
  }
  else Redistribute(neighbor, node, index);

  buffer_pool_manager_->UnpinPage(neighbor_id, 1);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), 1);
  if (flag) return CoalesceOrRedistribute(parent_node, transaction);
  return 0;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index != 0) {
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    parent->Remove(index);
    if (parent->GetSize() >= parent->GetMinSize()) {
      buffer_pool_manager_->DeletePage(node->GetPageId());
      return false;
    }
    return CoalesceOrRedistribute<InternalPage>(parent, transaction);
  }
  else {
    neighbor_node->MoveAllTo(node);
    node->SetNextPageId(neighbor_node->GetNextPageId());
    parent->Remove(1);
    if (parent->GetSize() >= parent->GetMinSize()) {
      buffer_pool_manager_->DeletePage(node->GetPageId());
      return false;
    }
    return CoalesceOrRedistribute<InternalPage>(parent, transaction);
  }
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  parent->Remove(index);
  if (parent->GetSize() >= parent->GetMinSize()) return false;
  return CoalesceOrRedistribute<InternalPage>(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if(parent_page == nullptr) return;
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

  if (index != 0) {
    neighbor_node->MoveLastToFrontOf(node);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }
  else {
    neighbor_node->MoveFirstToEndOf(node);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if(parent_page == nullptr) return;
  InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

  if (index != 0) {
    GenericKey* new_key = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
    neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(parent_node->ValueIndex(neighbor_node->GetPageId())), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), new_key);
  }
  else {
    GenericKey* new_key = neighbor_node->KeyAt(1);
    neighbor_node->MoveFirstToEndOf(node, parent_node->KeyAt(parent_node->ValueIndex(neighbor_node->GetPageId())), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), new_key);
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  page_id_t old_root_id = old_root_node->GetPageId();
  if (!old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 1) {
      InternalPage* old_root=reinterpret_cast<InternalPage*>(old_root_node);
      root_page_id_=old_root->RemoveAndReturnOnlyChild();
      UpdateRootPageId();
      Page* new_root_page=buffer_pool_manager_->FetchPage(root_page_id_);
      BPlusTreePage*new_root=reinterpret_cast<BPlusTreePage*>(new_root_page->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_,true);//因为新根改了parent
      buffer_pool_manager_->UnpinPage(old_root_id,false);
      buffer_pool_manager_->DeletePage(old_root_id);
      return true;
    }
  }
  else if (old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->DeletePage(old_root_id);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  if (IsEmpty()) return End();
  Page *left = FindLeafPage(nullptr, INVALID_PAGE_ID, true);
  if (left == nullptr) return IndexIterator();
  buffer_pool_manager_->UnpinPage(left->GetPageId(), false);
  return IndexIterator(left->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  if (IsEmpty()) return End();
  Page *left = FindLeafPage(key, INVALID_PAGE_ID, false);
  if (left == nullptr) return IndexIterator();
  LeafPage *leaf = reinterpret_cast<LeafPage*>(left->GetData());
  buffer_pool_manager_->UnpinPage(left->GetPageId(), false);
  return IndexIterator(left->GetPageId(), buffer_pool_manager_, leaf->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (IsEmpty()) return nullptr;

  if (page_id == INVALID_PAGE_ID) page_id = root_page_id_;

  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) return nullptr;

  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(node);
    page_id_t next_page_id;
    if (leftMost) next_page_id = internal_node->ValueAt(0);
    else next_page_id = internal_node->Lookup(key, processor_);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = next_page_id;
    page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
      return nullptr;
    }
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *header = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage* root_page = reinterpret_cast<IndexRootsPage*>(header->GetData());
  if (insert_record != 0) root_page->Insert(index_id_, root_page_id_);
  else root_page ->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}