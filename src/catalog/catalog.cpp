#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t size = sizeof (uint32_t) * 3;
  size += table_meta_pages_.size() * (sizeof (table_id_t) + sizeof (page_id_t));
  size += index_meta_pages_.size() * (sizeof (page_id_t) + sizeof (index_id_t));
  return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) catalog_meta_ = new CatalogMeta();
  else {
    Page* meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }

  for (auto &pair : catalog_meta_->table_meta_pages_) {
    LoadTable(pair.first, pair.second);
  }
  next_table_id_ = catalog_meta_->GetNextTableId();

  for (auto &pair : catalog_meta_->index_meta_pages_) {
    LoadIndex(pair.first, pair.second);
  }
  next_index_id_ = catalog_meta_->GetNextIndexId();
  FlushCatalogMetaPage();
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.count(table_name) != 0) return DB_TABLE_ALREADY_EXIST;

  page_id_t page_id;
  Page* page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) return DB_FAILED;

  TableSchema *schema_copy = Schema::DeepCopySchema(schema);
  table_id_t table_id = next_table_id_.fetch_add(1);// CatalogManager拥有Schema的拷贝
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema_copy, txn, log_manager_, lock_manager_);
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, page_id, schema_copy);
  table_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);

  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap); // table_meta 所有权转移

  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_name, table_id);
  catalog_meta_->table_meta_pages_.emplace(table_id, page_id);

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto it_name = table_names_.find(table_name);
  if (it_name == table_names_.end()) {
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = it_name->second;

  auto it_id = tables_.find(table_id);
  if (it_id == tables_.end()) {
    table_info = nullptr;
    return DB_FAILED;
  }
  table_info = it_id->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto pair : tables_) tables.push_back(pair.second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;
  if (index_names_[table_name].count(index_name) != 0) return DB_INDEX_ALREADY_EXIST;

  std::vector<uint32_t> key_map;
  TableInfo *table_info = tables_[table_names_[table_name]];
  const TableSchema *table_schema = table_info->GetSchema();
  for (const auto &key_col_name : index_keys) {
    uint32_t column_index;
    if (table_schema->GetColumnIndex(key_col_name, column_index) == DB_SUCCESS) {
      key_map.push_back(column_index);
    }
    else return DB_COLUMN_NAME_NOT_EXIST;
  }
  if (key_map.empty()) return DB_FAILED;

  page_id_t page_id;
  Page* page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) return DB_FAILED;

  index_id_t new_index_id = next_index_id_.fetch_add(1);
  IndexMetadata *index_meta = IndexMetadata::Create(new_index_id, index_name, table_info->GetTableId(), key_map);
  if (index_meta == nullptr) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      buffer_pool_manager_->DeletePage(page_id);
      return DB_FAILED;
  }
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  index_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);

  catalog_meta_->index_meta_pages_.emplace(new_index_id, page_id);
  indexes_.emplace(new_index_id, index_info);
  index_names_[table_name].emplace(index_name, new_index_id);

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto tb_name = table_names_.find(table_name);
  if (tb_name == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it_name = index_names_.at(table_name).find(index_name);
  if (it_name == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;
  auto id_name = index_names_.find(table_name)->second.find(index_name);
  auto id_id = id_name->second;
  index_info = indexes_.find(id_id)->second;
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto id_name = index_names_.find(table_name);
  if (id_name == index_names_.end()) return DB_TABLE_NOT_EXIST;
  for (auto pair : index_names_.find(table_name)->second) {
    indexes.push_back(indexes_.find(pair.second)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.count(table_name) == 0) return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];

  std::vector<IndexInfo *> indexes_on_table;
  GetTableIndexes(table_name, indexes_on_table);
  for (auto *index_info : indexes_on_table) {
    DropIndex(table_name, index_info->GetIndexName());
  }

  page_id_t table_meta_page_id = catalog_meta_->table_meta_pages_[table_id];
  catalog_meta_->table_meta_pages_.erase(table_id);
  buffer_pool_manager_->DeletePage(table_meta_page_id);

  TableInfo *table_to_delete = tables_[table_id];
  table_to_delete->GetTableHeap()->FreeTableHeap();
  delete table_to_delete;
  tables_.erase(table_id);
  table_names_.erase(table_name);

  return FlushCatalogMetaPage();
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.count(table_name) == 0) return DB_INDEX_NOT_FOUND;
  if (index_names_[table_name].count(index_name) == 0) return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_[table_name][index_name];

  page_id_t index_meta_page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  buffer_pool_manager_->DeletePage(index_meta_page_id);

  delete indexes_[index_id];
  indexes_.erase(index_id);
  index_names_[table_name].erase(index_name);

  return FlushCatalogMetaPage();
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if (catalog_meta_ == nullptr) return DB_FAILED;
  Page* page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (page == nullptr) return DB_FAILED;

  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.count(table_id)) return DB_TABLE_ALREADY_EXIST;
  Page* table_meta_page = buffer_pool_manager_->FetchPage(page_id);
  if (table_meta_page == nullptr) return DB_FAILED;
  TableMetadata *table = nullptr;
  TableMetadata::DeserializeFrom(table_meta_page->GetData(), table);
  buffer_pool_manager_->UnpinPage(page_id, false);
  if (table == nullptr) return DB_FAILED;

  TableSchema *schema = table->GetSchema();
  TableHeap *heap = TableHeap::Create(buffer_pool_manager_, table->GetFirstPageId(), schema, log_manager_, lock_manager_);

  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table, heap);

  tables_[table_id] = table_info;
  table_names_[table->GetTableName()] = table_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.count(index_id)) return DB_TABLE_ALREADY_EXIST;
  Page* index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  if (index_meta_page == nullptr) return DB_FAILED;
  IndexMetadata *index = nullptr;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index);
  buffer_pool_manager_->UnpinPage(page_id, false);
  if (index == nullptr) return DB_FAILED;

  IndexInfo *index_info = IndexInfo::Create();
  IndexMetadata *index_meta = nullptr;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
  TableInfo *table_info = tables_[index_meta->GetIndexId()];
  index_info->Init(index, table_info, buffer_pool_manager_);

  indexes_[index_id] = index_info;
  index_names_[table_info->GetTableName()][index->GetIndexName()] = index_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto it = tables_.find(table_id);
  if (it == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it->second;
  return DB_SUCCESS;
}