#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  char *current_ptr = buf;

  // 1. Write Magic Number
  MACH_WRITE_UINT32(current_ptr, SCHEMA_MAGIC_NUM);
  current_ptr += sizeof(uint32_t);

  // 2. Write number of columns
  uint32_t num_columns = static_cast<uint32_t>(columns_.size());
  MACH_WRITE_UINT32(current_ptr, num_columns);
  current_ptr += sizeof(uint32_t);

  // 3. Serialize each column
  for (const auto column : columns_) {
    current_ptr += column->SerializeTo(current_ptr);
  }

  // 4. Serialize is_manage_ flag
  MACH_WRITE_TO(bool, current_ptr, is_manage_);
  current_ptr += sizeof(bool);

  return static_cast<uint32_t>(current_ptr - buf);
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t total_size = 0;

  // Magic Number
  total_size += sizeof(uint32_t);
  // Number of columns
  total_size += sizeof(uint32_t);
  // Size of each column
  for (const auto column : columns_) {
    total_size += column->GetSerializedSize();
  }
  // is_manage_ flag
  total_size += sizeof(bool);

  return total_size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  if (schema != nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in schema deserialize.";
  }

  char *current_ptr = buf;

  // 1. Read and check Magic Number
  uint32_t magic_num = MACH_READ_UINT32(current_ptr);
  current_ptr += sizeof(uint32_t);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Schema magic number mismatch.");
  if (magic_num != SCHEMA_MAGIC_NUM) {
    LOG(ERROR) << "Schema magic number mismatch during deserialization.";
    return 0;
  }

  // 2. Read number of columns
  uint32_t num_columns = MACH_READ_UINT32(current_ptr);
  current_ptr += sizeof(uint32_t);

  // 3. Deserialize each column
  std::vector<Column *> deserialized_columns;
  deserialized_columns.reserve(num_columns);
  for (uint32_t i = 0; i < num_columns; ++i) {
    Column *col = nullptr;
    uint32_t col_bytes_read = Column::DeserializeFrom(current_ptr, col);
    if (col == nullptr || col_bytes_read == 0) {
      LOG(ERROR) << "Failed to deserialize column " << i << "for schema.";
      for (auto cleanup_col : deserialized_columns) {
        delete cleanup_col;
      }
      return 0;
    }
    deserialized_columns.push_back(col);
    current_ptr += col_bytes_read;
  }

  // 4. Read is_manage_ flag
  bool is_managed_from_buf = MACH_READ_FROM(bool, current_ptr);
  current_ptr += sizeof(bool);

  schema = new Schema(deserialized_columns, is_managed_from_buf);

  return static_cast<uint32_t>(current_ptr - buf);
}