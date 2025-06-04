#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  char *current_ptr = buf;

  // 1. Write Magic Number
  MACH_WRITE_UINT32(current_ptr, COLUMN_MAGIC_NUM);
  current_ptr += sizeof(uint32_t);

  // 2. Write column name length and name
  uint32_t name_len = static_cast<uint32_t>(name_.length());
  MACH_WRITE_UINT32(current_ptr, name_len);
  current_ptr += sizeof(uint32_t);
  MACH_WRITE_STRING(current_ptr, name_);
  current_ptr += name_len;

  // 3. Write TypeId (as uint32_t for defined size)
  MACH_WRITE_TO(uint32_t, current_ptr, static_cast<uint32_t>(type_));
  current_ptr += sizeof(uint32_t);

  // 4. Write length (len_)
  MACH_WRITE_UINT32(current_ptr, len_);
  current_ptr += sizeof(uint32_t);

  // 5. Write table index (table_ind_)
  MACH_WRITE_UINT32(current_ptr, table_ind_);
  current_ptr += sizeof(uint32_t);

  // 6. Write nullable (as bool, typically 1 byte)
  MACH_WRITE_TO(bool, current_ptr, nullable_);
  current_ptr += sizeof(bool);

  // 7. Write unique (as bool)
  MACH_WRITE_TO(bool, current_ptr, unique_);
  current_ptr += sizeof(bool);

  return static_cast<uint32_t>(current_ptr - buf);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size = 0;
  // Magic number
  size += sizeof(uint32_t);
  // Column name (length + string)
  size += sizeof(uint32_t) + static_cast<uint32_t>(name_.length());
  // TypeId (serialized as uint32_t)
  size += sizeof(uint32_t);
  // Length (len_)
  size += sizeof(uint32_t);
  // Table index (table_ind_)
  size += sizeof(uint32_t);
  // Nullable (bool)
  size += sizeof(bool);
  // Unique (bool)
  size += sizeof(bool);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
  }

  char *current_ptr = buf;

  // 1. Read and check Magic Number
  uint32_t magic_num = MACH_READ_UINT32(current_ptr);
  current_ptr += sizeof(uint32_t);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Column magic number mismatch.");  // Only for Debugging
  if (magic_num != COLUMN_MAGIC_NUM) {
    LOG(ERROR) << "Column magic number mismatch during deserialization.";
    return 0;
  }

  // 2. Read column name length and name
  uint32_t name_len = MACH_READ_UINT32(current_ptr);
  current_ptr += sizeof(uint32_t);
  std::string column_name(current_ptr, name_len);
  current_ptr += name_len;

  // 3. Read TypeId
  TypeId type = static_cast<TypeId>(MACH_READ_UINT32(current_ptr));
  current_ptr += sizeof(uint32_t);

  // 4. Read length (col_len)
  uint32_t col_len = MACH_READ_UINT32(current_ptr);
  current_ptr += sizeof(uint32_t);

  // 5. Read table index (col_ind)
  uint32_t col_ind = MACH_READ_UINT32(current_ptr);
  current_ptr += sizeof(uint32_t);

  // 6. Read nullable
  bool nullable = MACH_READ_FROM(bool, current_ptr);
  current_ptr += sizeof(bool);

  // 7. Read unique
  bool unique = MACH_READ_FROM(bool, current_ptr);
  current_ptr += sizeof(bool);

  // Allocate object based on type
  if (type == TypeId::kTypeChar) {
    column = new Column(column_name, type, col_len, col_ind, nullable, unique);
  } else {
    column = new Column(column_name, type, col_ind, nullable, unique);
  }
  return static_cast<uint32_t>(current_ptr - buf);
}
