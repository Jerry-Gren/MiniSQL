#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  char *current_ptr = buf;
  uint32_t num_columns = schema->GetColumnCount();

  // 1. Serialize Header: Field Nums
  MACH_WRITE_UINT32(current_ptr, num_columns);
  current_ptr += sizeof(uint32_t);

  // 2. Serialize Header: Null Bitmap
  uint32_t null_bitmap_byte_size = (num_columns + 7) / 8; // Align to bytes
  if (num_columns > 0) {
    std::vector<char> null_bitmap(null_bitmap_byte_size, 0);

    for (uint32_t i = 0; i < num_columns; i++) {
      if (fields_[i] == nullptr || fields_[i]->IsNull()) {
        uint32_t byte_index = i / 8;
        uint32_t bit_index = i % 8;
        null_bitmap[byte_index] |= (1 << bit_index);
      }
    }
    memcpy(current_ptr, null_bitmap.data(), null_bitmap_byte_size);
    current_ptr += null_bitmap_byte_size;
  }

  // 3. Serialize Fields Data
  for (uint32_t i = 0; i < num_columns; i++) {
    ASSERT(fields_[i] != nullptr, "Field pointer itself should not be null if schema expects a field.");
    current_ptr += fields_[i]->SerializeTo(current_ptr);
  }

  return static_cast<uint32_t>(current_ptr - buf);
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  char *current_ptr = buf;

  // 1. Deserialize Header: Field Nums
  uint32_t num_columns_from_buf = MACH_READ_UINT32(current_ptr);
  current_ptr += sizeof(uint32_t);

  ASSERT(num_columns_from_buf == schema->GetColumnCount(), "Deserialized column count mismatch with schema.");
  if (num_columns_from_buf != schema->GetColumnCount()) {
    LOG(ERROR) << "Row deserialization error: column count from buffer (" << num_columns_from_buf
               << ") does not match schema (" << schema->GetColumnCount() << ").";
    return 0;
  }

  fields_.clear();  // For safety
  fields_.reserve(num_columns_from_buf);

  // 2. Deserialize Header: Null Bitmap
  uint32_t null_bitmap_byte_size = (num_columns_from_buf + 7) / 8;
  std::vector<char> null_bitmap_data;
  if (num_columns_from_buf > 0) {
    null_bitmap_data.resize(num_columns_from_buf);
    memcpy(null_bitmap_data.data(), current_ptr, null_bitmap_byte_size);
    current_ptr += null_bitmap_byte_size;
  }

  // 3. Deserialize Field Data
  //    Only done when num_columns_from_buf > 0
  for (uint32_t i = 0; i < num_columns_from_buf; i++) {
    bool is_null = false;
    uint32_t byte_index = i / 8;
    uint32_t bit_index = i % 8;
    if (byte_index < null_bitmap_data.size()) {
      is_null = (null_bitmap_data[byte_index] & (1 << bit_index));
    } else {
      // This should not happen if num_columns_from_buf and null_bitmap_byte_size are consistent
      LOG(ERROR) << "Null bitmap access out of bounds during row deserialization.";
      for (Field *field : fields_) { delete field; }
      fields_.clear();
      return 0;
    }

    TypeId type_id = schema->GetColumn(i)->GetType();
    Field *field = nullptr;
    uint32_t field_bytes_read = Field::DeserializeFrom(current_ptr, type_id, &field, is_null);
    if (field == nullptr) {
      // This should not happen
      LOG(ERROR) << "Field deserialization returned a null field pointer.";
      for (Field *field : fields_) { delete field; }
      fields_.clear();
      return 0;
    }
    fields_.push_back(field);
    current_ptr += field_bytes_read;
  }

  return static_cast<uint32_t>(current_ptr - buf);
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  if (fields_.empty() && schema->GetColumnCount() == 0) {
    return sizeof(uint32_t);  // Only for field_nums = 0, no bitmap
  }

  uint32_t total_size = 0;
  uint32_t num_columns = static_cast<uint32_t>(schema->GetColumnCount());

  // Header: Field Nums
  total_size += sizeof(uint32_t);
  // Header: Null Bitmap
  if (num_columns > 0) {
    total_size += (num_columns + 7) / 8;
  }
  // Field Data
  for (const auto field : fields_) {
    ASSERT(field != nullptr, "Field pointer itself should not be null.");
    total_size += field->GetSerializedSize();
  }

  return total_size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
