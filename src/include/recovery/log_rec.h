#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  lsn_t prev_lsn_{INVALID_LSN}; // Previous LSN for the same transaction
  txn_id_t txn_id_{INVALID_TXN_ID};

  // Fields for specific log types
  // For kInsert
  KeyType ins_key_;
  ValType ins_val_;

  // For kDelete
  KeyType del_key_;
  ValType del_val_; // The value that was deleted

  // For kUpdate
  KeyType old_key_;
  ValType old_val_;
  KeyType new_key_;
  ValType new_val_;


  LogRec() = default; // Default constructor

  // Constructor for BEGIN, COMMIT, ABORT logs (unique signature)
  LogRec(LogRecType type, txn_id_t txn_id) : type_(type), txn_id_(txn_id) {}


  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  auto log_rec = std::make_shared<LogRec>(); // Use default constructor
  log_rec->type_ = LogRecType::kInsert;
  log_rec->txn_id_ = txn_id;
  log_rec->ins_key_ = std::move(ins_key);
  log_rec->ins_val_ = ins_val;

  log_rec->lsn_ = LogRec::next_lsn_++;
  // Assuming txn_id is already in prev_lsn_map_ due to a BEGIN log
  if (LogRec::prev_lsn_map_.count(txn_id)) {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  } else {
    // This case should ideally not happen if BEGIN always precedes operations.
    // Based on tests, map should contain the txn_id.
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  }
  LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
  return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  auto log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kDelete;
  log_rec->txn_id_ = txn_id;
  log_rec->del_key_ = std::move(del_key);
  log_rec->del_val_ = del_val;

  log_rec->lsn_ = LogRec::next_lsn_++;
  if (LogRec::prev_lsn_map_.count(txn_id)) {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  } else {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  }
  LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
  return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  auto log_rec = std::make_shared<LogRec>();
  log_rec->type_ = LogRecType::kUpdate;
  log_rec->txn_id_ = txn_id;
  log_rec->old_key_ = std::move(old_key);
  log_rec->old_val_ = old_val;
  log_rec->new_key_ = std::move(new_key);
  log_rec->new_val_ = new_val;

  log_rec->lsn_ = LogRec::next_lsn_++;
  if (LogRec::prev_lsn_map_.count(txn_id)) {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  } else {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  }
  LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
  return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  auto log_rec = std::make_shared<LogRec>(LogRecType::kBegin, txn_id);
  log_rec->lsn_ = LogRec::next_lsn_++;
  log_rec->prev_lsn_ = INVALID_LSN; // BEGIN log is the start of a transaction's log chain
  LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_; // Initialize or update map for this txn
  return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  auto log_rec = std::make_shared<LogRec>(LogRecType::kCommit, txn_id);
  log_rec->lsn_ = LogRec::next_lsn_++;
  if (LogRec::prev_lsn_map_.count(txn_id)) {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  } else {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  }
  LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
  return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  auto log_rec = std::make_shared<LogRec>(LogRecType::kAbort, txn_id);
  log_rec->lsn_ = LogRec::next_lsn_++;
  if (LogRec::prev_lsn_map_.count(txn_id)) {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  } else {
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
  }
  LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_;
  return log_rec;
}

#endif  // MINISQL_LOG_REC_H
