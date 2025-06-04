#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase persist_data_{};

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
  /**
    * TODO: Student Implement
    */
  void Init(CheckPoint &last_checkpoint) {
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    active_txns_ = last_checkpoint.active_txns_;
    data_ = last_checkpoint.persist_data_;
  }

private:
  // Helper function to undo a specific transaction
  void UndoSpecificTransaction(txn_id_t txn_id, lsn_t last_lsn_of_txn) {
    lsn_t current_lsn = last_lsn_of_txn;
    while (current_lsn != INVALID_LSN) {
      if (log_recs_.find(current_lsn) == log_recs_.end()) {
        break;
      }
      LogRecPtr current_log = log_recs_.at(current_lsn);

      switch (current_log->type_) {
        case LogRecType::kInsert:
          data_.erase(current_log->ins_key_);
          break;
        case LogRecType::kDelete:
          data_[current_log->del_key_] = current_log->del_val_;
          break;
        case LogRecType::kUpdate:
          data_[current_log->old_key_] = current_log->old_val_;
          break;
        case LogRecType::kBegin:
          current_lsn = INVALID_LSN; // Stop undo for this txn
          continue;
        case LogRecType::kCommit:
        case LogRecType::kAbort:
          // These mark end of operations; actual undoable operations are prior.
          break;
        case LogRecType::kInvalid:
          break;
      }
      current_lsn = current_log->prev_lsn_;
    }
  }

public:
  /**
    * TODO: Student Implement
    */
  void RedoPhase() {
    auto it = log_recs_.lower_bound(persist_lsn_);

    for (; it != log_recs_.end(); ++it) {
      LogRecPtr current_log = it->second;
      txn_id_t txn_id = current_log->txn_id_;

      switch (current_log->type_) {
        case LogRecType::kInsert:
          data_[current_log->ins_key_] = current_log->ins_val_;
          active_txns_[txn_id] = current_log->lsn_;
          break;
        case LogRecType::kDelete:
          data_.erase(current_log->del_key_);
          active_txns_[txn_id] = current_log->lsn_;
          break;
        case LogRecType::kUpdate:
          data_[current_log->new_key_] = current_log->new_val_;
          active_txns_[txn_id] = current_log->lsn_;
          break;
        case LogRecType::kBegin:
          active_txns_[txn_id] = current_log->lsn_;
          break;
        case LogRecType::kCommit:
          active_txns_.erase(txn_id);
          break;
        case LogRecType::kAbort:
          if (current_log->prev_lsn_ != INVALID_LSN) {
            UndoSpecificTransaction(txn_id, current_log->prev_lsn_);
          }
          active_txns_.erase(txn_id);
          break;
        case LogRecType::kInvalid:
          break;
      }
    }
  }

  /**
    * TODO: Student Implement
    */
  void UndoPhase() {
    std::vector<std::pair<txn_id_t, lsn_t>> txns_to_undo;
    for(const auto& pair : active_txns_) {
      txns_to_undo.push_back(pair);
    }

    for (const auto& pair : txns_to_undo) {
      txn_id_t txn_id = pair.first;
      lsn_t last_lsn = pair.second;
      UndoSpecificTransaction(txn_id, last_lsn);
    }
    active_txns_.clear();
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

private:
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
