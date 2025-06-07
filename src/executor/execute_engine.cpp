#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

#ifndef MAX_CHAR_LEN
#define MAX_CHAR_LEN 255 // Placeholder if not defined elsewhere
#endif

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

void PrintSyntaxTreeRecursive(pSyntaxNode node, int indent_level, bool is_last_child) {
  if (node == nullptr) {
    return;
  }

  // Print indentation
  for (int i = 0; i < indent_level; ++i) {
    std::cout << "  "; // 2 spaces for each indent level
  }

  // Print node information
  std::cout << "|- (" << node->id_ << ") " << GetSyntaxNodeTypeStr(node->type_);
  if (node->val_ != nullptr) {
    std::cout << " [val: \"" << node->val_ << "\"]";
  }
  std::cout << " (L" << node->line_no_ << ", C" << node->col_no_ << ")" << std::endl;

  // Recursively print children
  pSyntaxNode child = node->child_;
  while (child != nullptr) {
    PrintSyntaxTreeRecursive(child, indent_level + 1, child->next_ == nullptr);
    child = child->next_; // Move to the next sibling in the child list
  }
}

// Public function to initiate AST printing
void PrintSyntaxTree(pSyntaxNode root_node) {
  if (root_node == nullptr) {
    std::cout << "AST is empty." << std::endl;
    return;
  }
  std::cout << "Abstract Syntax Tree:" << std::endl;
  PrintSyntaxTreeRecursive(root_node, 0, true);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  // 1. Initial checks for context and basic AST structure
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }

  std::string table_name = ast->child_->val_;

  pSyntaxNode definitions_list_node = ast->child_->next_;

  // Stage 1: Parse all definitions from AST into temporary structures
  struct TempColumnInfo {
    std::string name;
    TypeId type_id;
    uint32_t char_length{0};
    bool is_unique_in_def{false}; // UNIQUE specified directly in column definition
    uint32_t original_index_in_ast; // Order in SQL statement
  };
  std::vector<TempColumnInfo> temp_col_infos;
  std::vector<std::string> pk_col_names_from_ast; // Names from PRIMARY KEY (col1, col2) clause
  uint32_t ast_col_def_order = 0;

  pSyntaxNode current_def_item = definitions_list_node->child_;
  while (current_def_item != nullptr) {
    if (current_def_item->type_ == kNodeColumnDefinition) {
      TempColumnInfo current_col_raw_info;
      current_col_raw_info.original_index_in_ast = ast_col_def_order++;

      if (current_def_item->val_ != nullptr && strcmp(current_def_item->val_, "unique") == 0) {
        current_col_raw_info.is_unique_in_def = true;
      }

      pSyntaxNode col_name_node = current_def_item->child_;
      current_col_raw_info.name = col_name_node->val_;

      pSyntaxNode type_node = col_name_node->next_;

      if (type_node->type_ == kNodeColumnType && strcmp(type_node->val_, "int") == 0) {
        current_col_raw_info.type_id = TypeId::kTypeInt;
      } else if (type_node->type_ == kNodeColumnType && strcmp(type_node->val_, "float") == 0) {
        current_col_raw_info.type_id = TypeId::kTypeFloat;
      } else if (type_node->type_ == kNodeColumnType && strcmp(type_node->val_, "char") == 0) {
        current_col_raw_info.type_id = TypeId::kTypeChar;
        pSyntaxNode len_node = type_node->child_;
        char *end_ptr;
        long len_val = strtol(len_node->val_, &end_ptr, 10);
        if (*end_ptr != '\0' || len_val <= 0 || len_val > MAX_CHAR_LEN) {
          std::cout << "Semantic error: Invalid length '" << len_node->val_ << "' for CHAR column '"
                     << current_col_raw_info.name << "'. Must be positive integer up to " << MAX_CHAR_LEN << "." << std::endl;
          return DB_FAILED;
        }
        current_col_raw_info.char_length = static_cast<uint32_t>(len_val);
      } else {
        LOG(ERROR) << "Syntax error: Unknown or malformed data type '" << type_node->val_
                   << "' for column '" << current_col_raw_info.name << "'.";
        return DB_FAILED;
      }
      temp_col_infos.push_back(current_col_raw_info);

    } else if (current_def_item->type_ == kNodeColumnList &&
               current_def_item->val_ != nullptr &&
               strcmp(current_def_item->val_, "primary keys") == 0) {
      pSyntaxNode pk_col_node = current_def_item->child_;
      while (pk_col_node != nullptr) {
        pk_col_names_from_ast.push_back(pk_col_node->val_);
        pk_col_node = pk_col_node->next_;
      }
    } else {
      LOG(ERROR) << "Syntax error: Unknown node type (" << GetSyntaxNodeTypeStr(current_def_item->type_)
                 << ") encountered in CREATE TABLE definition list.";
      return DB_FAILED;
    }
    current_def_item = current_def_item->next_;
  }

  if (temp_col_infos.empty()) {
    std::cout << "Syntax error: No columns defined for table '" << table_name << "'." << std::endl;
    return DB_FAILED;
  }

  if (!temp_col_infos.empty()) {
    std::unordered_set<std::string> defined_column_names;
    for (const auto& col_info : temp_col_infos) {
      if (defined_column_names.count(col_info.name)) {
        std::cout << "Semantic error: Duplicate column name '" << col_info.name
                   << "' in definition of table '" << table_name << "'." << std::endl;
        return DB_FAILED;
      }
      defined_column_names.insert(col_info.name);
    }
  }

  // Stage 2: Semantic validation of PK columns and creation of final Column objects
  std::vector<Column *> final_columns_vec;
  final_columns_vec.reserve(temp_col_infos.size());

  if (pk_col_names_from_ast.empty()) {
    std::cout << "Syntax error: No primary keys defined for table '" << table_name << "'." << std::endl;
    return DB_FAILED;
  }

  // First, validate that all PK column names actually refer to defined columns
  if (!pk_col_names_from_ast.empty()) {
    for (const std::string& pk_col_name : pk_col_names_from_ast) {
      auto it = std::find_if(temp_col_infos.begin(), temp_col_infos.end(),
                             [&](const TempColumnInfo& ci){ return ci.name == pk_col_name; });
      if (it == temp_col_infos.end()) {
        std::cout << "Semantic error: Primary key column '" << pk_col_name << "' is not defined as a column in table '" << table_name << "'." << std::endl;
        // No Column objects created yet, so no cleanup needed for final_columns_vec
        return DB_FAILED;
      }
    }
  }

  // Now create the final Column objects with correct properties
  for (const auto& temp_info : temp_col_infos) {
    bool is_pk_member = (std::find(pk_col_names_from_ast.begin(), pk_col_names_from_ast.end(), temp_info.name) != pk_col_names_from_ast.end());

    bool is_final_nullable = is_pk_member ? false : true; // PK columns are NOT NULL
    bool is_final_unique = is_pk_member ? true : temp_info.is_unique_in_def; // PK implies UNIQUE

    Column* new_column;
    if (temp_info.type_id == TypeId::kTypeChar) {
      new_column = new Column(temp_info.name, temp_info.type_id, temp_info.char_length,
                              temp_info.original_index_in_ast, is_final_nullable, is_final_unique);
    } else {
      new_column = new Column(temp_info.name, temp_info.type_id,
                              temp_info.original_index_in_ast, is_final_nullable, is_final_unique);
    }
    final_columns_vec.push_back(new_column);
  }

  // Stage 3: Create Schema and call CatalogManager
  Schema *schema = new Schema(final_columns_vec); // Schema takes ownership of Column objects

  TableInfo* table_info_out_ptr = nullptr;

  CatalogManager *catalog = context->GetCatalog();
  dberr_t result = catalog->CreateTable(table_name, schema, context->GetTransaction(), table_info_out_ptr);

  delete schema;
  schema = nullptr;

  if (result == DB_SUCCESS) {
    if (!pk_col_names_from_ast.empty()) {
        std::string pk_index_name = "pk_" + table_name;
        IndexInfo* pk_index_info_ptr = nullptr;
        std::string pk_index_type = "btree";

        dberr_t pk_index_result = catalog->CreateIndex(
            table_name,
            pk_index_name,
            pk_col_names_from_ast,
            context->GetTransaction(),
            pk_index_info_ptr,
            pk_index_type
        );

        if (pk_index_result != DB_SUCCESS) {
            LOG(ERROR) << "Table '" << table_name << "' created, but failed to create primary key index '"
                       << pk_index_name << "'. Error code: " << pk_index_result;
            catalog->DropTable(table_name);
            return pk_index_result;
        }
    }

    for (const auto& col_info : temp_col_infos) {
      if (col_info.is_unique_in_def) {
        bool already_part_of_pk = false;
        if (!pk_col_names_from_ast.empty()) {
          if (std::find(pk_col_names_from_ast.begin(), pk_col_names_from_ast.end(), col_info.name) != pk_col_names_from_ast.end()) {
            already_part_of_pk = true;
          }
        }

        if (!already_part_of_pk) {
          std::string unique_index_name = "uk_" + table_name + "_" + col_info.name;
          std::vector<std::string> index_key_for_unique_col = {col_info.name};
          IndexInfo* unique_index_info_ptr = nullptr;
          std::string unique_index_type = "btree";

          dberr_t unique_index_result = catalog->CreateIndex(
            table_name,
            unique_index_name,
            index_key_for_unique_col,
            context->GetTransaction(),
            unique_index_info_ptr,
            unique_index_type
          );
          if (unique_index_result != DB_SUCCESS) {
            LOG(ERROR) << "Table '" << table_name << "' created, but failed to create unique index '"
            << unique_index_name << "' on column '" << col_info.name
            << "'. Error code: " << unique_index_result;
            catalog->DropTable(table_name);
            if (result == DB_SUCCESS) {
              result = unique_index_result;
            }
          }
        }
      }
    }
  }

  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }

  std::string table_name = ast->child_->val_;

  std::vector<IndexInfo *> indexes;
  TableInfo *table_info;
  CatalogManager *catalog = context->GetCatalog();
  if (catalog->GetTable(table_name, table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }
  catalog->GetTableIndexes(table_name, indexes);

  for (auto index_info : indexes) {
    if (index_info != nullptr) {
      // Call CatalogManager's DropIndex for each index.
      dberr_t drop_index_result = catalog->DropIndex(table_name, index_info->GetIndexName());

      if (drop_index_result != DB_SUCCESS) {
        LOG(ERROR) << "Failed to drop index '" << index_info->GetIndexName()
                   << "' while dropping table '" << table_name << "'. Aborting operation.";
        return drop_index_result; // Return the specific error from DropIndex.
      }
    }
  }

  // CatalogManager::DropTable should return DB_TABLE_NOT_EXIST if table not found.
  return catalog->DropTable(table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif

  // 1. Initial checks: Ensure a database is currently selected.
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }

  // 2. Data Retrieval Phase
  CatalogManager *catalog = context->GetCatalog();
  std::vector<TableInfo *> all_tables;

  // Get all tables from the current database.
  if (catalog->GetTables(all_tables) != DB_SUCCESS || all_tables.empty()) {
    std::cout << "Empty set (0.00 sec)" << std::endl;
    return DB_SUCCESS;
  }

  // This structure will hold the necessary info for each index before printing.
  struct IndexDisplayInfo {
    std::string table_name;
    std::string index_name;
    std::string column_names;
    std::string index_type;
  };
  std::vector<IndexDisplayInfo> all_indexes_info;
  auto start_time = std::chrono::system_clock::now();

  // 3. Iterate through each table to find its indexes.
  for (const auto &table_info : all_tables) {
    std::string table_name = table_info->GetTableName();
    std::vector<IndexInfo *> table_indexes;

    // Get all indexes defined on the current table.
    catalog->GetTableIndexes(table_name, table_indexes);

    // For each index found, extract its details.
    for (const auto &index_info : table_indexes) {
      IndexDisplayInfo display_info;
      display_info.table_name = table_name;
      display_info.index_name = index_info->GetIndexName();
      display_info.index_type = "btree";

      // Format the key column names into a single comma-separated string.
      std::string key_cols_str;
      const auto& key_columns = index_info->GetIndexKeySchema()->GetColumns();
      for (size_t i = 0; i < key_columns.size(); ++i) {
        key_cols_str += key_columns[i]->GetName();
        if (i < key_columns.size() - 1) {
          key_cols_str += ", ";
        }
      }
      display_info.column_names = key_cols_str;

      all_indexes_info.push_back(display_info);
    }
  }

  // If no indexes exist in the entire database, report an empty set.
  if (all_indexes_info.empty()) {
    std::cout << "Empty set (0.00 sec)" << std::endl;
    return DB_SUCCESS;
  }

  // 4. Output Formatting using ResultWriter
  std::stringstream ss;
  ResultWriter writer(ss);

  // Define headers and calculate the required width for each column.
  std::vector<std::string> headers = {"Table", "Key_name", "Column_name", "Index_type"};
  std::vector<int> col_widths(headers.size());
  for(size_t i = 0; i < headers.size(); ++i) {
    col_widths[i] = headers[i].length();
  }

  for (const auto &info : all_indexes_info) {
    col_widths[0] = std::max(col_widths[0], (int)info.table_name.length());
    col_widths[1] = std::max(col_widths[1], (int)info.index_name.length());
    col_widths[2] = std::max(col_widths[2], (int)info.column_names.length());
    col_widths[3] = std::max(col_widths[3], (int)info.index_type.length());
  }

  // Print the table header.
  writer.Divider(col_widths);
  writer.BeginRow();
  writer.WriteHeaderCell(headers[0], col_widths[0]);
  writer.WriteHeaderCell(headers[1], col_widths[1]);
  writer.WriteHeaderCell(headers[2], col_widths[2]);
  writer.WriteHeaderCell(headers[3], col_widths[3]);
  writer.EndRow();
  writer.Divider(col_widths);

  // Print each row of index data.
  for (const auto &info : all_indexes_info) {
    writer.BeginRow();
    writer.WriteCell(info.table_name, col_widths[0]);
    writer.WriteCell(info.index_name, col_widths[1]);
    writer.WriteCell(info.column_names, col_widths[2]);
    writer.WriteCell(info.index_type, col_widths[3]);
    writer.EndRow();
  }
  writer.Divider(col_widths);

  // Print summary information.
  auto stop_time = std::chrono::system_clock::now();
  double duration_time = double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  writer.EndInformation(all_indexes_info.size(), duration_time, true);

  // Output the final formatted string.
  std::cout << writer.stream_.rdbuf();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif

  // 1. Initial checks: Ensure a database is selected.
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }

  // 2. Parse the Abstract Syntax Tree (AST) to extract necessary information.
  pSyntaxNode index_name_node = ast->child_;
  pSyntaxNode table_name_node = index_name_node->next_;
  pSyntaxNode column_list_node = table_name_node->next_;

  // Validate the basic structure of the AST.
  if (index_name_node == nullptr || table_name_node == nullptr || column_list_node == nullptr ||
      index_name_node->type_ != kNodeIdentifier || table_name_node->type_ != kNodeIdentifier ||
      column_list_node->type_ != kNodeColumnList) {
    LOG(ERROR) << "Syntax error: Malformed CREATE INDEX statement.";
    return DB_FAILED;
  }

  std::string index_name = index_name_node->val_;
  std::string table_name = table_name_node->val_;

  // Extract column names from the column list node.
  std::vector<std::string> index_keys;
  pSyntaxNode current_col_node = column_list_node->child_;
  if (current_col_node == nullptr) {
    std::cout << "Syntax error: At least one column must be specified for the index." << std::endl;
    return DB_FAILED;
  }
  while (current_col_node != nullptr) {
    index_keys.push_back(current_col_node->val_);
    current_col_node = current_col_node->next_;
  }

  // 3. Call the CatalogManager to perform the actual index creation.
  IndexInfo *index_info = nullptr; // This is an output parameter for the CreateIndex method.
  CatalogManager *catalog = context->GetCatalog();

  std::string index_type = "btree";
  dberr_t result = catalog->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(),
                                      index_info, index_type);

  // 4. Return the result code. The calling function will handle printing user-friendly messages.
  if (result == DB_SUCCESS) {
    std::cout << "Query OK, 0 rows affected" << std::endl;
  }

  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif

  // Step 1: Initial checks, ensure a database is selected.
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }

  // Step 2: Parse the AST to get the name of the index to drop.
  if (ast->child_ == nullptr || ast->child_->type_ != kNodeIdentifier || ast->child_->val_ == nullptr) {
    LOG(ERROR) << "Syntax error: Missing or invalid index name for DROP INDEX.";
    return DB_FAILED;
  }
  std::string index_name = ast->child_->val_;

  // Step 3: find the table name by iterating through all tables.
  std::string table_name = "";
  std::vector<TableInfo *> tables;

  // Get metadata for all tables in the current database from the CatalogManager.
  dberr_t get_tables_result = context->GetCatalog()->GetTables(tables);
  if (get_tables_result != DB_SUCCESS) {
    return get_tables_result;
  }

  for (auto table_info : tables) {
    IndexInfo *index_info = nullptr;

    dberr_t get_index_result = context->GetCatalog()->GetIndex(table_info->GetTableName(), index_name, index_info);

    if (get_index_result == DB_SUCCESS) {
      table_name = table_info->GetTableName();
      break;
    }
    if (get_index_result == DB_INDEX_NOT_FOUND) {

    } else {
      return get_index_result;
    }
  }

  // Step 4: Check if a table containing the index was found.
  if (table_name.empty()) {
    std::cout << "Index '" << index_name << "' not found in any table of the current database." << std::endl;
    return DB_INDEX_NOT_FOUND;
  }

  // Step 5: Call the CatalogManager to perform the actual index drop using the found table_name.
  dberr_t drop_result = context->GetCatalog()->DropIndex(table_name, index_name);

  // Step 6: Report the result and return the status code.
  if (drop_result == DB_SUCCESS) {
    std::cout << "Index '" << index_name << "' on table '" << table_name << "' dropped successfully." << std::endl;
  }

  return drop_result;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */

extern "C" {
  int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  if (ast->child_ == nullptr || ast->child_->type_ != kNodeString) {
    LOG(ERROR) << "Syntax error: Missing filename for EXECFILE.";
    return DB_FAILED;
  }
  std::string filename = ast->child_->val_;
  std::ifstream sql_file(filename);

  if (!sql_file.is_open()) {
    std::cout << "Failed to open file: " << filename << std::endl;
    return DB_FAILED;
  }

  std::string command_to_execute;
  dberr_t overall_status = DB_SUCCESS;
  int total_rows_inserted = 0;

  auto start_time = std::chrono::high_resolution_clock::now();

  char ch;
  while (sql_file.get(ch)) {
    command_to_execute += ch;
    if (ch == ';') {
      if (!command_to_execute.empty() && !std::all_of(command_to_execute.begin(), command_to_execute.end(), ::isspace)) {
        YY_BUFFER_STATE bp = yy_scan_string(command_to_execute.c_str());
        if (bp == nullptr) {
          LOG(ERROR) << "EXECFILE: Failed to create yy buffer state for command: " << command_to_execute;
          overall_status = DB_FAILED;
          command_to_execute.clear();
          continue;
        }
        yy_switch_to_buffer(bp);
        MinisqlParserInit();
        yyparse();

        dberr_t cmd_result;
        if (MinisqlParserGetError()) {
          printf("Error in file '%s' processing command '%s': %s\n",
                 filename.c_str(), command_to_execute.c_str(), MinisqlParserGetErrorMessage());
          cmd_result = DB_FAILED;
        } else {
          // Get the syntax tree root for the current command
          pSyntaxNode root_node = MinisqlGetParserRootNode();

          cmd_result = this->Execute(root_node);

          if (root_node && root_node->type_ == kNodeInsert && cmd_result == DB_SUCCESS) {
              total_rows_inserted++;
          }
        }

        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();

        if (cmd_result != DB_SUCCESS && cmd_result != DB_QUIT) {
            overall_status = cmd_result; // Record first significant error
        }
        if (cmd_result == DB_QUIT) {
            sql_file.close();
            // Stop timer and print before quitting if needed
            auto end_time = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed = end_time - start_time;
            if (total_rows_inserted > 0) {
              std::cout << total_rows_inserted << " row" << (total_rows_inserted > 1 ? "s" : "")
                        << " in set (" << std::fixed << std::setprecision(4) << elapsed.count() << " sec)." << std::endl;
            }
            return DB_QUIT; // Propagate QUIT immediately
        }
      }
      command_to_execute.clear(); // Reset for next command
    }
  }
  sql_file.close();

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end_time - start_time;

  // If there's leftover in command_buffer not ending with ';', it might be an incomplete command.
  if (!command_to_execute.empty() && !std::all_of(command_to_execute.begin(), command_to_execute.end(), ::isspace)) {
      LOG(WARNING) << "EXECFILE: Trailing content in '" << filename << "' without a semicolon: " << command_to_execute;
  }

  if (total_rows_inserted > 0) {
      std::cout << total_rows_inserted << " row" << (total_rows_inserted > 1 ? "s" : "")
                << " in set (" << std::fixed << std::setprecision(4) << elapsed.count() << " sec)." << std::endl;
  }

  return overall_status;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
