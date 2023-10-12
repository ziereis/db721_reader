#define DUCKDB_EXTENSION_MAIN

#include "db721_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "yyjson.h"
#include "db721_reader.hpp"
#include "resizable_buffer.hpp"



// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {




class db721ScanFunction : public TableFunction {
public:
  db721ScanFunction()
  : TableFunction("db721_scan", {LogicalType::VARCHAR}, db721Scan, db721ScanBind, db721ScanInitGlobal, db721ScanInitLocal) {
    projection_pushdown = true;
    filter_pushdown = true;
    statistics = db721ScanStats;
  }

private:
  static unique_ptr<FunctionData> db721ScanBind(ClientContext& context, TableFunctionBindInput& input,
                                                vector<LogicalType>& return_types, vector<string>& names) {
    auto reader = make_uniq<db721Reader>(context, input.inputs[0].ToString());

    return_types = reader->return_types;
    names = reader->names;

    auto result = make_uniq<db721ScanBindData>();

    result->sel.Initialize(STANDARD_VECTOR_SIZE);
    result->reader = std::move(reader);

    return std::move(result);
  }


  static unique_ptr<GlobalTableFunctionState> db721ScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    auto& bind_data = input.bind_data->Cast<db721ScanBindData>();
    auto result = make_uniq<db721ScanGlobalState>();

    result->column_ids = input.column_ids;
    result->filters = input.filters.get();
    result->row_group_index = 0;
    result->max_threads = bind_data.reader->metadata->NumRowGroups();
    result->reader = bind_data.reader.get();

    return std::move(result);
  }

  static unique_ptr<LocalTableFunctionState> db721ScanInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
    auto& bind_data = input.bind_data->Cast<db721ScanBindData>();
    auto& global_data = global_state->Cast<db721ScanGlobalState>();

    auto result = make_uniq<db721ScanLocalState>();
    result->is_parallel = true;

    result->column_data.resize(global_data.reader->return_types.size());
    result->file_handle = global_data.reader->GetNewFileHandle();

    if (!db721ParallelStateNext(*result, global_data)) {
      return nullptr;
    }

    return std::move(result);

  };

  static bool db721ParallelStateNext( db721ScanLocalState &local_state, db721ScanGlobalState& global_state) {
      unique_lock<mutex> lock(global_state.lock);

      if (global_state.row_group_index < global_state.reader->metadata->NumRowGroups()) {
        local_state.reader = global_state.reader;
        InitializeScan(local_state, {global_state.row_group_index});
        global_state.row_group_index++;
        return true;
      }
      return false;
  }

  static void InitializeScan(db721ScanLocalState &local_state,
                                   vector<idx_t> groups_to_read) {
      local_state.current_group = -1;
      local_state.finished = false;
      local_state.group_offset = 0;
      local_state.group_idx_list = std::move(groups_to_read);
      local_state.sel.Initialize(STANDARD_VECTOR_SIZE);
  }

  static unique_ptr<BaseStatistics> db721ScanStats(ClientContext &context, const FunctionData *bind_data,
                                                   column_t column_index) {
        auto &data = bind_data->Cast<db721ScanBindData>();

        if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
          return nullptr;
        }

        return  data.reader->ReadStatistics(column_index);
  }


  static void db721Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
        if (!data_p.local_state) {
          return;
        }

        auto& local_state = data_p.local_state->Cast<db721ScanLocalState>();
        auto& global_state = data_p.global_state->Cast<db721ScanGlobalState>();

        do {
          local_state.reader->Scan(local_state, global_state, output);
          if (output.size() > 0) {
            return;
          }
          if (!db721ParallelStateNext(local_state, global_state)) {
            return;
          }

        } while (true);
  }

};


static void LoadInternal(DatabaseInstance &instance) {
  db721ScanFunction scan_func;
  scan_func.name = "db721_scan";
  ExtensionUtil::RegisterFunction(instance, scan_func);

}

void Db721Extension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string Db721Extension::Name() {
	return "db721";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void db721_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *db721_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
