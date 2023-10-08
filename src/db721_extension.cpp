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

struct db721ScanGlobalState : public GlobalTableFunctionState {
};


class db721ScanFunction : public TableFunction {
public:
  db721ScanFunction()
  : TableFunction("db721_scan", {LogicalType::VARCHAR}, db721Scan, db721ScanBind, db721ScanInitGlobal) {
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
    result->group_offset = 0;
    result->current_group = -1;
    result->column_data.resize(reader->return_types.size());

    result->sel.Initialize(STANDARD_VECTOR_SIZE);
    result->reader = std::move(reader);

    return std::move(result);
  }


  static unique_ptr<GlobalTableFunctionState> db721ScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    auto& bind_data = input.bind_data->Cast<db721ScanBindData>();
    bind_data.reader->column_ids = input.column_ids;
    bind_data.reader->filters = input.filters;

    auto result = make_uniq<db721ScanGlobalState>();

    return std::move(result);
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
        data_p.bind_data->Cast<db721ScanBindData>().reader->Scan(context, data_p,output);
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
