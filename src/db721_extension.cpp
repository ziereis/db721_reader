#define DUCKDB_EXTENSION_MAIN

#include "db721_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "yyjson.h"
#include "db721_reader.hpp"
#include "resizable_buffer.hpp"


// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

  struct db721ScanColumData {
  idx_t chuck_offset;
  idx_t chunk_size;

  ResizeableBuffer buf;
};

struct db721ScanBindData : public TableFunctionData {
  int64_t current_group;
  int64_t group_offset;
  unique_ptr<db721Reader> reader;
  bool finished;
  vector<db721ScanColumData> column_data;
};

struct db721ScanGlobalState : public GlobalTableFunctionState {
};

class db721ScanFunction : public TableFunction {
public:
  db721ScanFunction()
  : TableFunction("db721_scan", {LogicalType::VARCHAR}, db721ScanImplementation, db721ScanBind, db721ScanInitGlobal) {
    projection_pushdown = true;
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
    result->reader = std::move(reader);
    result->column_data.resize(return_types.size());

    return std::move(result);
  }

  static void prepare_chunk_buffer(db721ScanBindData &data, unsigned long col_idx) {
    int32_t chunk_start;
    int32_t chunk_len;
    auto& col = data.reader->metadata->columns[col_idx];
    auto& block = col.block_stats[data.current_group];
    chunk_start = block->block_start;
    chunk_len = block->total_size;

    auto* file_handle  = data.reader->file_handle.get();

    data.column_data[col_idx].buf = ResizeableBuffer();

    file_handle->Seek(chunk_start);
    data.column_data[col_idx].buf.resize(data.reader->allocator,chunk_len);
    file_handle->Read(data.column_data[col_idx].buf.ptr, chunk_len);

  }


  template<class T>
  static void fill_from_plain(db721ScanColumData& col_data, idx_t count, Vector& target,
                              idx_t target_offset) {
    for (idx_t i = 0; i < count; i++) {
      reinterpret_cast<T*>(FlatVector::GetData(target))[i + target_offset] = col_data.buf.read<T>();
    }
  }

  static unique_ptr<GlobalTableFunctionState> db721ScanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    input.bind_data->Cast<db721ScanBindData>().reader->column_ids = input.column_ids;
    auto result = make_uniq<db721ScanGlobalState>();

    return std::move(result);
  }


  static void db721ScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);



};

template<>
void db721ScanFunction::fill_from_plain<string>(db721ScanColumData& col_data, idx_t count, Vector& target,
                             idx_t target_offset) {
  for (idx_t i = 0; i < count; i++) {
    auto str_len = strlen(reinterpret_cast<const char *>(col_data.buf.ptr));
    string str(reinterpret_cast<const char *>(col_data.buf.ptr));
    D_ASSERT(str_len <= 32);
    FlatVector::GetData<string_t>(target)[i + target_offset] =
        StringVector::AddString(target, str.data(), str.size());
    col_data.buf.inc(32);
  }
}

void db721ScanFunction::db721ScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = data_p.bind_data->CastNoConst<db721ScanBindData>();

  if (data.finished) {
    return;
  }

  if ((data.current_group < 0) ||
      (data.group_offset >= data.reader->metadata->columns[0].block_stats[data.current_group]->count)) {
    data.current_group++;
    data.group_offset = 0;
    if (data.current_group >= data.reader->metadata->NumRowGroups()) {
      data.finished = true;
      return;
    }

    for (idx_t out_col_idx = 0; out_col_idx < output.ColumnCount(); out_col_idx++) {
      auto file_col_idx = data.reader->column_ids[out_col_idx];

      if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
        continue;
      }

      prepare_chunk_buffer(data, file_col_idx);
      data.column_data[file_col_idx].chuck_offset = 0;
      data.column_data[file_col_idx].chunk_size =
          data.reader->metadata->columns[file_col_idx].block_stats[data.current_group]->count;
    }
  }

  output.SetCardinality(std::min((int64_t)STANDARD_VECTOR_SIZE, data.reader->metadata->columns[0].block_stats[data.current_group]->count - data.group_offset));
  D_ASSERT(output.size() > 0);

  for (idx_t out_col_idx = 0; out_col_idx < output.ColumnCount(); out_col_idx++) {
    auto file_col_idx = data.reader->column_ids[out_col_idx];

    if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
      Value constant_42 = Value::BIGINT(42);
      output.data[out_col_idx].Reference(constant_42);
      continue;
    }

    auto &col_data = data.column_data[file_col_idx];
    idx_t output_offset = 0;
    while (output_offset < output.size()) {
      auto current_batch_size = std::min(col_data.chunk_size - col_data.chuck_offset, output.size() - output_offset);
      D_ASSERT(current_batch_size > 0);

      switch (data.reader->return_types[file_col_idx].id()) {
      case LogicalType::INTEGER:
        fill_from_plain<int32_t>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
        break;
      case LogicalType::FLOAT:
        fill_from_plain<float>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
        break;
      case LogicalType::VARCHAR:
        fill_from_plain<string>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
        break;
      default:
        throw NotImplementedException("Unimplemented type for db721 reader");
      }
      output_offset += current_batch_size;
      col_data.chuck_offset += current_batch_size;
    }
  }
  data.group_offset += output.size();
}

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
