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
  SelectionVector sel;
};

struct db721ScanGlobalState : public GlobalTableFunctionState {
};

typedef std::bitset<STANDARD_VECTOR_SIZE> db721_filter_t;

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
    result->reader = std::move(reader);
    result->column_data.resize(return_types.size());
    result->sel.Initialize(STANDARD_VECTOR_SIZE);

    return std::move(result);
  }

  static void prepare_chunk_buffer(db721ScanBindData &data, unsigned long col_idx) {
    int32_t chunk_start;
    int32_t chunk_len;
    auto file_col_idx = data.reader->column_ids[col_idx];
    auto& col = data.reader->metadata->columns[file_col_idx];
    auto& block = col.block_stats[data.current_group];
    chunk_start = block->block_start;
    chunk_len = block->total_size;

    if (data.reader->filters) {
      auto filter_entry = data.reader->filters->filters.find(col_idx);
      if (filter_entry != data.reader->filters->filters.end()) {
        bool skip_chunk = false;
        unique_ptr<BaseStatistics> stats;
        switch (col.type) {
        case db721MetaData::db721Type::INT:
          stats = db721Reader::get_block_stats(
              dynamic_cast<db721MetaData::BlockStatsInt &>(*block));
          break;
        case db721MetaData::db721Type::FLOAT:
          stats = db721Reader::get_block_stats(
              dynamic_cast<db721MetaData::BlockStatsFloat &>(*block));
          break;
        case db721MetaData::db721Type::STRING:
          stats = db721Reader::get_block_stats(
              dynamic_cast<db721MetaData::BlockStatsString &>(*block));
          break;
        }
        auto &filter = filter_entry->second;
        auto prune_result = filter->CheckStatistics(*stats);
        if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
          skip_chunk = true;
        }
        if (skip_chunk) {
          data.group_offset = block->count;
          return;
        }
      }
    }
    auto* file_handle  = data.reader->file_handle.get();

    data.column_data[file_col_idx].buf = ResizeableBuffer();

    file_handle->Seek(chunk_start);
    data.column_data[file_col_idx].buf.resize(data.reader->allocator,chunk_len);
    file_handle->Read(data.column_data[file_col_idx].buf.ptr, chunk_len);

  }


  template<class T>
  static void fill_from_plain(db721ScanColumData& col_data, idx_t count, db721_filter_t mask,
                              Vector& target, idx_t target_offset) {
    if (mask.none()) {
      col_data.buf.inc(sizeof(T) * count);
      return;
    }
    for (idx_t i = 0; i < count; i++) {
      auto value = col_data.buf.read<T>();
      if (!mask[i + target_offset]) {
        continue;
      } else {
        reinterpret_cast<T*>(FlatVector::GetData(target))[i + target_offset] = value;
      }
    }
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


  static bool db721ScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
  static void ScanColumn(db721ScanBindData& data, db721_filter_t& mask, idx_t count, idx_t out_col_idx, Vector& out);

  static void db721Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
        while(db721ScanImplementation(context, data_p, output)) {
          if (output.size() > 0) {
            break;
          }
          output.Reset();

        }
  }

};

template<>
void db721ScanFunction::fill_from_plain<string>(db721ScanColumData& col_data, idx_t count, db721_filter_t mask,
                                                Vector& target, idx_t target_offset) {
  if (mask.none()) {
        col_data.buf.inc(32 * count);
        return;
  }
  for (idx_t i = 0; i < count; i++) {
    if (!mask[i + target_offset]) {
      col_data.buf.inc(32);
      continue;
    }
      auto str_len = strlen(reinterpret_cast<const char *>(col_data.buf.ptr));
      if (str_len > 0) {
        D_ASSERT(str_len <= 32);
        FlatVector::GetData<string_t>(target)[i + target_offset] =
            StringVector::AddString(target, reinterpret_cast<const char *>(col_data.buf.ptr), str_len);
      } else {
        FlatVector::SetNull(target, i + target_offset, true);
      }
    col_data.buf.inc(32);
  }
}

void db721ScanFunction::ScanColumn(db721ScanBindData& data, db721_filter_t& mask, idx_t count, idx_t out_col_idx, Vector& out) {
  auto file_col_idx = data.reader->column_ids[out_col_idx];

  if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
    Value constant_42 = Value::BIGINT(42);
    out.Reference(constant_42);
    return;
  }

  auto &col_data = data.column_data[file_col_idx];
  idx_t output_offset = 0;
  while (output_offset < count) {
    auto current_batch_size = std::min(col_data.chunk_size - col_data.chuck_offset, count - output_offset);

    if (current_batch_size == 0) {
      break;
    }

    switch (data.reader->return_types[file_col_idx].id()) {
    case LogicalType::INTEGER:
      fill_from_plain<int32_t>(col_data, current_batch_size, mask, out, output_offset);
      break;
    case LogicalType::FLOAT:
      fill_from_plain<float>(col_data, current_batch_size, mask, out, output_offset);
      break;
    case LogicalType::VARCHAR:
      fill_from_plain<string>(col_data, current_batch_size, mask, out, output_offset);
      break;
    default:
      throw NotImplementedException("Unimplemented type for db721 reader");
    }
    output_offset += current_batch_size;
    col_data.chuck_offset += current_batch_size;
  }

}

template <class T, class OP>
void TemplatedFilterOperation(Vector &v, T constant, db721_filter_t &filter_mask, idx_t count) {
  if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    auto v_ptr = ConstantVector::GetData<T>(v);
    auto &mask = ConstantVector::Validity(v);

    if (mask.RowIsValid(0)) {
      if (!OP::Operation(v_ptr[0], constant)) {
        filter_mask.reset();
      }
    }
    return;
  }

  D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);
  auto v_ptr = FlatVector::GetData<T>(v);
  auto &mask = FlatVector::Validity(v);

  if (!mask.AllValid()) {
    for (idx_t i = 0; i < count; i++) {
      if (mask.RowIsValid(i)) {
        filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
      }
    }
  } else {
    for (idx_t i = 0; i < count; i++) {
      filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
    }
  }
}

template <class T, class OP>
void TemplatedFilterOperation(Vector &v, const Value &constant, db721_filter_t &filter_mask, idx_t count) {
  TemplatedFilterOperation<T, OP>(v, constant.template GetValueUnsafe<T>(), filter_mask, count);
}



template <class OP>
static void FilterOperationSwitch(Vector &v, Value &constant, db721_filter_t &filter_mask, idx_t count) {
  if (filter_mask.none() || count == 0) {
    return;
  }
  switch (v.GetType().InternalType()) {
  case PhysicalType::INT32:
    TemplatedFilterOperation<int32_t, OP>(v, constant, filter_mask, count);
    break;
  case PhysicalType::FLOAT:
    TemplatedFilterOperation<float, OP>(v, constant, filter_mask, count);
    break;
  case PhysicalType::VARCHAR:
    TemplatedFilterOperation<string_t, OP>(v, constant, filter_mask, count);
    break;
  default:
    throw NotImplementedException("Unsupported type for filter %s", v.ToString());
  }
}

static void FilterIsNotNull(Vector &v, db721_filter_t &filter_mask, idx_t count) {
  if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    auto &mask = ConstantVector::Validity(v);
    if (!mask.RowIsValid(0)) {
      filter_mask.reset();
    }
    return;
  }
  D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);

  auto &mask = FlatVector::Validity(v);
  if (!mask.AllValid()) {
    for (idx_t i = 0; i < count; i++) {
      filter_mask[i] = filter_mask[i] && mask.RowIsValid(i);
    }
  }
}

static void FilterIsNull(Vector &v, db721_filter_t &filter_mask, idx_t count) {
  if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    auto &mask = ConstantVector::Validity(v);
    if (mask.RowIsValid(0)) {
      filter_mask.reset();
    }
    return;
  }
  D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);

  auto &mask = FlatVector::Validity(v);
  if (mask.AllValid()) {
    filter_mask.reset();
  } else {
    for (idx_t i = 0; i < count; i++) {
      filter_mask[i] = filter_mask[i] && !mask.RowIsValid(i);
    }
  }
}

static void ApplyFilter(Vector &v, TableFilter &filter, db721_filter_t& filter_mask, idx_t count) {
  switch (filter.filter_type) {
  case TableFilterType::CONJUNCTION_AND: {
    auto &conjunction = filter.Cast<ConjunctionAndFilter>();
    for (auto &child_filter : conjunction.child_filters) {
      ApplyFilter(v, *child_filter, filter_mask, count);
    }
    break;
  }
  case TableFilterType::CONJUNCTION_OR: {
    auto &conjunction = filter.Cast<ConjunctionOrFilter>();
    db721_filter_t or_mask;
    for (auto &child_filter : conjunction.child_filters) {
      db721_filter_t child_mask = filter_mask;
      ApplyFilter(v, *child_filter, child_mask, count);
      or_mask |= child_mask;
    }
    filter_mask &= or_mask;
    break;
  }
  case TableFilterType::CONSTANT_COMPARISON: {
    auto &constant_filter = filter.Cast<ConstantFilter>();
    switch (constant_filter.comparison_type) {
    case ExpressionType::COMPARE_EQUAL:
      FilterOperationSwitch<Equals>(v, constant_filter.constant, filter_mask, count);
      break;
    case ExpressionType::COMPARE_LESSTHAN:
      FilterOperationSwitch<LessThan>(v, constant_filter.constant, filter_mask, count);
      break;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      FilterOperationSwitch<LessThanEquals>(v, constant_filter.constant, filter_mask, count);
      break;
    case ExpressionType::COMPARE_GREATERTHAN:
      FilterOperationSwitch<GreaterThan>(v, constant_filter.constant, filter_mask, count);
      break;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      FilterOperationSwitch<GreaterThanEquals>(v, constant_filter.constant, filter_mask, count);
      break;
    default:
      D_ASSERT(0);
    }
    break;
  }
  case TableFilterType::IS_NOT_NULL:
    FilterIsNotNull(v, filter_mask, count);
   break;
  case TableFilterType::IS_NULL:{
   FilterIsNull(v, filter_mask, count);
    break;
  }
  default:
    D_ASSERT(0);
    break;
  }
}



bool db721ScanFunction::db721ScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = data_p.bind_data->CastNoConst<db721ScanBindData>();

  if (data.finished) {
    return false;
  }

  if ((data.current_group < 0) ||
      (data.group_offset >= data.reader->metadata->columns[0].block_stats[data.current_group]->count)) {
    data.current_group++;
    data.group_offset = 0;
    if (data.current_group >= data.reader->metadata->NumRowGroups()) {
      data.finished = true;
      return false;
    }

    for (idx_t out_col_idx = 0; out_col_idx < output.ColumnCount(); out_col_idx++) {
      auto file_col_idx = data.reader->column_ids[out_col_idx];

      if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
        continue;
      }

      prepare_chunk_buffer(data, out_col_idx);
      data.column_data[file_col_idx].chuck_offset = 0;
      data.column_data[file_col_idx].chunk_size =
          data.reader->metadata->columns[file_col_idx].block_stats[data.current_group]->count;
    }

    return true;
  }


  auto output_chunk_rows = std::min((int64_t)STANDARD_VECTOR_SIZE, data.reader->metadata->columns[0].block_stats[data.current_group]->count - data.group_offset);

  if (output_chunk_rows == 0) {
    data.finished = true;
    return false;
  }
  output.SetCardinality(output_chunk_rows);
  D_ASSERT(output.size() > 0);

  db721_filter_t filter_mask;
  filter_mask.set();

  for(idx_t i = output_chunk_rows; i < STANDARD_VECTOR_SIZE; i++) {
    filter_mask.set(i, false);
  }


  if (data.reader->filters) {
    vector<bool> need_to_read(data.reader->column_ids.size(), true);

    for (auto& filter_col : data.reader->filters->filters) {
      if (filter_mask.none()) {
        break;
      }
      ScanColumn(data,filter_mask, output.size(), filter_col.first, output.data[filter_col.first]);
      need_to_read[filter_col.first] = false;

      ApplyFilter(output.data[filter_col.first], *filter_col.second, filter_mask, output_chunk_rows);

      }

    for (idx_t col_idx = 0; col_idx < data.reader->column_ids.size(); col_idx++) {
      if (need_to_read[col_idx]) {
        ScanColumn(data,filter_mask, output.size(), col_idx, output.data[col_idx]);
      }
    }

    idx_t sel_size = 0;
    for (idx_t i = 0; i < output_chunk_rows; i++) {
      if (filter_mask[i]) {
        data.sel.set_index(sel_size++, i);
      }
    }
    output.Slice(data.sel, sel_size);
    output.Verify();
  } else {
    for (idx_t col_idx = 0; col_idx < data.reader->column_ids.size(); col_idx++) {
        ScanColumn(data,filter_mask, output.size(), col_idx, output.data[col_idx]);
    }
  }

  data.group_offset += output_chunk_rows;
  return true;
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
