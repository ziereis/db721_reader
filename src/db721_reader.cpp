
#include "db721_reader.hpp"
#include "resizable_buffer.hpp"
#include "yyjson.h"
#include <iostream>
#include "filter_operations.hpp"

namespace duckdb {

  JsonReader::JsonReader(const char *data, uint64_t data_len)
  : doc(yyjson_read(data, data_len, 0)) {
    if(!doc) {
      throw InvalidInputException("couldnt read json, invalid input");
    }
  }

  JsonReader::~JsonReader() {
      yyjson_doc_free(doc);
  }

  yyjson_val &JsonReader::get_root() const {
      return *yyjson_doc_get_root(doc);
  }

  string JsonReader::get_str(yyjson_val &obj) {
      D_ASSERT(yyjson_is_str(&obj));
      const char* str = yyjson_get_str(&obj);
      return string(str);
  }

  yyjson_val &JsonReader::get_obj(yyjson_val &obj, const string& key) {
    return *yyjson_obj_get(&obj, key.data());
  }

  int32_t JsonReader::get_int(yyjson_val &obj) {
    D_ASSERT(yyjson_is_int(&obj));
    return yyjson_get_int(&obj);
  }
  float JsonReader::get_float(yyjson_val &obj) {
    return static_cast<float>(yyjson_get_real(&obj));

  }

  template <class Fn>
  void JsonReader::for_each(yyjson_val &obj, Fn&& func) const {
    yyjson_val* val;
    yyjson_val* key;
    size_t idx, max;

    yyjson_obj_foreach(&obj, idx, max, key, val) {
      func(key,val);
    }
  }

  void db721MetaData::read(Allocator& allocator, FileHandle &file_handle,
                         uint64_t metadata_pos, uint32_t footer_len) {
    ResizeableBuffer buf;
    buf.resize(allocator, footer_len);
    buf.zero();

    file_handle.Read(buf.ptr, footer_len, metadata_pos);


    string_t str(reinterpret_cast<const char*>(buf.ptr), footer_len);

    JsonReader json_reader(str.GetData(),footer_len);

    auto& root = json_reader.get_root();

    auto& table_obj = json_reader.get_obj(root, "Table");
    table_name = json_reader.get_str(table_obj);

    auto& max_block_entrys_obj = json_reader.get_obj(root, "Max Values Per Block");
    max_block_entrys = json_reader.get_int(max_block_entrys_obj);

    auto& column_obj = json_reader.get_obj(root, "Columns");



    auto parse_column_data = [this, &json_reader](yyjson_val* key, yyjson_val* value) {
      ColumnMetaData column_metadata;
      auto& type_obj = json_reader.get_obj(*value, "type");
      string type_str = json_reader.get_str(type_obj);

      if (type_str == "int") {
        column_metadata.type = db721Type::INT;
      } else if (type_str == "float") {
        column_metadata.type = db721Type::FLOAT;
      } else if (type_str == "str") {
        column_metadata.type = db721Type::STRING;
      } else {
        throw InvalidInputException("could not parse string %s as type.", type_str);
      }

      column_metadata.name = json_reader.get_str(*key);

      auto& start_offset_obj = json_reader.get_obj(*value, "start_offset");
      column_metadata.start_offset = json_reader.get_int(start_offset_obj);

      auto& num_blocks_obj = json_reader.get_obj(*value, "num_blocks");
      column_metadata.num_blocks = json_reader.get_int(num_blocks_obj);

      auto& block_stats_obj = json_reader.get_obj(*value, "block_stats");

      db721Type type = column_metadata.type;
      int32_t block_start = column_metadata.start_offset;

      auto parse_block_stats = [&column_metadata, &json_reader, type, &block_start](yyjson_val* key, yyjson_val* value) {
        string idx = json_reader.get_str(*key);
        auto& count_obj = json_reader.get_obj(*value, "num");
        int32_t count = json_reader.get_int(count_obj);

        switch (type) {
          case db721Type::INT: {
            auto block_stats = make_uniq<BlockStatsInt>();
            auto& min_obj = json_reader.get_obj(*value, "min");
            auto& max_obj = json_reader.get_obj(*value, "max");

            block_stats->block_start = block_start;
            block_stats->total_size = count * sizeof(int32_t);
            block_start += block_stats->total_size;
            block_stats->count = count;
            block_stats->min = json_reader.get_int(min_obj);
            block_stats->max = json_reader.get_int(max_obj);

            column_metadata.block_stats.push_back(std::move(block_stats));
            break;
          }
          case db721Type::FLOAT: {
            auto block_stats = make_uniq<BlockStatsFloat>();

            auto& min_obj = json_reader.get_obj(*value, "min");
            auto& max_obj = json_reader.get_obj(*value, "max");

            block_stats->block_start = block_start;
            block_stats->total_size = count * sizeof(float);
            block_start += block_stats->total_size;
            block_stats->count = count;
            block_stats->min = json_reader.get_float(min_obj);
            block_stats->max = json_reader.get_float(max_obj);

            column_metadata.block_stats.push_back(std::move(block_stats));
            break;

          }
          case db721Type::STRING: {
            auto block_stats = make_uniq<BlockStatsString>();
            auto& min_obj = json_reader.get_obj(*value, "min");
            auto& max_obj = json_reader.get_obj(*value, "max");
            auto& min_len_obj = json_reader.get_obj(*value, "min_len");
            auto& max_len_obj = json_reader.get_obj(*value, "max_len");


            block_stats->block_start = block_start;
            block_stats->total_size = count * 32;
            block_start += block_stats->total_size;
            block_stats->count = count;
            block_stats->min = json_reader.get_str(min_obj);
            block_stats->max = json_reader.get_str(max_obj);
            block_stats->min_len = json_reader.get_int(min_len_obj);
            block_stats->max_len = json_reader.get_int(max_len_obj);

            column_metadata.block_stats.push_back(std::move(block_stats));
            break;
          }
        }
      };

      json_reader.for_each(block_stats_obj,parse_block_stats);

      this->columns.push_back(std::move(column_metadata));

    };

    json_reader.for_each(column_obj, parse_column_data);
  }
  uint32_t db721MetaData::NumRowGroups() const {
        return columns[0].num_blocks;
  }
  int64_t db721MetaData::NumRowsOfGroup(idx_t group) const {
        D_ASSERT(!columns.empty());
        return columns[0].block_stats[group]->count;
  }

  static unique_ptr<db721MetaData> LoadMetadata(Allocator& allocator, FileHandle& file_handle) {

    auto file_size = file_handle.GetFileSize();

    if (file_size < 4) {
      throw InvalidInputException("File %s too small to be a db721 file", file_handle.path);
    }

    ResizeableBuffer buf;
    buf.resize(allocator, 4);
    buf.zero();

    file_handle.Read(buf.ptr, 4, file_size -4);

    auto footer_len = *reinterpret_cast<uint32_t*>(buf.ptr);

    if (footer_len == 0 || file_size < 4 + footer_len) {
      throw InvalidInputException("Footer length error in file %s", file_handle.path);
    }

    auto metadata_pos = file_size - (footer_len + 4);

    auto metadata = make_uniq<db721MetaData>();
    metadata->read(allocator,file_handle, metadata_pos, footer_len);


    return metadata;

  }

  //return true of chunk can be skipped if not false
  bool LoadChunk(db721ScanBindData &data, unsigned long col_idx) {

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
        unique_ptr<BaseStatistics> stats;
        switch (col.type) {
        case db721Type::INT:
          stats = db721Reader::get_block_stats(
              dynamic_cast<BlockStatsInt &>(*block));
          break;
        case db721Type::FLOAT:
          stats = db721Reader::get_block_stats(
              dynamic_cast<BlockStatsFloat &>(*block));
          break;
        case db721Type::STRING:
          stats = db721Reader::get_block_stats(
              dynamic_cast<BlockStatsString &>(*block));
          break;
        }
        auto &filter = filter_entry->second;
        auto prune_result = filter->CheckStatistics(*stats);
        if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
          return true;
        }
      }
    }
    auto* file_handle  = data.reader->file_handle.get();

    data.column_data[file_col_idx].buf = ResizeableBuffer();

    file_handle->Seek(chunk_start);
    data.column_data[file_col_idx].buf.resize(data.reader->allocator,chunk_len);
    file_handle->Read(data.column_data[file_col_idx].buf.ptr, chunk_len);
    return false;


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

  template<>
  void fill_from_plain<string>(db721ScanColumData& col_data, idx_t count, db721_filter_t mask,
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

  db721Reader::db721Reader(ClientContext &context, string file_name)
      : fs(FileSystem::GetFileSystem(context))
      , allocator(BufferAllocator::Get(context))
      , filename(std::move(file_name))
      , file_handle(fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ)){

      if (!file_handle->CanSeek()) {
        throw InvalidInputException("db721 file needs to be seakable.");
      }

      metadata = LoadMetadata(allocator, *file_handle);
      InitializeSchema();
    }

  void db721Reader::InitializeSchema() {
      for(const auto& col : metadata->columns) {
        names.push_back(col.name);
        return_types.push_back(DeriveLogicalType(col));
      }
  }

  LogicalType db721Reader::DeriveLogicalType(
      const ColumnMetaData &column_metadata) {
        switch (column_metadata.type) {
          case db721Type::INT:
            return LogicalType::INTEGER;
          case db721Type::FLOAT:
            return LogicalType::FLOAT;
          case db721Type::STRING:
            return LogicalType::VARCHAR;
        }
  }

   unique_ptr<BaseStatistics> db721Reader::get_block_stats(const BlockStatsString& blockstats) {
        auto string_stats = StringStats::CreateEmpty(LogicalType::VARCHAR);
        StringStats::Update(string_stats, blockstats.min);
        StringStats::Update(string_stats, blockstats.max);
        return string_stats.ToUnique();
  }

   unique_ptr<BaseStatistics> db721Reader::get_block_stats(const BlockStatsInt& blockstats) {
        auto stats = NumericStats::CreateUnknown(LogicalType::INTEGER);

        Value min = blockstats.min;
        Value max = blockstats.max;

        NumericStats::SetMax(stats, max);
        NumericStats::SetMin(stats, min);

        return stats.ToUnique();
  }

   unique_ptr<BaseStatistics> db721Reader::get_block_stats(const BlockStatsFloat& blockstats) {
        auto stats = NumericStats::CreateUnknown(LogicalType::FLOAT);

        Value min = blockstats.min;
        Value max = blockstats.max;

        NumericStats::SetMax(stats, max);
        NumericStats::SetMin(stats, min);

        return stats.ToUnique();
  }

  unique_ptr<BaseStatistics> db721Reader::ReadStatistics(column_t col_idx) const {
        unique_ptr<BaseStatistics> column_stats;

        auto& column_metadata = metadata->columns[col_idx];

        for (auto& row_group : column_metadata.block_stats) {
            unique_ptr<BaseStatistics> chunk_stats;
            switch (column_metadata.type) {
            case db721Type::INT:
              chunk_stats = get_block_stats(dynamic_cast<BlockStatsInt&>(*row_group));
              break;
            case db721Type::FLOAT:
              chunk_stats = get_block_stats(dynamic_cast<BlockStatsFloat&>(*row_group));
              break;
            case db721Type::STRING:
              chunk_stats = get_block_stats(dynamic_cast<BlockStatsString&>(*row_group));
              break;
            }

            if (!column_stats) {
              column_stats = std::move(chunk_stats);
            } else {
              column_stats->Merge(*chunk_stats);
            }

        }
        return column_stats;
  }

void db721Reader::ScanColumn(db721ScanBindData& data, db721_filter_t& mask, idx_t count, idx_t out_col_idx, Vector& out) {
  auto file_col_idx = column_ids[out_col_idx];

  if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
    Value constant_42 = Value::BIGINT(42);
    out.Reference(constant_42);
    return;
  }

  auto &col_data = data.column_data[file_col_idx];
  idx_t output_offset = 0;
  while (output_offset < count) {
    auto current_batch_size = std::min(col_data.RowsLeftToScan(), count - output_offset);

    if (current_batch_size == 0) {
      break;
    }

    switch (return_types[file_col_idx].id()) {
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
    col_data.InrementOffset(current_batch_size);
  }

}

void db721Reader::Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    while(ScanImplementation(context, data_p, output)) {
      if (output.size() > 0) {
        break;
      }
      output.Reset();

    }
}

bool db721Reader::ScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = data_p.bind_data->CastNoConst<db721ScanBindData>();

  if (data.finished) {
    return false;
  }

  if ((data.current_group < 0) ||
      (data.group_offset >= metadata->NumRowsOfGroup(data.current_group))) {
    data.current_group++;
    data.group_offset = 0;
    if (data.current_group >= metadata->NumRowGroups()) {
      data.finished = true;
      return false;
    }

    for (idx_t out_col_idx = 0; out_col_idx < output.ColumnCount(); out_col_idx++) {
      auto file_col_idx = column_ids[out_col_idx];

      if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
        continue;
      }

      auto& col_data = data.column_data[file_col_idx];

      bool can_skip = LoadChunk(data, out_col_idx);
      if (can_skip) {
        data.group_offset = (int64_t) metadata->NumRowsOfGroup(data.current_group);
      } else {
        col_data.SetOffset(0);
        col_data.SetSize(metadata->NumRowsOfGroup(data.current_group));

      }
    }

    return true;
  }


  auto output_chunk_rows = std::min((int64_t)STANDARD_VECTOR_SIZE, metadata->NumRowsOfGroup(data.current_group) - data.group_offset);

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


  if (filters) {
    vector<bool> need_to_read(column_ids.size(), true);

    for (auto& filter_col : filters->filters) {
      if (filter_mask.none()) {
        break;
      }
      ScanColumn(data,filter_mask, output.size(), filter_col.first, output.data[filter_col.first]);
      need_to_read[filter_col.first] = false;

      ApplyFilter(output.data[filter_col.first], *filter_col.second, filter_mask, output_chunk_rows);

    }

    for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
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
  } else {
    for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
      ScanColumn(data,filter_mask, output.size(), col_idx, output.data[col_idx]);
    }
  }

  data.group_offset += (int64_t) output_chunk_rows;
  return true;
}

  }
