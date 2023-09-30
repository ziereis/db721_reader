
#include "db721_reader.hpp"
#include "resizable_buffer.hpp"
#include "yyjson.h"
#include <iostream>

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

  string JsonReader::get_str(yyjson_val &obj) const {
      D_ASSERT(yyjson_is_str(&obj));
      const char* str = yyjson_get_str(&obj);
      return string(str);
  }

  yyjson_val &JsonReader::get_obj(yyjson_val &obj, const string& key) const {
    return *yyjson_obj_get(&obj, key.data());
  }

  int32_t JsonReader::get_int(yyjson_val &obj) const {
    D_ASSERT(yyjson_is_int(&obj));
    return yyjson_get_int(&obj);
  }
  float JsonReader::get_float(yyjson_val &obj) const {
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
      const db721MetaData::ColumnMetaData &column_metadata) {
        switch (column_metadata.type) {
          case db721MetaData::db721Type::INT:
            return LogicalType::INTEGER;
          case db721MetaData::db721Type::FLOAT:
            return LogicalType::FLOAT;
          case db721MetaData::db721Type::STRING:
            return LogicalType::VARCHAR;
        }
  }

  }
