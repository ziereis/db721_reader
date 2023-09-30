#pragma once

#include "duckdb.hpp"
#include "yyjson.h"

namespace duckdb {

  class JsonReader {
  public:
    JsonReader(const char* data, uint64_t data_len);
    ~JsonReader();

    yyjson_val& get_root() const;
    string get_str(yyjson_val& obj) const;

    yyjson_val& get_obj(yyjson_val& obj, const string& key) const;
    int32_t get_int(yyjson_val& obj) const;
    float get_float(yyjson_val& obj) const;

    template<class Fn>
    void for_each(yyjson_val& obj, Fn&& func) const;




    yyjson_doc* doc;


  };

  class db721MetaData {
  public:
    enum class db721Type  {
        INT,
        FLOAT,
        STRING,
    };

    struct BlockStatsBase {
        int32_t block_start;
        int32_t total_size;
        int32_t count;
        virtual ~BlockStatsBase() = default;

    };

    struct BlockStatsString : public BlockStatsBase {
        string min;
        string max;
        int32_t min_len;
        int32_t max_len;
    };

    struct BlockStatsInt : public BlockStatsBase {
      int32_t min;
      int32_t max;
    };

    struct BlockStatsFloat : public BlockStatsBase {
      float min;
      float max;
    };

    struct ColumnMetaData {
      string name;
      db721Type type;
      int32_t num_blocks;
      int32_t start_offset;
      vector<unique_ptr<BlockStatsBase>> block_stats;
    };


    string table_name;
    int32_t max_block_entrys;
    vector<ColumnMetaData> columns;

    void read(Allocator& allocator, FileHandle& file_handle,
              uint64_t metadata_pos, uint32_t footer_len);
    uint32_t NumRowGroups() const;


  };

  class db721Reader {
  public:
    db721Reader(ClientContext &context, string file_name);

    void InitializeSchema();
    static LogicalType DeriveLogicalType(const db721MetaData::ColumnMetaData& column_metadata);


    FileSystem& fs;
    Allocator& allocator;
    string filename;
    unique_ptr<FileHandle> file_handle;
    unique_ptr<db721MetaData> metadata;
    vector<LogicalType> return_types;
    vector<string> names;
    vector<column_t> column_ids;


  };


}