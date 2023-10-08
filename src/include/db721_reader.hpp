#pragma once

#include "duckdb.hpp"
#include "yyjson.h"
#include "resizable_buffer.hpp"

namespace duckdb {

  class JsonReader {
  public:
    JsonReader(const char* data, uint64_t data_len);
    ~JsonReader();

    yyjson_val& get_root() const;
    static string get_str(yyjson_val& obj) ;

    static yyjson_val& get_obj(yyjson_val& obj, const string& key) ;
    static int32_t get_int(yyjson_val& obj) ;
    static float get_float(yyjson_val& obj) ;

    template<class Fn>
    void for_each(yyjson_val& obj, Fn&& func) const;

    yyjson_doc* doc;


  };

  typedef std::bitset<STANDARD_VECTOR_SIZE> db721_filter_t;


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



  class db721MetaData {
  public:
    string table_name;
    int32_t max_block_entrys;
    vector<ColumnMetaData> columns;

    void read(Allocator& allocator, FileHandle& file_handle,
              uint64_t metadata_pos, uint32_t footer_len);
    uint32_t NumRowGroups() const;
    idx_t NumRowsOfGroup(idx_t group) const;


  };

  class db721Reader;



class db721ScanColumData {
private:
  idx_t chunk_offset;
  idx_t chunk_size;

public:
  idx_t RowsLeftToScan() const {
    return chunk_size - chunk_offset;
  }
  void InrementOffset(idx_t count) {
    chunk_offset += count;
  }
  void SetOffset(uint64_t offset) {
    chunk_offset = offset;
  }
  void SetSize(uint64_t size) {
    chunk_size = size;
  }

  ResizeableBuffer buf;
};

  struct db721ScanBindData : public TableFunctionData {
    int64_t current_group;
    int64_t group_offset;
    unique_ptr<db721Reader> reader;
    bool finished;
    SelectionVector sel;
    vector<db721ScanColumData> column_data;
  };





  class db721Reader {
  public:
    db721Reader(ClientContext &context, string file_name);

    void InitializeSchema();
    static LogicalType DeriveLogicalType(const ColumnMetaData& column_metadata);
    unique_ptr<BaseStatistics> ReadStatistics(column_t col_idx) const;
    static unique_ptr<BaseStatistics> get_block_stats(const BlockStatsString& blockstats);
    static unique_ptr<BaseStatistics> get_block_stats(const BlockStatsFloat& blockstats);
    static unique_ptr<BaseStatistics> get_block_stats(const BlockStatsInt& blockstats);
    void ScanColumn(db721ScanBindData&, db721_filter_t& mask, idx_t count, idx_t out_col_idx, Vector& out);
    void Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    bool ScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);




    FileSystem& fs;
    Allocator& allocator;
    string filename;
    unique_ptr<FileHandle> file_handle;
    unique_ptr<db721MetaData> metadata;
    vector<LogicalType> return_types;
    vector<string> names;
    vector<column_t> column_ids;
    optional_ptr<TableFilterSet> filters;


  };


}