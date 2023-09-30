#pragma once

#include "duckdb.hpp"

namespace duckdb {

class Db721Extension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
