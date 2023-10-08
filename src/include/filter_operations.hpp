#pragma once
#include "duckdb/planner/filter/constant_filter.hpp"

// basically all copied from parquet reader

namespace duckdb {

template <class T, class OP>
void TemplatedFilterOperation(Vector &v, T constant,
                              db721_filter_t &filter_mask, idx_t count) {
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
void TemplatedFilterOperation(Vector &v, const Value &constant,
                              db721_filter_t &filter_mask, idx_t count) {
  TemplatedFilterOperation<T, OP>(v, constant.template GetValueUnsafe<T>(),
                                  filter_mask, count);
}

template <class OP>
static void FilterOperationSwitch(Vector &v, Value &constant,
                                  db721_filter_t &filter_mask, idx_t count) {
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
    throw NotImplementedException("Unsupported type for filter %s",
                                  v.ToString());
  }
}

static void FilterIsNotNull(Vector &v, db721_filter_t &filter_mask,
                            idx_t count) {
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

static void ApplyFilter(Vector &v, TableFilter &filter,
                        db721_filter_t &filter_mask, idx_t count) {
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
      FilterOperationSwitch<Equals>(v, constant_filter.constant, filter_mask,
                                    count);
      break;
    case ExpressionType::COMPARE_LESSTHAN:
      FilterOperationSwitch<LessThan>(v, constant_filter.constant, filter_mask,
                                      count);
      break;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      FilterOperationSwitch<LessThanEquals>(v, constant_filter.constant,
                                            filter_mask, count);
      break;
    case ExpressionType::COMPARE_GREATERTHAN:
      FilterOperationSwitch<GreaterThan>(v, constant_filter.constant,
                                         filter_mask, count);
      break;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      FilterOperationSwitch<GreaterThanEquals>(v, constant_filter.constant,
                                               filter_mask, count);
      break;
    default:
      D_ASSERT(0);
    }
    break;
  }
  case TableFilterType::IS_NOT_NULL:
    FilterIsNotNull(v, filter_mask, count);
    break;
  case TableFilterType::IS_NULL: {
    FilterIsNull(v, filter_mask, count);
    break;
  }
  default:
    D_ASSERT(0);
    break;
  }
}
}
