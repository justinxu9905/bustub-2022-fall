//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::AntiLeftJoinTuple(Tuple *left_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left_tuple->GetValue(&child_executor_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
    values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(idx).GetType()));
  }

  return Tuple{values, &GetOutputSchema()};
}

auto NestIndexJoinExecutor::InnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < child_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left_tuple->GetValue(&child_executor_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < plan_->InnerTableSchema().GetColumnCount(); idx++) {
    values.push_back(right_tuple->GetValue(&plan_->InnerTableSchema(), idx));
  }

  return Tuple{values, &GetOutputSchema()};
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID left_rid;
  RID right_rid;

  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), table_info->name_);
  Transaction *tx = GetExecutorContext()->GetTransaction();

  while (true) {
    if (!right_result_.empty()) {
      right_rid = right_result_.back();
      right_result_.pop_back();

      table_info->table_->GetTuple(right_rid, &right_tuple, tx);

      *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);
      *rid = tuple->GetRid();

      return true;
    }

    if (!child_executor_->Next(&left_tuple_, &left_rid)) {
      return false;
    }

    index_info->index_->ScanKey(
        Tuple({plan_->KeyPredicate()->Evaluate(&left_tuple_, child_executor_->GetOutputSchema())},
              &index_info->key_schema_),
        &right_result_, tx);

    if (right_result_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = AntiLeftJoinTuple(&left_tuple_);
      *rid = tuple->GetRid();

      return true;
    }
  }

  return false;
}

}  // namespace bustub
