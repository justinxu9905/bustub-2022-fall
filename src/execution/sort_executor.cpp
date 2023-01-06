#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  child_tuples_.clear();

  Tuple child_tuple{};
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    child_tuples_.push_back(child_tuple);
  }

  auto cmp = [order_bys = plan_->order_bys_, schema = child_executor_->GetOutputSchema()](const Tuple &a,
                                                                                          const Tuple &b) {
    for (const auto &order_key : order_bys) {
      switch (order_key.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(
                  order_key.second->Evaluate(&a, schema).CompareLessThan(order_key.second->Evaluate(&b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&a, schema)
                                           .CompareGreaterThan(order_key.second->Evaluate(&b, schema)))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(
                  order_key.second->Evaluate(&a, schema).CompareGreaterThan(order_key.second->Evaluate(&b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_key.second->Evaluate(&a, schema)
                                           .CompareLessThan(order_key.second->Evaluate(&b, schema)))) {
            return false;
          }
          break;
      }
    }
    return false;
  };

  std::sort(child_tuples_.begin(), child_tuples_.end(), cmp);

  iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == child_tuples_.end()) {
    return false;
  }

  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;

  return true;
}

}  // namespace bustub
