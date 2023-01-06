#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  child_tuples_.clear();

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

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);

  Tuple child_tuple{};
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    pq.push(child_tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }

  while (!pq.empty()) {
    child_tuples_.push_back(pq.top());
    pq.pop();
  }

  std::reverse(child_tuples_.begin(), child_tuples_.end());

  iter_ = child_tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == child_tuples_.end()) {
    return false;
  }

  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;

  return true;
}

}  // namespace bustub
