#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeMergeFilterNLJ(p);
  // p = OptimizeNLJPickIndex(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

auto Optimizer::OptimizeNLJPickIndex(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJPickIndex(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&nlj_plan.Predicate()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (nlj_plan.GetJoinType() == JoinType::INNER) {
          if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
              left_expr != nullptr) {
            if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
                right_expr != nullptr) {
              // Ensure left child is table scan
              if (nlj_plan.GetLeftPlan()->GetType() == PlanType::SeqScan) {
                if (nlj_plan.GetRightPlan()->GetType() == PlanType::SeqScan) {  // skip if right table is indexed
                  const auto &right_seq_scan = dynamic_cast<const SeqScanPlanNode &>(*nlj_plan.GetRightPlan());
                  if (auto index = MatchIndex(right_seq_scan.table_name_, right_expr->GetColIdx());
                      index != std::nullopt) {
                    return optimized_plan;
                  }
                }

                const auto &left_seq_scan = dynamic_cast<const SeqScanPlanNode &>(*nlj_plan.GetLeftPlan());
                if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
                  if (auto index = MatchIndex(left_seq_scan.table_name_, right_expr->GetColIdx());
                      index != std::nullopt) {
                    auto [index_oid, index_name] = *index;
                    return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetRightPlan(),
                                                                    nlj_plan.GetLeftPlan(), nlj_plan.predicate_,
                                                                    nlj_plan.GetJoinType());
                  }
                }
                if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
                  if (auto index = MatchIndex(left_seq_scan.table_name_, left_expr->GetColIdx());
                      index != std::nullopt) {
                    auto [index_oid, index_name] = *index;
                    return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetRightPlan(),
                                                                    nlj_plan.GetLeftPlan(), nlj_plan.predicate_,
                                                                    nlj_plan.GetJoinType());
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
