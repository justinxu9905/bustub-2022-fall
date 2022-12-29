//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  called_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  Transaction *tx = GetExecutorContext()->GetTransaction();
  TableHeap *table_heap = table_info->table_.get();
  std::vector<IndexInfo *> index_info_vector = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);

  Tuple child_tuple{};
  int size = 0;
  while (child_executor_->Next(&child_tuple, rid)) {
    table_heap->MarkDelete(*rid, tx);

    for (auto &index_info : index_info_vector) {
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, tx);
    }

    size++;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, size);

  *tuple = Tuple{values, &GetOutputSchema()};

  if (size == 0 && !called_) {
    called_ = true;
    return true;
  }

  called_ = true;
  return size != 0;
}

}  // namespace bustub
