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
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  this->child_executor_->Init();
  this->called_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // Delete 时，并不是直接删除，而是将 tuple 标记为删除状态，也就是逻辑删除。（在事务提交后，再进行物理删除，Project 3
  // 中无需实现）
  if (called_) {
    return false;
  }
  called_ = true;
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
  auto indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int cnt = 0;
  while (this->child_executor_->Next(tuple, rid)) {
    cnt++;
    // 更新标记为删除
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);
    for (auto &index : indexes) {
      auto key = tuple->KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, this->exec_ctx_->GetTransaction());
    }
  }
  *tuple = Tuple{{{TypeId::INTEGER, cnt}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
