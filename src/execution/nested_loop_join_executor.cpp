//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "include/type/value_factory.h"
namespace bustub {
// sql knowledge:
// 1. left join 是左表全连接右表，右表没有匹配的行，左表的行也会被保留
// 2. inner join 是左表和右表的交集 on
// 3. left join 和 inner join 的区别在于，left join 会保留左表没有匹配的行，inner join 不会保留

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_bool_ = left_executor_->Next(&left_tuple_, &left_rid_);
  left_end_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID right_rid{};
  auto predicate = plan_->Predicate();
  // 左连接的不匹配值判断在内部
  while (left_bool_) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // 判断是否满足连接条件
      auto join_result = predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                 right_executor_->GetOutputSchema());
      // 对每一个左边的元组进行判断右边有没有和它匹配的，若有join_result.GetAs为true
      if (join_result.GetAs<bool>()) {
        std::vector<Value> values;
        // 一个元组对应多个值，要把所有值都放进去
        for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
        }
        // 连接操作右边元组的值均不为null的情况下
        for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&this->right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        left_end_ = true;
        return true;
      }
    }
    // false means left table tuple has not matched tuple in right table
    // only left join needs to return tuple cons`isting of left table tuple and null values in right table
    if (!left_end_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      // 文档要求：左连接操作要求左边的元组都存在，如果不存在满足条件的右边元组，则将右边元组的值都设置为null
      for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均为null
      for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(this->right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      left_end_ = true;
      return true;
    }
    // 接着匹配左表的下个元组
    left_bool_ = left_executor_->Next(&this->left_tuple_, &this->left_rid_);
    right_executor_->Init();
    left_end_ = false;
  }
  return false;

}

}  // namespace bustub
