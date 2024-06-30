#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  output_idx_ = 0;
  if (!output_tuples_.empty()) {
    return;
  }

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    output_tuples_.push_back(tuple);
  }
  // sort
  std::sort(output_tuples_.begin(), output_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto &order_by : plan_->GetOrderBy()) {
      auto order_by_type = order_by.first;
      auto expr_ref = order_by.second;
      auto left = expr_ref->Evaluate(&a, child_executor_->GetOutputSchema());
      auto right = expr_ref->Evaluate(&b, child_executor_->GetOutputSchema());

      BUSTUB_ASSERT(order_by_type != OrderByType::INVALID, "Invalid OrderByType");
      BUSTUB_ASSERT(left.CheckComparable(right), "Cannot compare left and right");

      if (left.CompareNotEquals(right) == CmpBool::CmpTrue) {
        if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
          return left.CompareLessThan(right) == CmpBool::CmpTrue;
        }
        if (order_by_type == OrderByType::DESC) {
          return left.CompareLessThan(right) == CmpBool::CmpFalse;
        }
      }
    }
    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_idx_ >= output_tuples_.size()) {
    return false;
  }
  *tuple = output_tuples_[output_idx_];
  output_idx_++;
  return true;
}

}  // namespace bustub
