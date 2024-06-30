#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  output_idx_ = 0;
  if (!output_tuples_.empty()) {
    return;
  }

  Tuple tuple;
  RID rid;
  child_executor_->Init();
  auto order_bys = plan_->GetOrderBy();
  auto n = plan_->GetN();

  auto cmp = [&](const Tuple &a, const Tuple &b) -> bool {
    for (auto &order_by : order_bys) {
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
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  while (child_executor_->Next(&tuple, &rid)) {
    if (pq.size() < n) {
      pq.push(tuple);
    } else {
      if (cmp(tuple, pq.top())) {
        pq.pop();
        pq.push(tuple);
      }
    }
  }

  while (!pq.empty()) {
    output_tuples_.push_back(pq.top());
    pq.pop();
  }
  std::reverse(output_tuples_.begin(), output_tuples_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_idx_ >= output_tuples_.size()) {
    return false;
  }
  *tuple = output_tuples_[output_idx_];
  output_idx_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return 0; };

}  // namespace bustub
