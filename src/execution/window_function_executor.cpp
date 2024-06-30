#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  for (auto &pair : plan_->window_functions_) {
    auto idx = pair.first;
    auto window_function = pair.second;
    auto type = window_function.type_;
    auto win_ht = SimpleWindowHashTable(type);
    win_ht_.insert({idx, win_ht});
  }
}

void WindowFunctionExecutor::Init() {
  output_idx_ = 0;
  if (!output_tuples_.empty()) {
    return;
  }

  child_executor_->Init();
  // 1. sort as order bys
  Tuple tuple;
  RID rid;
  std::vector<Tuple> tuples;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.push_back(tuple);
  }

  auto &order_bys = plan_->window_functions_.begin()->second.order_by_;
  if (!order_bys.empty()) {
    std::sort(tuples.begin(), tuples.end(), [&](const Tuple &a, const Tuple &b) {
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
    });
  }
  // 2. initial for every partition for every window function
  // if partition by is not empty, we need to initialize the hash table
  else {
    for (const auto &tuple : tuples) {
      for (uint32_t i = 0; i < plan_->columns_.size(); i++) {
        if (win_ht_.find(i) != win_ht_.end()) {
          auto it = win_ht_.find(i);
          auto &win_ht = it->second;
          auto win = plan_->window_functions_.find(i)->second;
          auto partition = win.partition_by_;
          auto key = MakeAggregateKey(&tuple, partition);
          auto value = MakeAggregateValue(&tuple, win.function_);
          win_ht.InsertCombine(key, value);
        }
      }
    }
  }

  // 3. combine & record partition value for every row
  for (const auto &tuple : tuples) {
    std::vector<Value> output_tuple_values;
    output_tuple_values.reserve(plan_->columns_.size());
    for (uint32_t i = 0; i < plan_->columns_.size(); i++) {
      Value tuple_value;
      if (win_ht_.find(i) == win_ht_.end()) {
        tuple_value = plan_->columns_[i]->Evaluate(&tuple, child_executor_->GetOutputSchema());
      } else {
        auto it = win_ht_.find(i);
        auto &win_ht = it->second;
        auto win = plan_->window_functions_.find(i)->second;
        auto partition = win.partition_by_;
        auto key = MakeAggregateKey(&tuple, partition);
        auto value = MakeAggregateValue(&tuple, win.function_);
        if (!order_bys.empty()) {
          if (win.type_ == WindowFunctionType::Rank) {
            AggregateKey now_order_by_value = MakeAggregateValueFromOrderBy(&tuple);
            if (!win_ht.CompareOrderBy(key, now_order_by_value)) {
              auto &last_order_count = win_ht.ht_last_order_count_[key];
              auto &last_order = win_ht.ht_last_order_[key];
              if (last_order_count == 0) {
                last_order_count = 1;
              }

              auto increase = ValueFactory::GetIntegerValue(last_order_count);
              win_ht.InsertCombine(key, increase);

              last_order = now_order_by_value;
              last_order_count = 1;
            } else {
              auto &last_order_count = win_ht.ht_last_order_count_[key];
              last_order_count++;
            }
          } else {
            win_ht.InsertCombine(key, value);
          }
        }
        tuple_value = win_ht.OutputWinValue(key);
      }
      output_tuple_values.push_back(tuple_value);
    }
    output_tuples_.emplace_back(output_tuple_values, &plan_->OutputSchema());
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_idx_ >= output_tuples_.size()) {
    return false;
  }
  *tuple = output_tuples_[output_idx_];
  output_idx_++;
  return true;
}

}  // namespace bustub
