#include <memory>
#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }
  // 这里只需要支持类似于 WHERE v = 1 的情况
  try {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if(seq_scan_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }

    const auto *cmp_filter = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
    if(cmp_filter == nullptr || cmp_filter->comp_type_ != ComparisonType::Equal) {
      return optimized_plan;
    }

    auto *col_expr = dynamic_cast<const ColumnValueExpression *>(cmp_filter->GetChildAt(0).get());
    if(col_expr == nullptr) {
      return optimized_plan;
    }
    auto filter_column_id = col_expr->GetColIdx();

    // 查找是否存在对应的索引
    auto table_info = catalog_.GetTable(seq_scan_plan.table_oid_);
    auto indices = catalog_.GetTableIndexes(table_info->name_);
    for(auto *index_info : indices) {
      auto columns = index_info->index_->GetKeyAttrs();
      // 只招在一个column上的index
      if(columns.size() != 1) {
        continue;
      }
      // 如果找到了对应的index，返回IndexScanPlanNode
      if(columns[0] == filter_column_id) {
        auto const_ref = dynamic_cast<ConstantValueExpression *>(cmp_filter->GetChildAt(1).get());
        if(const_ref == nullptr) {
          return optimized_plan;
        }
        return std::make_shared<IndexScanPlanNode>(
          seq_scan_plan.output_schema_,
          seq_scan_plan.table_oid_,
          index_info->index_oid_,
          seq_scan_plan.filter_predicate_,
          const_ref
        );
      }
    }
  }
  catch (const std::bad_cast &) {
    // 在发生 std::bad_cast 异常时，直接返回 optimized_plan
    return optimized_plan;
  }
  return optimized_plan;

  // if(optimized_plan->GetType() == PlanType::SeqScan){
  //   const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  //   if(seq_scan_plan.filter_predicate_ != nullptr){
  //     // 这里只需要支持类似于 WHERE v = 1 的情况
  //     const auto *cmp_filter = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
  //     if(cmp_filter != nullptr && cmp_filter->comp_type_ == ComparisonType::Equal){
  //       auto *col_expr = dynamic_cast<const ColumnValueExpression *>(cmp_filter->GetChildAt(0).get());
  //       auto filter_column_id = col_expr->GetColIdx();
  //       // 查找是否存在对应的索引
  //       auto table_info = catalog_.GetTable(seq_scan_plan.table_oid_);
  //       auto indices = catalog_.GetTableIndexes(table_info->name_);
  //       for(auto *index_info : indices) {
  //         auto columns = index_info->index_->GetKeyAttrs();
  //         if(columns.size() != 1){
  //           continue;
  //         }
  //         if(columns[0] == filter_column_id){
  //           return std::make_shared<IndexScanPlanNode>(
  //             seq_scan_plan.output_schema_,
  //             seq_scan_plan.table_oid_,
  //             index_info->index_oid_,
  //             seq_scan_plan.filter_predicate_,
  //             dynamic_cast<ConstantValueExpression *>(cmp_filter->GetChildAt(1).get())
  //           );
  //         }
  //       }
  //     }
  //   }
  // }
  // return optimized_plan;
}

}  // namespace bustub
