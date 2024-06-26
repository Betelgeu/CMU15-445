#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

// 检查表达式是否满足要求
// expr是一个二叉树，root是AND或==
// AND的子节点是AND或==
// ==的子节点是两个列(叶节点)
// 将这个二叉树转换为两组列的表达式
auto Check(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_exprs, std::vector<AbstractExpressionRef> &right_exprs) -> bool {
  bool is_and = true;
  bool is_equal = true;
  try {
    auto &root = dynamic_cast<ComparisonExpression &>(*expr);
    if(root.comp_type_ != ComparisonType::Equal) {
      return false;
    }

    auto &left_child_expr = dynamic_cast<ColumnValueExpression &>(*root.GetChildAt(0));
    auto &right_child_expr = dynamic_cast<ColumnValueExpression &>(*root.GetChildAt(1));
    if(left_child_expr.GetTupleIdx() == 0) {
      left_exprs.push_back(std::make_shared<ColumnValueExpression>(left_child_expr));
    } else {
      right_exprs.push_back(std::make_shared<ColumnValueExpression>(left_child_expr));
    }

    if(right_child_expr.GetTupleIdx() == 0) {
      left_exprs.push_back(std::make_shared<ColumnValueExpression>(right_child_expr));
    } else {
      right_exprs.push_back(std::make_shared<ColumnValueExpression>(right_child_expr));
    }
    return true;
  } catch (const std::bad_cast &) {
    is_equal = false;
  }

  try {
    auto &root = dynamic_cast<LogicExpression &>(*expr);
    if(root.logic_type_ != LogicType::And) {
      return false;
    }
    auto &left_child_expr = root.GetChildAt(0);
    auto &right_child_expr = root.GetChildAt(1);
    if(Check(left_child_expr, left_exprs, right_exprs) && Check(right_child_expr, left_exprs, right_exprs)) {
      return true;
    }
  } catch (const std::bad_cast &) {
    is_and = false;
  }

  return is_and || is_equal;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  // 先对所有子节点应用本条优化规则
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if(optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    if(Check(nlj_plan.predicate_, left_exprs, right_exprs)) {
      return std::make_shared<HashJoinPlanNode>(
        nlj_plan.output_schema_,
        nlj_plan.children_[0],
        nlj_plan.children_[1],
        left_exprs,
        right_exprs,
        nlj_plan.join_type_
      );
    }
  }

  return optimized_plan;
}

}  // namespace bustub
