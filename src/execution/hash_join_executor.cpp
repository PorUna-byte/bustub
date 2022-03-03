//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),plan_(plan),
    left_child_(std::move(left_child)),right_child_(std::move(right_child)) {
  }
//We build the hash table during Init phase.
void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  bkt_idx_=0;
  Tuple left_tuple;
  RID left_rid;
  //Hash all left tuples into the hash table
  while(left_child_->Next(&left_tuple,&left_rid)){
    HashJoinKey hash_key{plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema())};
    std::vector<Value> copy_vals_from_tuple;
    uint32_t left_columns_count=plan_->GetLeftPlan()->OutputSchema()->GetColumnCount();
    copy_vals_from_tuple.reserve(left_columns_count);
    for(uint32_t i=0;i<left_columns_count;i++){
      copy_vals_from_tuple.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(),i)); 
    }  
    HashJoinValue hash_value{copy_vals_from_tuple};
    // Insert {hash_key,hash_value} into the hash table
    if(hash_table_.count(hash_key)>0){
      hash_table_[hash_key].push_back(std::move(hash_value));
    }
    else
      hash_table_.insert({hash_key,{hash_value}});  
  }
}

//Try every right tuple, hash it into the hash_table to find the 
//corresponding left tuples(i.e. the corresponding bucket).
bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) { 
  bool is_find=false;
  if(bkt_idx_>=cur_bucket_.size()){
    //we need try next right tuple until we find a bucket that contains some left tuples.
    while(right_child_->Next(&right_tuple_,&right_rid_)){
      HashJoinKey key{plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, plan_->GetRightPlan()->OutputSchema())};
      auto iter=hash_table_.find(key);
      //the corresponding bucket has some left tuples, update the cur_bucket_.
      if(iter!=hash_table_.end()){
        cur_bucket_=iter->second;
        bkt_idx_=0;
        is_find=true;
        break;
      }
    }
    if(!is_find)
      return false;
  }
  std::vector<Value> out_vals{};
  out_vals.reserve(plan_->OutputSchema()->GetColumnCount());
  for(auto column:plan_->OutputSchema()->GetColumns()){
    auto column_expr=reinterpret_cast<const ColumnValueExpression*>(column.GetExpr());
    //tuple index 0 = left side of join, tuple index 1 = right side of join
    if(column_expr->GetTupleIdx()==0){
      out_vals.push_back(cur_bucket_[bkt_idx_].vals_[column_expr->GetColIdx()]);
    }
    else{
      out_vals.push_back(right_tuple_.GetValue(plan_->GetRightPlan()->OutputSchema(),column_expr->GetColIdx()));
    }
  }
  bkt_idx_++;
  *tuple=Tuple(out_vals,plan_->OutputSchema());
  return true; 
}

}  // namespace bustub
