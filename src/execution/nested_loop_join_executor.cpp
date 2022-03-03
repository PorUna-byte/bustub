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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor)) {   
  if(plan_->Predicate()==nullptr){ 
    //if plan_ provides no predicate, then we create a constant 'true' predicate
    predicate_=new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
    is_alloc_=true; //This indicates we need to free memory for predicate_ after the class is destructed.
  }
  else
    predicate_=plan_->Predicate();    
}

NestedLoopJoinExecutor::~NestedLoopJoinExecutor(){
  if(is_alloc_){
    delete predicate_;
    predicate_=nullptr;
  }
}
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  is_left_select=left_executor_->Next(&left_tuple_,&left_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) { 
  if(!is_left_select) //There is nothing in left table
    return false;
  Tuple right_tuple;
  RID right_rid;  
  while(true){
    while(!right_executor_->Next(&right_tuple,&right_rid)){ 
      //try next left tuple
      if(!left_executor_->Next(&left_tuple_,&left_rid_)){
        is_left_select=false;  //The join is finished.
        return false;
      }
      right_executor_->Init(); //start scan right table over again for each left tuple 
    }
    Value satisfied= predicate_->EvaluateJoin(&left_tuple_,left_executor_->GetOutputSchema(),
    &right_tuple,right_executor_->GetOutputSchema());
    if(satisfied.GetAs<bool>()){
      std::vector<Value>out_tuple_vals;
      out_tuple_vals.reserve(plan_->OutputSchema()->GetColumnCount());
      for(Column column:plan_->OutputSchema()->GetColumns()){
        auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
        if(column_expr->GetTupleIdx()==0){ //This output column comes from left child
          out_tuple_vals.push_back(left_tuple_.GetValue(left_executor_->GetOutputSchema(),column_expr->GetColIdx()));
        }
        else{
          out_tuple_vals.push_back(right_tuple.GetValue(right_executor_->GetOutputSchema(),column_expr->GetColIdx()));
        }  
      }
      *tuple=Tuple(out_tuple_vals,plan_->OutputSchema());
      *rid=tuple->GetRid();
      return true;  
    }  
  }    
  return false; 
}
}  // namespace bustub
