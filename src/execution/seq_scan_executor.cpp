//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_(nullptr, RID{}, nullptr), end_(nullptr, RID{}, nullptr) {
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  if(plan_->GetPredicate()==nullptr){ 
    //if plan_ provides no predicate, then we create a constant 'true' predicate
    predicate_=new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
    is_alloc_=true; //This indicates we need to free memory for predicate_ after the class is destructed.
  }
  else
    predicate_=plan_->GetPredicate();
  
  //build outschema_idx_ from output_schema, since every plan node will spit out tuples,
  //and this tells you what schema this plan node's tuples will have.
  out_schema_idx_.reserve(plan_->OutputSchema()->GetColumnCount());
  try{
    for(const Column& out_column:plan_->OutputSchema()->GetColumns()){
      out_schema_idx_.push_back(table_info_->schema_.GetColIdx(out_column.GetName()));
    }  
  }
  catch(const std::logic_error &error){
    for(uint32_t i=0;i<plan_->OutputSchema()->GetColumnCount();i++){
      out_schema_idx_.push_back(i);
    }  
  }  
}

SeqScanExecutor::~SeqScanExecutor() {
  if(is_alloc_){
    delete predicate_;
    predicate_=nullptr;
  }  
}

void SeqScanExecutor::Init() {
  cur_=table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_=table_info_->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while(cur_!=end_){
    Value accept=predicate_->Evaluate(&(*cur_),&table_info_->schema_);
    Tuple temp=*cur_;
    cur_++;
    if(accept.GetAs<bool>()){ 
      std::vector<Value> vals;
      //Not all values of original tuple are needed to create a new tuple
      vals.reserve(plan_->OutputSchema()->GetColumnCount());
      //we just extract some columns from original tuple
      for(uint32_t i=0;i<plan_->OutputSchema()->GetColumnCount();i++){
        vals.push_back(temp.GetValue(&table_info_->schema_,out_schema_idx_[i]));
      }  
      *tuple=Tuple(vals,plan_->OutputSchema());
      *rid=temp.GetRid();
      return true;
    }  
  }  
  return false;
}

}  // namespace bustub
