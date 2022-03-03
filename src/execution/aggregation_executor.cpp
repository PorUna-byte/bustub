//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),plan_(plan),child_(std::move(child)),
    aht_(plan_->GetAggregates(),plan_->GetAggregateTypes()),aht_iterator_(aht_.Begin()) {
  if(plan_->GetHaving()==nullptr){
    predicate_=new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
    is_allo_=true;
  }
  else
    predicate_=plan_->GetHaving();
}
AggregationExecutor::~AggregationExecutor(){
  if(is_allo_){
    delete predicate_;
    predicate_=nullptr;
  }
}
void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  //wheather need to group by
  bool is_group_by =!plan_->GetGroupBys().empty();
  //Note that this aggr_key is just a place holder, if there is no group by, 
  //we assume all tuples are aggregated together
  AggregateKey aggr_key{{ValueFactory::GetBooleanValue(false)}}; 
  //build the hash table
  while(child_->Next(&tuple,&rid)){
    if(is_group_by){
      aggr_key=MakeAggregateKey(&tuple);
    }
    AggregateValue aggr_val=MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggr_key,aggr_val);
  }
  aht_iterator_=aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while(aht_iterator_!=aht_.End()){
    Value satisfied=predicate_->EvaluateAggregate(aht_iterator_.Key().group_bys_,aht_iterator_.Val().aggregates_);
    //check if the tuple can be emit
    if(satisfied.GetAs<bool>()){
      std::vector<Value> out_vals;
      out_vals.reserve(plan_->OutputSchema()->GetColumnCount());
      for (const auto &column : plan_->OutputSchema()->GetColumns()) {
        auto agg_expr = reinterpret_cast<const AggregateValueExpression *>(column.GetExpr());
        out_vals.push_back(agg_expr->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
      }
      *tuple = Tuple(out_vals, plan_->OutputSchema());
      ++aht_iterator_;
      return true;  
    }
    ++aht_iterator_;;
  }
  return false; 
}
const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
