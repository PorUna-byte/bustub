//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) { 
  while(child_executor_->Next(tuple,rid)){
    DistinctKey key=make_Distinctkey(*tuple);
    if(ht_.count(key)<=0){ //the tuple has not been emit before.
      ht_.insert({key,1});
      return true;
    }
  }
  return false; 
}

}  // namespace bustub
