//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  next_pos_=0;
  if(!plan_->IsRawInsert())
    child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool is_inserted=false; 
  bool RID_valid=false;
  auto txn=exec_ctx_->GetTransaction();  
  if(plan_->IsRawInsert()){
    if(next_pos_<plan_->RawValues().size()){
      auto vals= plan_->RawValues();
      *tuple=Tuple(vals[next_pos_++],&table_info_->schema_);
      is_inserted=table_info_->table_->InsertTuple(*tuple,rid,exec_ctx_->GetTransaction()); 
    }
  }  
  else if(child_executor_->Next(tuple,rid))
  { 
    RID_valid=true;
    is_inserted=table_info_->table_->InsertTuple(*tuple,rid,exec_ctx_->GetTransaction());
  }
  //You will need to update all indexes for the table into which tuples are inserted.
  if(is_inserted){
    //Acquire an exclusive lock before we modify the table.
    if(!exec_ctx_->GetLockManager()->LockExclusive(txn,*rid)){
      exec_ctx_->GetTransactionManager()->Abort(txn);
      return false;
    }
    if(RID_valid)
      txn->AppendTableWriteRecord({*rid,WType::INSERT,*tuple,table_info_->table_.get()});
    //now we need to update all indexes
    for(IndexInfo* index:indexs_){
      Tuple index_key=tuple->KeyFromTuple(table_info_->schema_,
      index->key_schema_,index->index_->GetKeyAttrs());  
      index->index_->InsertEntry(index_key,*rid,exec_ctx_->GetTransaction());
    }  
    //Only repeatable read need to comply with 2PL
    if(RID_valid&&txn->GetIsolationLevel()!=IsolationLevel::REPEATABLE_READ)
      exec_ctx_->GetLockManager()->Unlock(txn,*rid); 

    //Only repeatable read need to comply with 2PL
    if(RID_valid&&txn->GetIsolationLevel()!=IsolationLevel::REPEATABLE_READ)
      exec_ctx_->GetLockManager()->Unlock(txn,*rid);        
  }     
  return is_inserted;  
}    

}  // namespace bustub
