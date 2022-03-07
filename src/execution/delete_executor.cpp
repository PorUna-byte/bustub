//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool is_deleted=false; 
  if(child_executor_->Next(tuple,rid)){
    auto txn=exec_ctx_->GetTransaction();
    if(txn->IsSharedLocked(*rid)){
      if(!exec_ctx_->GetLockManager()->LockUpgrade(txn,*rid)){
        exec_ctx_->GetTransactionManager()->Abort(txn);
        return false;
      }
    }
    else{
      if(!exec_ctx_->GetLockManager()->LockExclusive(txn,*rid)){
        exec_ctx_->GetTransactionManager()->Abort(txn);
        return false;
      }
    }
    is_deleted=table_info_->table_->MarkDelete(*rid,exec_ctx_->GetTransaction());
    if(is_deleted){
      // txn->AppendTableWriteRecord({*rid,WType::DELETE,*tuple,table_info_->table_.get()});
      for(IndexInfo* index:indexs_){
        Tuple key=tuple->KeyFromTuple(table_info_->schema_,index->key_schema_,index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key,*rid,exec_ctx_->GetTransaction());
      }
    }
    //Only repeatable read need to comply with 2PL
    if(txn->GetIsolationLevel()!=IsolationLevel::REPEATABLE_READ)
      exec_ctx_->GetLockManager()->Unlock(txn,*rid);  
  }  
  return is_deleted; 
}

}  // namespace bustub
