//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexs_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool is_update=false; 
  if(child_executor_->Next(tuple,rid)){
    Tuple update_tuple=GenerateUpdatedTuple(*tuple);
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
    is_update=table_info_->table_->UpdateTuple(update_tuple,*rid,exec_ctx_->GetTransaction());
    if(is_update){
      txn->AppendTableWriteRecord({*rid,WType::UPDATE,*tuple,table_info_->table_.get()});
      for(IndexInfo* index:indexs_){
        Tuple update_tuple_key=
        update_tuple.KeyFromTuple(table_info_->schema_,index->key_schema_,index->index_->GetKeyAttrs());
        Tuple old_tuple_key=
        tuple->KeyFromTuple(table_info_->schema_,index->key_schema_,index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(old_tuple_key,*rid,exec_ctx_->GetTransaction());
        index->index_->InsertEntry(update_tuple_key,update_tuple.GetRid(),exec_ctx_->GetTransaction());
      }
    }
    //Only repeatable read need to comply with 2PL
    if(txn->GetIsolationLevel()!=IsolationLevel::REPEATABLE_READ)
      exec_ctx_->GetLockManager()->Unlock(txn,*rid);    
  }  
  return is_update; 
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  //scan all values of a tuple to see if it needs update and how to update
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) { //The value doesn't need to update
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      //update the value according to its update type
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
