//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

//Exclusive lock treat transactions with different isolation-level differently
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  //A transaction in READ_UNCOMMITTED iso-level can't acquire a shared lock,
  //since it allows dirty read.
  if(txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED){
    Implicit_Abort(txn,AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  //A transaction in REPEATABLE_READ iso-level must conform with two-phase-locking protocol to ensure repeatable read.
  //Hence, In shrinking phase, the transaction can't acquire any lock.
  if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ&&txn->GetState()==TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    // Implicit_Abort(txn,AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  //If is already locked, just return true
  if(txn->IsSharedLocked(rid)||txn->IsExclusiveLocked(rid))
    return true;
  //Use LM's latch_ to protect internal data structure ---lock_table_, since multiple transactions will
  //require LM for lock_table_ concurrently.
  latch_.lock();
  txn_map_[txn->GetTransactionId()]=txn; 
  auto &request_queue=lock_table_[rid];
  latch_.unlock();
  //before we access a request_queue, we must get its internal latch
  std::unique_lock<std::mutex> queue_lock(request_queue.latch_);
  auto &request=request_queue.request_queue_.emplace_back(txn->GetTransactionId(),LockMode::SHARED);

  WOUND_WAIT(txn,LockMode::SHARED,request_queue);
  //We need to wait until no other transaction is writing or This transaction is aborted
  request_queue.cv_.wait(queue_lock,[txn,&request_queue]()->bool{
    return (txn->GetState()==TransactionState::ABORTED||!request_queue.is_writing_);
  });
  //This transaction is aborted by other old transaction
  if(check_deadLock_abort(txn,request_queue)){
    return false;
  }
  //Now we can grant shared_lock to this txn
  request_queue.shared_read_++;
  request.granted_=true;
  txn->GetSharedLockSet()->emplace(rid);
  //When return, request_queue's internal latch will be released automatically
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  //Only transactions with repeatable read isolation-level need to comply with 2PL
  if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ&&txn->GetState()==TransactionState::SHRINKING){
    Implicit_Abort(txn,AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if(txn->IsExclusiveLocked(rid))
    return true;
  latch_.lock();
  txn_map_[txn->GetTransactionId()]=txn; 
  auto &request_queue=lock_table_[rid];
  latch_.unlock();
  std::unique_lock<std::mutex> queue_lock(request_queue.latch_);
  auto &request=request_queue.request_queue_.emplace_back(txn->GetTransactionId(),LockMode::EXCLUSIVE);

  WOUND_WAIT(txn,LockMode::EXCLUSIVE,request_queue);
  //wait until no other txn is writing or reading
  request_queue.cv_.wait(queue_lock,[txn,&request_queue]()->bool{
    return txn->GetState()==TransactionState::ABORTED||
    (!request_queue.is_writing_&&request_queue.shared_read_==0);
  });

  if(check_deadLock_abort(txn,request_queue)){
    return false;
  }
  request.granted_=true;
  request_queue.is_writing_=true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  //Only transactions with repeatable read isolation-level need to comply with 2PL
  if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ&&txn->GetState()==TransactionState::SHRINKING){
    Implicit_Abort(txn,AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if(txn->IsExclusiveLocked(rid))
    return true;
  //The txn must hold shared lock before  
  if(!txn->IsSharedLocked(rid))
    return false;  
  latch_.lock();
  auto &request_queue=lock_table_[rid];
  latch_.unlock();

  std::unique_lock<std::mutex> queue_lock(request_queue.latch_);
  if(request_queue.upgrading_!=INVALID_TXN_ID){
    Implicit_Abort(txn,AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  auto iter=std::find_if(request_queue.request_queue_.begin(),request_queue.request_queue_.end(),
  [txn](const LockRequest& request)->bool{
    return request.txn_id_==txn->GetTransactionId()&&request.lock_mode_==LockMode::SHARED;
  });
  iter->granted_=false;
  iter->lock_mode_=LockMode::EXCLUSIVE;
  request_queue.shared_read_--;
  txn->GetSharedLockSet()->erase(rid);
  //Indicate there is an upgrading on the record, so other upgrade requests will fail immediately
  request_queue.upgrading_=txn->GetTransactionId();

  WOUND_WAIT(txn,LockMode::EXCLUSIVE,request_queue);

  //Wait until we can acquire an exclusive lock on the record  
  request_queue.cv_.wait(queue_lock,[txn,&request_queue]()->bool{
    return txn->GetState()==TransactionState::ABORTED||(!request_queue.is_writing_&&request_queue.shared_read_==0);
  });
  if(check_deadLock_abort(txn,request_queue)){
    request_queue.upgrading_=INVALID_TXN_ID;
    return false;
  }
  iter->granted_=true;
  request_queue.upgrading_=INVALID_TXN_ID;
  request_queue.is_writing_=true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  //Only transactions with repeatable read isolation-level need to comply with 2PL
  if(txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ&&txn->GetState()==TransactionState::GROWING)
    txn->SetState(TransactionState::SHRINKING);
  latch_.lock();
  auto &request_queue=lock_table_[rid];
  latch_.unlock();
  std::unique_lock<std::mutex> queue_lock(request_queue.latch_);
  auto iter=std::find_if(request_queue.request_queue_.begin(),request_queue.request_queue_.end(),
  [txn](LockRequest& request)->bool{
    return txn->GetTransactionId()==request.txn_id_;
  });
  if(iter!=request_queue.request_queue_.end()){
    request_queue.request_queue_.erase(iter);
  }
  else
    return false;
  if(txn->IsSharedLocked(rid)){
    txn->GetSharedLockSet()->erase(rid);
    request_queue.shared_read_--;
  }
  if(txn->IsExclusiveLocked(rid)){
    txn->GetExclusiveLockSet()->erase(rid);
    request_queue.is_writing_=false;
  }
  if(!request_queue.is_writing_)
    request_queue.cv_.notify_all();
  return true;
}
void LockManager::Implicit_Abort(Transaction* txn,AbortReason reason){
  txn->SetState(TransactionState::ABORTED);
  throw new TransactionAbortException(txn->GetTransactionId(),reason);
}
bool LockManager::check_deadLock_abort(Transaction* txn,LockRequestQueue& request_queue){
  if(txn->GetState()==TransactionState::ABORTED){
    request_queue.request_queue_.remove_if([&txn](const LockRequest& it)->bool{
      return txn->GetTransactionId()==it.txn_id_;
    });
    throw new TransactionAbortException(txn->GetTransactionId(),AbortReason::DEADLOCK);
    return true;
  }
  return false;
}
//Young wait for old, and old kills young
//small txn_id for old, big txn_id for young
void LockManager::WOUND_WAIT(Transaction* txn,LockMode mode,LockRequestQueue&  request_queue_){
  for(const auto &request:request_queue_.request_queue_){
    if(txn->GetTransactionId()<request.txn_id_&&request.granted_){
      if(mode==LockMode::EXCLUSIVE||request.lock_mode_==LockMode::EXCLUSIVE||request.txn_id_==request_queue_.upgrading_)
      {
        if(request.lock_mode_==LockMode::SHARED)
          request_queue_.shared_read_--;
        else  
          request_queue_.is_writing_=false;
        txn_map_[request.txn_id_]->SetState(TransactionState::ABORTED);  
      }
    }
  }
}
}  // namespace bustub
