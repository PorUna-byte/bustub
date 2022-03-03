//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"
#include "common/util/hash_util.h"
namespace bustub {
  /** DistinctKey represents a key in an Distinct operation */
  struct DistinctKey {
  /** All values of a tuple*/
  std::vector<Value> vals_;

  /**
   * Compares two Distinct keys for equality.
   * @param other the other Dintinct key to be compared with
   * @return `true` if both Distinct keys have equivalent vals_ expressions, `false` otherwise
   */
  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.vals_.size(); i++) {
      if (vals_[i].CompareEquals(other.vals_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}
namespace std {

/** Implements std::hash on DistinctKey */
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &distinct_key) const {
    size_t curr_hash = 0;
    for (const auto &key : distinct_key.vals_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
namespace bustub{
/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };
  
  DistinctKey make_Distinctkey(Tuple &tuple){
    std::vector<Value> vals;
    uint32_t column_count=child_executor_->GetOutputSchema()->GetColumnCount();
    vals.reserve(column_count);
    for(uint i=0;i<column_count;i++){
      vals.push_back(tuple.GetValue(child_executor_->GetOutputSchema(),i));
    }
    return {vals};
  }
 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The hash table used to eliminate duplicate tuples*/
  std::unordered_map<DistinctKey,uint32_t> ht_;

};
}  // namespace bustub
