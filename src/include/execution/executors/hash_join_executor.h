//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "common/util/hash_util.h"
#include "execution/expressions/column_value_expression.h"
namespace bustub {
  /** HashJoinKey represents a key in an Hash-Join operation */
  struct HashJoinKey {
  /** The HashJoin key is constructed by LeftJoinKeyExpression and RightJoinKeyExpression */
  Value key_;

  /**
   * Compares two HashJoin key for equality.
   * @param other the other HashJoin key to be compared with
   * @return `true` if both HashJoin keys have equivalent key_ expressions, `false` otherwise
   */
  bool operator==(const HashJoinKey &other) const {
    return key_.CompareEquals(other.key_) == CmpBool::CmpTrue;
  }
};
/** HashJoinValue represents a value for each of the value of a tuple */
struct HashJoinValue {
  /** The tuple's all values */
  std::vector<Value> vals_;
};

}
namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  std::size_t operator()(const bustub::HashJoinKey &hash_key) const {
    size_t curr_hash = 0;
    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&hash_key.key_));
    return curr_hash;
  }
};

}  // namespace std
namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  /** The HashTable that maps a hash(HashJoinKey), which is a size_t to a vector of left_tuples
   *  Note that a HashJoinValue is corresponding to a left_tuple.
   */
  std::unordered_map<HashJoinKey, std::vector<HashJoinValue>> hash_table_{};
  /** current bucket contains a vector of left tuples.
   *  It means the bucket that the considered right tuple being hashed in.
   *  we need to try out this bucket before we consider next right tuple.
  */
  std::vector<HashJoinValue> cur_bucket_{};
  /** This indicate next left tuple we need to consider in current bucket.
   *  if(bkt_idx>=cur_bucket_.size()) we know we have already tried out current bucket.
  */
  uint32_t bkt_idx_=0;

  Tuple right_tuple_{};
  RID right_rid_{};
};
}  // namespace bustub