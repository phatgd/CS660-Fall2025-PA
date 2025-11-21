#include <db/Query.hpp>
#include <iostream>
#include <map>
#include <numeric>
// #include <bits/stdc++.h>

using namespace db;

// function to process predicates
// takes in predicateOp for comparison
// returns true or false dep on comparison
//@author Sam Gibson
bool predicate_results(db::PredicateOp op, field_t value, field_t comp){
  switch(op){
    case db::PredicateOp::EQ:
      return value == comp;
    case db::PredicateOp::NE:
      return value != comp;
    case db::PredicateOp::LT:
      return value < comp;
    case db::PredicateOp::LE:
      return value <= comp;
    case db::PredicateOp::GT:
      return value > comp;
    case db::PredicateOp::GE:
      return value >= comp;
    default:
      return false;
  }
}

//@author Sam Gibson
const field_t agg_operations(db::AggregateOp op, std::vector<int> stuff){
  switch(op){
    case db::AggregateOp::SUM:
      return std::accumulate(stuff.begin(), stuff.end(), 0);
    case db::AggregateOp::AVG:
      return (double) std::accumulate(stuff.begin(), stuff.end(), 0) / stuff.size();
    case db::AggregateOp::MIN:
      return *min_element(stuff.begin(), stuff.end());
    case db::AggregateOp::MAX:
      return *max_element(stuff.begin(), stuff.end());
    case db::AggregateOp::COUNT:
      return (int) stuff.size();
  }
}

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  //@author Phat Duong
  auto &in_td = in.getTupleDesc();

  for(const auto &it : in){
    std::vector<field_t> result_fields{};
    for(auto &field: field_names){
      result_fields.push_back(it.get_field(in_td.index_of(field)));
    }

    out.insertTuple(result_fields);
    
  }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  //@author Sam Gibson
  auto &in_td = in.getTupleDesc();

  for(const auto &it : in){
    bool pass = true; // if all conditions are true
    for(const auto &p: pred){
      if(!predicate_results(p.op, it.get_field(in_td.index_of(p.field_name)), p.value)){
        pass = false;
        break;
      }
    }

    if(pass){
      out.insertTuple(it);
    }
  }
    
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  //@author Sam Gibson, Phat Duong
  auto &in_td = in.getTupleDesc();
  std::map<field_t, std::vector<int>> agg_map;
 
  // construct the unordered_map
  for(const auto &it: in){
    const auto field = agg.group ? it.get_field(in_td.index_of(*agg.group)) : 0;
    const auto value = std::get<int>(it.get_field(in_td.index_of(agg.field)));
    
    // emplace will not happen if field already exists
    agg_map.emplace(field, std::vector<int>());
    agg_map[field].push_back(value);
  }

  // iterate through map and calculate results
  for (auto &group:agg_map){
    // save for results
    std::vector<field_t> result_fields{}; 
    if (agg.group){
      result_fields.push_back(group.first);
    }
    result_fields.push_back(agg_operations(agg.op, group.second));
    out.insertTuple(result_fields);
  }
}

/**
 * @brief function to merge two tuples
 * @param tuple_1 the first tuple
 * @param tuple_2 the second tuple
 * @param exclude_index the index to exclude from tuple_2
 * @return the tuple that is merged from tuple_1 and tuple_2
 */
Tuple merge_tuple(const Tuple &tuple_1, const Tuple &tuple_2, size_t exclude_index){
  // @author Phat Duong

  std::vector<field_t> merged_fields {};
  for (size_t i = 0; i<tuple_1.size(); i++){
    merged_fields.push_back(tuple_1.get_field(i));
  }

  for (size_t i = 0; i<tuple_2.size(); i++){
    if (i == exclude_index){
      continue;
    }
    merged_fields.push_back(tuple_2.get_field(i));
  }

  return merged_fields;
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // @author Phat Duong
  TupleDesc left_td = left.getTupleDesc();
  TupleDesc right_td = right.getTupleDesc();

  size_t exclude_index = -1;

  if (pred.op == PredicateOp::EQ){
    exclude_index = right_td.index_of(pred.right);
  }

  for(const auto &left_tuple : left){
    for(const auto &right_tuple : right){
      size_t right_field_index = right_td.index_of(pred.right);
      if(predicate_results(
        pred.op, 
        left_tuple.get_field(left_td.index_of(pred.left)),
        right_tuple.get_field(right_td.index_of(pred.right))
      )){
        out.insertTuple(merge_tuple(left_tuple, right_tuple, exclude_index));
      };
    }
  }
}
