#include <db/Query.hpp>
#include <iostream>
#include <set>
#include <list>

#include <unordered_map>
#include <bits/stdc++.h>

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
void agg_operations(db::AggregateOp op, std::vector<double> stuff, double &answ){
  switch(op){
    case db::AggregateOp::SUM:
      for(auto x: stuff){ answ += x; }
      break;
    case db::AggregateOp::AVG:
      for(auto x: stuff){ answ += x; }
      answ = answ/ stuff.size();
      break;
    case db::AggregateOp::MIN:
      answ = *min_element(stuff.begin(), stuff.end());
      break;
    case db::AggregateOp::MAX:
      answ = *max_element(stuff.begin(), stuff.end());
      break;
    case db::AggregateOp::COUNT:
      answ = stuff.size();
      break;
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

    out.insertTuple(Tuple(result_fields));
    
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
      out.insertTuple(Tuple(it));
    }
  }
    
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  //@author Sam Gibson
  auto &in_td = in.getTupleDesc();
  std::vector<field_t> result_fields{}; // save for results


  std::unordered_map<std::string, std::vector<double>> blob = {};
  std::string none = "none";
  blob.insert(none, std::vector<double>()); // insert no group array


  for(auto &it: in){
    if(!agg.group){
      auto foo = blob.find("none");
      foo->second.push_back(it.get_field(in_td.index_of(agg.field)));
    }
    else{
      auto foo = blob.find(it.get_field(in_td.index_of(*agg.group)));
      if(foo != blob.end()) {
        foo->second.push_back(it.get_field(in_td.index_of(agg.field)));
      }
      else{
        foo.insert(it.get_field(in_td.index_of(*agg.group)), it.get_field(in_td.index_of(agg.field)));
      }
      
    }
  }

  if(agg.group){
    for(auto x: blob){
      double answ = 0.0;
      agg_operations(agg.op, x.second, answ);
      x.second.clear();
      x.second.push_back(answ);
    }
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

  return Tuple(merged_fields);
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
