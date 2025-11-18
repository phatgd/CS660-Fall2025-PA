#include <db/Query.hpp>
#include <iostream>
#include <set>
#include <unordered_map>
#include <algorithm>

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
      answ = accumulate(stuff.first(), stuff.last(), 0);
      break;
    case db::AggregateOp::AVG:
      answ = accumulate(stuff.first(), stuff.last(), 0)/ stuff.size();
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
  // TODO: Implement this function
  //@author Sam Gibson
  auto &in_td = in.getTupleDesc();

  if(pred.size() == 0){
    for(const auto &it : in){
      std::vector<field_t> result_fields{};
      for(int x = 0; x< it.size(); x++){
        result_fields.push_back(it.get_field(x));
      }
      out.insertTuple(Tuple(result_fields));
    }
  }
  else{ // if not empty loop through and check stuff
    for(const auto &it : in){
      std::vector<field_t> result_fields{};
      bool pass = true; // if all conditions are true
      
      for(const auto &p: pred){
        if(!(pass = predicate_results(p.op, p.value, it.get_field(in_td.index_of(p.field_name))))){
          break;
        }
        result_fields.push_back(it.get_field(in_td.index_of(p.field_name)));
      }
      if(pass){
        out.insertTuple(Tuple(result_fields));
        std::cout<< pass<< ": pushed";
      }
    }
  }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  //@author Sam Gibson
  auto &in_td = in.getTupleDesc();
  std::vector<field_t> result_fields{}; // save for results


  std::unordered_map<std::string, std::vector<double>> blob = {};
  blob['none'] = std::vector<double>(); // insert no group array


  for(auto &it: in){
    if(!agg.group){
      blob['none'].push_back(it.get_field(in_td.index_of(agg.field)));
    }
    else{
      blob[it.get_field(in_td.index_of(*agg.group))].push_back(it.get_field(in_td.index_of(agg.field)));
    }
  }

  if(agg.group){
    for(int x = 0; x< blob.size(); x++){
      
    }
  }
   
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function

  /*
  
  */
}
