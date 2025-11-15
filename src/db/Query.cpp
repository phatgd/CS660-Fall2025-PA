#include <db/Query.hpp>
#include <iostream>
#include <set>

using namespace db;

// function to process predicates
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
  return false;
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
  auto &in_td = in.getTupleDesc();

  // if empty add all to out
  if(pred.empty()){
    for(const auto &it : in){
      std::vector<field_t> result_fields{};
      for(int x = 0; x< it.size(); x++){
        result_fields.push_back(it.get_field(x));
      }
      
      out.insertTuple(Tuple(result_fields));

    }

  }
  else{
    // check what columns filters are on
    std::set<std::string> cols{};
    
    for(auto &p: pred){
      cols.insert(p.field_name);
    }

    for(const auto &it : in){
      std::vector<field_t> result_fields{};
      // if not empty loop through and check stuff
      for(auto &p: pred){
        if(predicate_results(p.op, p.value, it.get_field(in_td.index_of(p.field_name)))){
          result_fields.push_back(it.get_field(in_td.index_of(p.field_name)));
        }
      }
    
      out.insertTuple(Tuple(result_fields));
      
    }
    
  }


  
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // TODO: Implement this function
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function
}
