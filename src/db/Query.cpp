#include <db/Query.hpp>
#include <iostream>
#include <set>
#include <list>


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

  // if empty add all to out
  if(pred.size() == 0){
    for(const auto &it : in){
      out.insertTuple(it);
    }
    return;
  }

  // check for predicates
  for(const auto &it : in){
    bool pass = true;
    for(auto &p: pred){
      if(!predicate_results(p.op, it.get_field(in_td.index_of(p.field_name)), p.value)){
        pass = false;
        break;
      }
    }

    if (pass){
      out.insertTuple(it);
    }
  }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // TODO: Implement this function

  // auto &in_td = in.getTupleDesc();

  // // if no grouping
  // if(agg.group.empty()){
  //   auto op = agg.op; 

  //   field_t count;
  //   field_t answ;
  //   for(auto &it: in){
  //     switch(op){
  //       case db::AggregateOp::SUM:
  //         // do i have to handle different types :,) 
  //         answ = answ + it.get_field(in_td.index_of(agg.field));
  //       case db::AggregateOp::AVG:
  //         return;
  //         count += 1;
  //       case db::AggregateOp::MIN:
  //         if(answ <= it.get_field(in_td.index_of(agg.field))){
  //           answ = it.get_field(in_td.index_of(agg.field));
  //         }
  //       case db::AggregateOp::MAX:
  //         it.get_field(in_td.index_of(agg.field));
  //       case db::AggregateOp::COUNT:
  //         count ++;
  //     }
  //   }


  // }
  /*
    
    if no grouping: {
      count, answ;
      for entire tuple{
        if COUNT or avg:
          count ++
        if min:
          answ get changed if it is less than current
        if max:
          answ get changed if bigger than current
        if sum or avg:
          answ += value;
        }
        if avg: 
        answ = answ/count;
          
        push answer
        answ = 0;
        count = 0;
    }
    if grouping isn't empty:{
        nested loop:
        answ; 
        count;
        for each group{
          for within each group{
            if COUNT or avg:
              count ++
            if min:
              answ get changed if it is less than current
            if max:
              answ get changed if bigger than current
            if sum or avg:
              answ += value;
          }
          if avg: 
          answ = answ/count;
          
          push answer
          answ = 0;
          count = 0;
        }
                
    }
  */
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function

  /*
  
  */
}
