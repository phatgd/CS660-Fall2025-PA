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

  // if empty add all to out
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
      bool pass = true;
      
      // if any of the predicates fail for the tuple it fails
      for(auto &p: pred){
        if(predicate_results(p.op, p.value, it.get_field(in_td.index_of(p.field_name)))){
          result_fields.push_back(it.get_field(in_td.index_of(p.field_name)));
          std::cout<< pass<< ": push_back";
        }
        else{
          pass= false;
          break;
        }
        
      }

      // only push back if all things match, so all need to be true
      if(pass == true){
        out.insertTuple(Tuple(result_fields));
        std::cout<< pass<< ": pushed";
      }
          
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
