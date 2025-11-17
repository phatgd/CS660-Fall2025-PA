#include <db/Query.hpp>
#include <iostream>
#include <set>
#include <list>


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
