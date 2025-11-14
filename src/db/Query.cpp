#include <db/Query.hpp>
#include <iostream>

using namespace db;


bool operand_check(field_t field1, field_t field2, PredicateOp operation){
  	// TODO: Implement this
}

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
<<<<<<< HEAD
	// @author Phat Duong, Sam Gibson
  	std::cout<<"test debugging"<<std::endl;
  	auto &result_td = in.getTupleDesc();
	
	for (const auto &it : in){
		std::vector<field_t> result_fields{};
		for (auto &field : field_names){
			result_fields.push_back(it.get_field(result_td.index_of(field)));
		}
		Tuple result_tuple = Tuple(result_fields);
		out.insertTuple(result_tuple);
 	}
=======
  //
  auto &in_td = in.getTupleDesc();

  for(const auto &it : in){
    std::vector<field_t> result_fields{};
    for(auto &field: field_names){
      result_fields.push_back(it.get_field(in_td.index_of(field)));
    }

    out.insertTuple(Tuple(result_fields));
    
  }
>>>>>>> 49298a3 (projection)
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
  	// TODO: Implement this function
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  	// TODO: Implement this function
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  	// TODO: Implement this function
}
