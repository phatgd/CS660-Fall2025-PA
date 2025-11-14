#include <db/Query.hpp>

using namespace db;

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
  //
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
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
  // TODO: Implement this function
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
  // TODO: Implement this function
}
