#include <cstring>
#include <db/Tuple.hpp>
#include <unordered_set>
#include <stdexcept>
#include <unordered_set>
#include <sys/stat.h>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
    const field_t &field = fields.at(i);
    if (std::holds_alternative<int>(field)) {
        return type_t::INT;
    }
    if (std::holds_alternative<double>(field)) {
        return type_t::DOUBLE;
    }
    if (std::holds_alternative<std::string>(field)) {
        return type_t::CHAR;
    }
    throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names) {
    // @author Phat Duong

    // Check lengths of names and types
    if (types.size() != names.size()) {
        throw std::logic_error("must have same number of field names and types");
    }

    // Check for unique names
    std::unordered_set<std::string> name_set;
    for (const auto &name : names) {
        if (!name_set.insert(name).second) {
            throw std::logic_error("Names must be unique");
        }
    }

    // Populate schema with names and associated types
    for (size_t i = 0; i < names.size(); ++i) {
        name_to_pos[names[i]] = i;
        field_names.push_back(names[i]);
        field_types.push_back(types[i]);
        field_sizes.push_back(
            types[i] == type_t::INT ? INT_SIZE :
            types[i] == type_t::DOUBLE ? DOUBLE_SIZE :
            CHAR_SIZE
        );
    }
}

bool TupleDesc::compatible(const Tuple &tuple) const {
    // @author Sam Gibson
    // if different number of fields return false
    if(this->size() != tuple.size()){
        return false;
    }

    // compare each field
    for(int x = 0; x < this->size(); x++){
        if(this->field_types[x] != tuple.field_type(x)){
            return false;
        }
    }  

    return true;
    throw std::runtime_error("not implemented");
}

size_t TupleDesc::index_of(const std::string &name) const {
    // @author Phat Duong

    // Check if name exists
    if (name_to_pos.find(name) == name_to_pos.end()) {
        throw std::logic_error("Field name does not exist");
    }
    return name_to_pos.at(name);
}

size_t TupleDesc::offset_of(const size_t &index) const {
    // @author Phat Duong

    if (index >= field_names.size()) {
        throw std::logic_error("Index out of bounds");
    }

    size_t offset = 0;
    for (size_t i = 0; i < index; ++i) {
        offset += field_sizes[i];
    }
    
    return offset;

}

size_t TupleDesc::length() const {
    // TODO pa1
}

size_t TupleDesc::size() const {
    // @author Sam Gibson
    return field_names.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
    // TODO pa1
    return reinterpret_cast<const Tuple &>(data);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
    // TODO pa1
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    // @author Phat Duong   

    for (int i = 0; i < td2.size(); ++i) {
        // Check for duplicate field namess
        if (td1.name_to_pos.find(td2.field_names[i]) != td1.name_to_pos.end()) {
            throw std::logic_error("Cannot merge TupleDescs with duplicate field names");
        }

        td1.field_names.push_back(td2.field_names[i]);
        td1.field_types.push_back(td2.field_types[i]);
        td1.field_sizes.push_back(td2.field_sizes[i]);
        td1.name_to_pos[td2.field_names[i]] = td1.size() - 1;
    }

    return td1;
}
