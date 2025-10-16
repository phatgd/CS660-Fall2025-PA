#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <cmath>
#include <iostream>
#include <array>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    // @author Sam Gibson

    capacity = std::floor((DEFAULT_PAGE_SIZE*8)/(td.length() * 8 + 1)); // number of slots in a page/ header
    header = page.data(); // begining of page
    data = header + (DEFAULT_PAGE_SIZE - td.length() * capacity); //  P − T ∗ C 
    std::cout << "header is: " << ((header[51/8] >> (7-51%8)) & 1)  << "\n";
}

size_t HeapPage::begin() const {
    // @author Sam Gibson
    for(size_t x = 0; x< capacity; x++){ //iterate through header
        if(!empty(x)){ // not empty
            return x;
        }
    }
    return capacity; 
}

size_t HeapPage::end() const {
    // @author Sam Gibson

    return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
    // find empty slot
    size_t slot = begin();

    if (slot == capacity){ // full page
        return false;
    }

    // serialize tuple into data
    td.serialize(data + slot/8 * td.length(), t);
    
    // mark slot as used in header
    *(header + slot) = 1;

    // bitwise or operator
}

void HeapPage::deleteTuple(size_t slot) {
    // TODO pa1
    // clear with bitwise and ~&
}

Tuple HeapPage::getTuple(size_t slot) const {
    // TODO pa1

    // ptr + page_size
    // td.length * capacity

    // data + slot * td.length
    // deserialize

    
}

void HeapPage::next(size_t &slot) const {
    // @author Sam Gibson

    while(++slot < capacity && empty(slot)){
        // std::cout << "slot number: " << slot << "; ";  // iterate through slots in header
    }
    std::cout << "slot number: " << slot << "; ";  // iterate through slots in header

}

bool HeapPage::empty(size_t slot) const {
     // @author Sam Gibson, Phat Duong
    // slot = slot - 1; // adjust for 0 index
    return *header == '\0' || ((header[slot/8] >> (7-slot%8)) & 1) == 0; // check if slot is empty
}
