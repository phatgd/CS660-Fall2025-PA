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
    // std::cout << "header is: " << ((header[51/8] >> (7-51%8)) & 1)  << "\n";
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
    // @author Phat Duong

    // find first occupied slot
    size_t slot = begin();

    // full page
    if (slot == 0){ 
        return false;
    }

    // get most recent empty slot
    slot -= 1;

    // serialize tuple into data
    td.serialize((data + slot * td.length()), t);
    
    // mark slot as used in header
    // bitwise or operator
    header[slot/8] |= (1 << (7-slot%8));

    // success
    return true;
   
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
    }
    // std::cout << "slot number: " << slot << "; ";  // iterate through slots in header

}

bool HeapPage::empty(size_t slot) const {
     // @author Phat Duong
    return ((header[slot/8] >> (7-slot%8)) & 1) == 0; // check if slot is empty
}
