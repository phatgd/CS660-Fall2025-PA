#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <cmath>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    // @author Sam Gibson
    capacity = std::floor((4096*8)/(static_cast<int>(td.length()) * 8 + 1)); // number of slots in a page
    header = &page[0]; // header is begining of page
    data = &page[capacity/8]; // data is after the header ends

}

size_t HeapPage::begin() const {
    // TODO pa1
    
    // loop to find first empty slot
    // for(int x = 0; x< capacity; x+= size of a slot){

    // }
    return capacity; // if no empty return end
}

size_t HeapPage::end() const {
    // @author Sam Gibson
    return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
    // TODO pa1
    // bitwise or operator
}

void HeapPage::deleteTuple(size_t slot) {
    // TODO pa1
    // clear with bitwise and ~&
}

Tuple HeapPage::getTuple(size_t slot) const {
    // TODO pa1
    // use name_to_pos? 
    
}

void HeapPage::next(size_t &slot) const {
    // TODO pa1
}

bool HeapPage::empty(size_t slot) const {
    // TODO pa1
}
