#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    // NOTE: header and data should point to locations inside the page buffer. Do not allocate extra memory.
    
    // capacity = std::floor((page.size()*8)/(T ∗ 8 + 1)); // number of slots in a page
    header = &page[0]; // header is begining of page
    // data = ( P − T ∗ C ); // data is after the header ends

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
    // bitwise or operator?
}

void HeapPage::deleteTuple(size_t slot) {
    // TODO pa1
    // clear with bitwise and ~&
}

Tuple HeapPage::getTuple(size_t slot) const {
    // TODO pa1
    
}

void HeapPage::next(size_t &slot) const {
    // TODO pa1
}

bool HeapPage::empty(size_t slot) const {
    // TODO pa1
}
