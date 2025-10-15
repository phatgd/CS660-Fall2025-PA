#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <cmath>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    // @author Sam Gibson

    capacity = std::floor((DEFAULT_PAGE_SIZE*8)/(static_cast<int>(td.length()) * 8 + 1)); // number of slots in a page
    header = page.data(); // header is begining of page
    data = header + (DEFAULT_PAGE_SIZE - td.length() * capacity); //  P − T ∗ C 
}

size_t HeapPage::begin() const {
    // @author Sam Gibson
    
    // code TA gave me not working ? 
    // for(size_t x = *header; x< capacity; x++){ //iterate through header
    //     if(!empty(x)){ // not empty
    //         return x;
    //     }
    // }
    uint8_t *ptr_first = header; // ptr to head
    int idx = 0; // index of first slot with data

    for(int x = 0; x< (capacity/8)-1; x++){
        if(*ptr_first == 1){ // find a full 
            idx = x;
            return td.length()*x;
        }
        ptr_first++;
    }

    return capacity; 
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

    // ptr + page_size
    // td.length * capacity

    // data + slot * td.length
    // deserialize

    
}

void HeapPage::next(size_t &slot) const {
    // @author Sam Gibson

    while(++slot < capacity && empty(slot)){ // iterate through slots in header
    }

}

bool HeapPage::empty(size_t slot) const {
     // @author Sam Gibson
    if(slot == 0){
        return true;
    }
    else{
        return false;
    }
}
