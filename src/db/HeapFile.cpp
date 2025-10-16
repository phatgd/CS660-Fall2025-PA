#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
    // @author Phat Duong
    if(!td.compatible(t)){
        throw std::invalid_argument("Tuple is not compatible with TupleDesc");
    }

    Page page;
    readPage(page, getNumPages()-1); // read last page
    HeapPage hp(page, td);
    
    if (hp.insertTuple(t)){
        writePage(page, getNumPages()-1); // write back to last page
        return;
    }

    // last page is full, create a new page
    Page new_page = {}; // zero-initialize new page
    HeapPage new_hp(new_page, td);
    if (!new_hp.insertTuple(t)){
        throw std::runtime_error("Failed to insert tuple into new page");
    }

    writePage(new_page, getNumPages()); // write new page to end of file
    numPages++; // increment number of pages
}

void HeapFile::deleteTuple(const Iterator &it) {
    // @author Phat Duong

    if(it.page >= getNumPages()){
        throw std::out_of_range("Iterator out of range of file");
    }

    Page page;
    readPage(page, it.page);
    HeapPage hp(page, td);

    hp.getTuple(it.slot) = Tuple({}); // optional: clear the tuple data
    hp.deleteTuple(it.slot);

    writePage(page, it.page); // write back to file
}

Tuple HeapFile::getTuple(const Iterator &it) const {
    // @author Phat Duong

    if(it.page >= getNumPages()){
        throw std::out_of_range("Iterator out of range of file");
    }

    Page page;
    readPage(page, it.page);
    HeapPage hp(page, td);

    return hp.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
    // @author Phat Duong

    // remember original page
    // if we advance to a new page, set slot to beginning of that page
    const size_t origin_page = it.page;

    if(origin_page >= getNumPages()){
        throw std::out_of_range("Iterator out of range of file");
    }

    // find next occupied slot
    while (it.page < getNumPages()) {
        Page page;
        size_t &slot = it.slot;

        readPage(page, it.page);
        HeapPage hp(page, td);

        // if we are on a new page, set slot to beginning
        // else advance to next slot
        if (origin_page != it.page){
            slot = hp.begin();
        } else {
            hp.next(slot);
        }
        
        // if current slot is not at the end, aka page is empty, return
        if (slot != hp.end()) {
            return;
        }        
        
        // otherwise advance to next page
        it.slot = 0; // reset slot
        ++it.page;
    }
}

Iterator HeapFile::begin() const {
    // @author Phat Duong

    for (size_t page_num = 0; page_num < getNumPages(); ++page_num) {
        Page page;
        readPage(page, page_num);
        HeapPage hp(page, td);
        size_t slot = hp.begin();
        if (slot != hp.end()) {
            return Iterator(*this, page_num, slot);
        }
    }

    return end(); // no tuples in file
}

Iterator HeapFile::end() const {
    // @author Phat Duong

    return Iterator(*this, getNumPages(), 0); // sentinel value, page is out of range
}
