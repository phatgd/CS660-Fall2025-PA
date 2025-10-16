#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
    // @author Sam Gibson, Phat Duong
    
    struct stat buffer;

    // open file and assign it to a unique file descriptor
    f_desc = open(name.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRWXG); 
    if(f_desc == -1){
        throw std::runtime_error("File can't be opened");
    }

    // get stats on file
    if(fstat(f_desc, &buffer)){ 
        throw std::runtime_error("File can't be opened");
    }

    numPages = buffer.st_size / DEFAULT_PAGE_SIZE; // initialize numPages

    if(numPages == 0){ // always has at least one page even if empty
        numPages++;
    }
    
}

DbFile::~DbFile() {
    // @author Sam Gibson

    // TODO pa1: close file
    // Hind: use close

    close(f_desc);
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
    // @author Sam Gibson

    // void* buf = &page; // void ptr for content
    ssize_t results = pread(f_desc, &page, DEFAULT_PAGE_SIZE, id*DEFAULT_PAGE_SIZE);
    if(results < 0){
        throw std::runtime_error("Couldn't read :O");
    }
    
    reads.push_back(id);
}

void DbFile::writePage(const Page &page, const size_t id) const {
    // @author Sam Gibson

    // const void* buf = &page; // void ptr for content
    ssize_t results = pwrite(f_desc, &page, DEFAULT_PAGE_SIZE, id*DEFAULT_PAGE_SIZE);
    if(results < 0){
        throw std::runtime_error("Couldn't write :P");
    }
    
    writes.push_back(id);
    // TODO pa1: write page
    // Hint: use pwrite
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
