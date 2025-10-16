#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
    // @author Sam Gibson
    
    struct stat buffer;
    int file_size;

    if(fstat(fd, &buffer)){ // get stats on file
        throw std::runtime_error("File can't be opened");
    }
    else{
        file_size = buffer.st_size;
    }

    // may need fopen()
    fd = open(name.c_str(), 2); // opens/ creates file at that location
    if(fd == -1){
        throw std::runtime_error("File can't be opened");
    }

    numPages = file_size/ DEFAULT_PAGE_SIZE; // initialize numPages

    if(numPages == 0){ // always has at least one page even if empty
        numPages++;
    }
    
}

DbFile::~DbFile() {
    // @author Sam Gibson

    // TODO pa1: close file
    // Hind: use close

    close(fd);
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
    // @author Sam Gibson

    void* buf = &page; // void ptr for content
    ssize_t results = pread(fd, buf, DEFAULT_PAGE_SIZE, id);
    if(results < 0){
        throw std::runtime_error("Couldn't read :O");
    }
    
    reads.push_back(id);
}

void DbFile::writePage(const Page &page, const size_t id) const {
    // @author Sam Gibson

    const void* buf = &page; // void ptr for content
    ssize_t results = pwrite(fd, buf, DEFAULT_PAGE_SIZE, id);
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
