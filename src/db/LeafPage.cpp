#include <db/LeafPage.hpp>
#include <stdexcept>
#include <cmath>
#include <string.h>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
  // TODO pa2
  // @author Sam Gibson

  // page header is a LeafPageHeader
  header = reinterpret_cast<LeafPageHeader *>(page.data()); 

  // based on the remaining size of the page and the size of the tuples
  capacity = (DEFAULT_PAGE_SIZE * 8 - page.size())/(td.length() * 8);

  data = page.data() + DEFAULT_PAGE_SIZE - td.length() * capacity;
}

bool LeafPage::insertTuple(const Tuple &t) {
  // TODO pa2
}

int LeafPage::split(LeafPage &new_page) {
  // TODO pa2

  // calc new size
  uint16_t origin_size = header -> size;

  this -> header -> size = std::ceil(origin_size/2);
  new_page.header -> size = std::floor(origin_size/2);

  // resetting next ptrs
  new_page.header->next_leaf = this -> header -> next_leaf;
  this -> header -> next_leaf = new_page.key_index;
  
  // split tuple
  uint8_t *split_index = data + (new_page.header -> size) * td.length();

  // move tuples in old page to new
  memmove(new_page.data, split_index, (new_page.header -> size) * td.length());
  
}

Tuple LeafPage::getTuple(size_t slot) const {
  // TODO pa2
  return td.deserialize(data+slot*td.length());
}
