#include <db/IndexPage.hpp>
#include <stdexcept>
#include <cstring>
#include <iostream>

using namespace db;

IndexPage::IndexPage(Page &page) {
  // @author Phat Duong
  
  header = reinterpret_cast<IndexPageHeader *>(page.data());

  // capacity = page size divided by size of (key + child) minus 1 extra key (because n keys have n+1 children)
  capacity = DEFAULT_PAGE_SIZE/(sizeof(int) + sizeof(size_t)) - 1;

  // keys = new int[capacity];
  // children = new size_t[capacity + 1];

  // keys pointer starts after header
  keys = reinterpret_cast<int *>(page.data() + DEFAULT_PAGE_SIZE - (sizeof(int) + sizeof(size_t)) * capacity - sizeof(size_t));

  // children pointer starts after keys
  children = reinterpret_cast<size_t *>(keys + capacity*sizeof(int));

  header -> size = 0;
}

bool IndexPage::insert(int key, size_t child) {
  // @author Phat Duong

  size_t slot = 0;
  size_t child_offset = 0;

  while(slot < header -> size) {

    if (key < keys[slot]) {
      // move keys and children to make space
      memmove(children + slot + 2, children + slot + 1, (header -> size - slot) * sizeof(size_t));
      memmove(keys + slot + 1, keys + slot, (header -> size - slot) * sizeof(int));
      break;
    }

    slot++;
  }

  // insert key and child
  keys[slot] = key;
  children[slot + 1] = child;
  header -> size ++;
  return header -> size >= capacity;
}

int IndexPage::split(IndexPage &new_page) {
  // TODO pa2
}
