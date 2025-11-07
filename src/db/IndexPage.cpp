#include <db/IndexPage.hpp>
#include <stdexcept>
#include <iostream>
#include <string.h>

using namespace db;

IndexPage::IndexPage(Page &page) {
	// @author Phat Duong
	
	header = reinterpret_cast<IndexPageHeader *>(page.data());

	// capacity = page size divided by size of (key + child) minus 1 extra key (because n keys have n+1 children)
	capacity = DEFAULT_PAGE_SIZE/(sizeof(int) + sizeof(size_t)) - 1;

	// keys pointer starts after header
	keys = reinterpret_cast<int *>(page.data() + sizeof(IndexPageHeader));

	// children pointer starts after keys
	children = reinterpret_cast<size_t *>(page.data() + sizeof(IndexPageHeader) + capacity * sizeof(int));

	header -> size = (header -> size == '\0') ? 0 : header -> size;
	// header -> index_children = false; // no index children proabably until after split
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
	// @author Sam Gibson

	// save middle poistion (starts from 0)
	int middle = header -> size / 2;
	
	// get new headers 
	new_page.header -> size = middle + header -> size % 2 - 1;
	new_page.header -> index_children = header -> index_children;
	header -> size = middle;
	
	// split key data
	int *key_split = keys + middle + 1;
	memmove(new_page.keys, key_split, (new_page.header -> size) * (sizeof(int)));
	std::fill(key_split - 1 , key_split, 0); // optional: clear old keys

	// move children
	// children size stays the same as keys size + 1
	size_t *child_split = children + middle;
	memmove(new_page.children, child_split, (new_page.header -> size + 1) * (sizeof(size_t)));

	return keys[middle];
}
