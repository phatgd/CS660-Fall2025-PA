#include <db/LeafPage.hpp>
#include <stdexcept>
#include <string.h>

#include <iostream>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index) : td(td), key_index(key_index) {
	// @author Sam Gibson, Phat Duong

	// page header is a LeafPageHeader
	header = reinterpret_cast<LeafPageHeader *>(page.data()); 
 
	// based on the remaining size of the page and the size of the tuples
	capacity = DEFAULT_PAGE_SIZE/td.length();

	// std::cout<< "LeafPage size: " << header -> size << std::endl;
 	header -> size = (header -> size == '\0') ? 0 : header -> size;

	// data starts after header
	data = page.data() + DEFAULT_PAGE_SIZE - td.length() * capacity;
}

bool LeafPage::insertTuple(const Tuple &t) {
	// @author Phat Duong

	// get key from tuple
	int key = std::get<int>(t.get_field(key_index));

	size_t slot = 0;

	while(slot < header -> size) {
		// get current key
		int current_key = std::get<int>(getTuple(slot).get_field(key_index));
		// std::cout << "Comparing with key: " << current_key << " with key: " << key << std::endl;

		if (key == current_key) {
			header -> size--;
			break;
		}

		if (key < current_key) {
			// move tuples to make space
			memmove(data + (slot + 1) * td.length(), data + slot * td.length(), (header -> size - slot) * td.length());
			break;
		}

		slot++;
	}

	// if (header -> size + 1>= capacity) {
	// 	std::cout << "size before insert: " << header -> size << ", inserting at slot: " << slot << std::endl;	
	// 	std::cout << "insert key: " << key << std::endl;
	// }
	header -> size++;
	td.serialize(data + slot * td.length(), t);

	return header -> size >= capacity;
}

int LeafPage::split(LeafPage &new_page) {
	// @author Sam Gibson, Phat Duong

	new_page.header->size = (header->size / 2) + (header->size % 2);
	header->size = header->size / 2;

	// resetting next ptrs
	new_page.header->next_leaf = this -> header -> next_leaf;

	// split tuple
	uint8_t *split_index = data + (header -> size) * td.length();

	// move tuples in old page to new
	memmove(new_page.data, split_index, (new_page.header -> size) * td.length());

	return std::get<int>(new_page.getTuple(0).get_field(key_index));
}

Tuple LeafPage::getTuple(size_t slot) const {
	// @author Phat Duong
	return td.deserialize(data + slot * td.length());
}
