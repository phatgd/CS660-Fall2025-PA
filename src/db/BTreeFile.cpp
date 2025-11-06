#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <list>
#include <stdexcept>

#include <iostream>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
		: DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
	// TODO pa2
	std::cout << "=========================================" << std::endl;

	BufferPool &bufferPool = getDatabase().getBufferPool();

	// get key from tuple
	int t_key = std::get<int>(t.get_field(key_index));

	// list of parent index pages to update if split occurs
	std::list<PageId> parent_pages = {};

	// use key to traverse tree from root to leaf
	// get the root index page
	PageId pid = PageId{name, root_id};
	IndexPage root_ip = IndexPage(bufferPool.getPage(pid));

	// get current index page
	IndexPage current_ip = root_ip;
	
	std::cout << "Inserting key: " << t_key << ", numPages: " << numPages  <<std::endl;

	// if tree is empty, create first Leaf page and assign it to children of root
	if (numPages == 1){

		// create new Leaf page and insert tuple
		pid.page = numPages;
		LeafPage lp = LeafPage(bufferPool.getPage(pid), td, key_index);
		lp.insertTuple(t);
		// bufferPool.markDirty(PageId{name, numPages});

		// set root index page to point to new leaf
		root_ip.keys[0] = std::get<int>(lp.getTuple(0).get_field(key_index));
		root_ip.children[0] = numPages;

		// set header data
		// root_ip.header->size = 1; // it's a bold strategy cotton, let's see if it pays off for 'em
		root_ip.header->index_children = false;

		// increment page count
		numPages++;
		return;
	}

	

	// if parent_pages is empty, that means we are at root
	while(current_ip.header->index_children || parent_pages.empty()) {
		// std::cout <<"RootID: "<< root_id << " pid.page: "<< pid.page << std::endl;
		
		current_ip = IndexPage(bufferPool.getPage(pid));
		parent_pages.push_front(pid);
		
		size_t slot = 0;
		for (;slot < current_ip.header->size; slot++) {
			// compare key with keys in index page to find child
			if (t_key < current_ip.keys[slot]) {
				break;        
			}
		}
		// if (t_key % 1000 == 0) {
		// 	std::cout << "Going to index child page: " << current_ip.children[slot] << " at slot: " << slot << std::endl;
		// }

		// move to child page
		// if index_children is false, child page is leaf and we exit
		pid.page = current_ip.children[slot];
	}

	// now at leaf page
	LeafPage lp = LeafPage(bufferPool.getPage(pid), td, key_index);
	
	// insert tuple into leaf page
	// bufferPool.markDirty(pid);
	std::cout << "Inserting into page: " << pid.page << std::endl;
	std::cout << "Leaf page capacity: " << lp.capacity << ", current size: " << lp.header->size << std::endl;
	if (!lp.insertTuple(t)) {
		return;
	}

	std::cout << "Leaf page full, need to split for key: " << t_key << std::endl;
	
	// Leaf page is full, need to split
	pid.page = numPages;
	db::LeafPage new_leaf_page = LeafPage(bufferPool.getPage(pid), td, key_index);
	
	bufferPool.markDirty(pid);
	int split_key = lp.split(new_leaf_page);

	// Re-assign reference for next leaf
	lp.header->next_leaf = numPages;
	
	// propagate split up to parent index page
	
	// since current_ip is now leaf's parent, we can pop from front
	parent_pages.pop_front();

	// repeat until at root or no more split is needed
	while(current_ip.insert(split_key, numPages)){
		
		numPages++;
		
		// parent index page is full, need to split
		pid.page = numPages;
		IndexPage new_ip = IndexPage(bufferPool.getPage(pid));

		// get split key to propagate up 
		split_key = current_ip.split(new_ip);
	
		// move up to next parent
		current_ip = IndexPage(bufferPool.getPage(parent_pages.front()));

		// mark used pages as dirty
		bufferPool.markDirty(pid);
		bufferPool.markDirty(parent_pages.front());

		// reassign index_children to true
		current_ip.header->index_children = true;

		// remove used parent page from list
		parent_pages.pop_front();
		
		// if no more parent pages, we are at root
		if (parent_pages.empty()) {
			break;
		}
	};

	// increment page count for new leaf page created.
	numPages++;

	// if parent_pages is not empty, that means root was not split nor inserted
	if(!parent_pages.empty()){
		return;
	}

	std::cout << "Inserting into root page, with key: "<< t_key <<" numPages: "<< numPages<< std::endl;

	// insert into root page
	if (!current_ip.insert(split_key, numPages -1)) {
		return;
	}

	std::cout << "Root is full, need to split with key: "<< t_key << std::endl;

	// create new index page for old root contents (current_ip)
	pid.page = numPages;
	IndexPage root_child_left = IndexPage(bufferPool.getPage(pid));
	bufferPool.markDirty(pid);

	pid.page = numPages + 1;
	IndexPage root_child_right = IndexPage(bufferPool.getPage(pid));
	bufferPool.markDirty(pid);

	// split current root index page into two new pages
	split_key = current_ip.split(root_child_right);
	root_child_left = current_ip;

	std::cout << "New root created with split key: " << split_key << std::endl;

	// root index page was split, need to reassign root
	// since root_id = 0 is constant, we need to overide the contents of root page
	root_ip.header->size = 0;
	root_ip.header->index_children = true;

	// assign children to new root_id
	root_ip.children[0] = numPages;
	root_ip.insert(split_key, numPages + 1);

	numPages+=2;
}	


void BTreeFile::deleteTuple(const Iterator &it) {
	// Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
	// @author Phat Duong

	BufferPool &bufferPool = getDatabase().getBufferPool();
	PageId pid{name, it.page};
	Page &p = bufferPool.getPage(pid);
	LeafPage lp(p, td, key_index);
	return lp.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
	// @author Phat Duong
	// TODO: This may be wrong

	// // log for every 1000 calls
	if (it.page * it.slot % 10 == 0) {
		std::cout << "BTreeFile::next() called. Current position - Page: " << it.page << ", Slot: " << it.slot << std::endl;
	}

	BufferPool &bufferPool = getDatabase().getBufferPool();

	LeafPage lp = LeafPage(bufferPool.getPage(PageId{name, it.page}), td, key_index);

	// next slot in the same page
	if (++it.slot < lp.header->size) {
		return;
	}

	// if not same page then move to next leaf page
	// if next leaf points to nothing, end
	if (lp.header->next_leaf == '\0') {
		it.page = numPages;
		it.slot = 0;
		return;
	}

	it.page = lp.header->next_leaf;
	it.slot = -1; // will be incremented to 0 in next call
	next(it);
}

Iterator BTreeFile::begin() const {
	// @author Sam Gibson

	BufferPool &bufferPool = getDatabase().getBufferPool();
	size_t id = root_id;

	Page &root_page = bufferPool.getPage(PageId{name, id});
	IndexPage parent_ip = IndexPage(root_page);
	
	while (parent_ip.header->index_children) {
		// traverse to leftmost index child
		id = parent_ip.children[0];
		Page &child_page = bufferPool.getPage(PageId{name, id});
		parent_ip = IndexPage(child_page);
	}

	// left most leaf children
	// if no children, then tree is empty
	// return end iterator
	size_t leaf_id = parent_ip.header -> size == 0? numPages : parent_ip.children[0];

	Iterator leaf_iter = {*this, leaf_id, 0}; // point to left most leaf
	return leaf_iter;
}

Iterator BTreeFile::end() const {
	return {*this, numPages, 0};
}
