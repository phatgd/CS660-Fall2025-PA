#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
		: DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
	// TODO pa2

	BufferPool &bufferPool = getDatabase().getBufferPool();

	// get key from tuple
	int t_key = std::get<int>(t.get_field(key_index));

	// list of parent index pages to update if split occurs
	list<PageId> parent_pages = [];

	// use key to traverse tree from root to leaf
	// get the root index page
	PageId pid = PageId{name, root_id};
	Page &p = bufferPool.getPage(pid);
	IndexPage ip = IndexPage(p);
	parent_pages.push_front(pid);

	// traverse until leaf page is reached
	do {
		for (size_t slot = 0; slot < ip.header->size; slot++) {
			// compare key with keys in index page to find child
			if (t_key < ip.keys[slot]) {
				// move to child page
				pid = PageId{name, ip.children[slot]};
				parent_pages.push_front(pid);
				p = bufferPool.getPage(pid);
				break;        
			}
		}
		ip = IndexPage(p);
	} while(!ip.header->index_children)
	
	// now at leaf page
	LeafPage lp = LeafPage(p, td, key_index);

	// insert tuple into leafpage
	if (!lp.insertTuple(t)) {
		return;
	}

	// Leaf page is full, need to split
	Page &new_page = Page{};
	db::LeafPage new_leaf_page = LeafPage(new_page, td, key_index);

	int split_key = lp.split(new_leaf_page);
	lp.header->next_leaf = numPages;

	// propagate split up to parent index page
	PageId parent_pid = parent_pages.pop_front();
	Page &parent_page = bufferPool.getPage(parent_pid);
	IndexPage parent_ip = IndexPage(parent_page);

	while(parent_ip.insert(split_key, numPages)){
		// parent index page is full, need to split
		Page &new_index_page = Page{};
		IndexPage new_ip = IndexPage(new_index_page);
	

		intsplit_key = parent_ip.split(new_ip);


		if
	};
	
	numPages++;

 

	// repeat until no more split is needed or at root

		 // if root is split, create new root index page with two children
		// reassign root id to new root page
	
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
	BufferPool &bufferPool = getDatabase().getBufferPool();
	
	
	while (it != end()) {
		Page &p = bufferPool.getPage(PageId{name, it.page});
		LeafPage lp(p, td, key_index);

		if (it.slot++ < lp.header->size) {
			return;
		}

		it.page = lp.header->next_leaf;
		it.slot = 0;
	}
}

Iterator BTreeFile::begin() const {
	// @author Phat Duong
	return {*this, root_id, 0};
}

Iterator BTreeFile::end() const {
	return {*this, numPages, 0};
}
