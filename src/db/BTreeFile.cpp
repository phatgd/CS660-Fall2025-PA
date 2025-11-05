#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <list>
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
	std::list<PageId> parent_pages = {};

	// use key to traverse tree from root to leaf
	// get the root index page
	PageId pid = PageId{name, root_id};
	Page p = bufferPool.getPage(pid);
	const IndexPage &root_ip = IndexPage(p);
	IndexPage current_ip = root_ip;

	// if tree is empty, create first Leaf page and assign it to children of root
	if (numPages == 1){

		// create new Leaf page and insert tuple
		Page &new_leaf_page = bufferPool.getPage(PageId{name, numPages});
		LeafPage lp = LeafPage(new_leaf_page, td, key_index);
		bufferPool.markDirty(PageId{name, numPages});
		lp.insertTuple(t);

		// set root index page to point to new leaf
		root_ip.keys[0] = std::get<int>(lp.getTuple(0).get_field(key_index));
		root_ip.children[0] = numPages;

		// set header data
		// root_ip.header->size = 1;
		root_ip.header->index_children = false;

		// increment page count
		numPages++;
		return;
	}

	// if parent_pages is empty, that means we are at root
	while(current_ip.header->index_children || parent_pages.empty()) {
		
		// page p is index if it got here
		if (!parent_pages.empty()) {
			current_ip = IndexPage(p);
		}
		parent_pages.push_front(pid);
		
		size_t slot = 0;
		for (;slot < current_ip.header->size; slot++) {
			// compare key with keys in index page to find child
			if (t_key < current_ip.keys[slot]) {
				break;        
			}
		}

		// move to child page
		// if index_children is false, child page is leaf and we exit
		pid.page = current_ip.children[slot];
		p = bufferPool.getPage(pid);
	}

	// now at leaf page
	LeafPage lp = LeafPage(p, td, key_index);
	
	// insert tuple into leaf page
	bufferPool.markDirty(pid);
	if (!lp.insertTuple(t)) {
		return;
	}

	// Leaf page is full, need to split
	Page new_page = bufferPool.getPage(PageId{name, numPages});
	db::LeafPage new_leaf_page = LeafPage(new_page, td, key_index);

	int split_key = lp.split(new_leaf_page);

	// Re-assign reference for next leaf
	lp.header->next_leaf = numPages;
	
	// propagate split up to parent index page
	// repeat until at root or no more split is needed
	while(!parent_pages.empty() && current_ip.insert(split_key, numPages)){
		
		numPages++;
		
		// parent index page is full, need to split
		Page new_index_page = bufferPool.getPage(PageId{name, numPages});
		IndexPage new_ip = IndexPage(new_index_page);
		
		// get split key to propagate up 
		split_key = current_ip.split(new_ip);

		// move up to next parent
		current_ip = IndexPage(bufferPool.getPage(parent_pages.front()));
		parent_pages.pop_front();
	};

	// increment page count for new leaf page created.
	numPages++;

	// if parent_pages is not empty, that means insert was done without split
	if(!parent_pages.empty()){
		return;
	}
	
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

	BufferPool &bufferPool = getDatabase().getBufferPool();
	
	
	while (it != end()) {
		Page &p = bufferPool.getPage(PageId{name, it.page});
		LeafPage lp(p, td, key_index);
		
		// if next leaf points to nothing, end
		if (lp.header->next_leaf == '\0') {
			it.page = numPages;
			it.slot = 0;
			return;
		}

		if (++it.slot < lp.header->size) {
			return;
		}

		
		it.page = lp.header->next_leaf;
		it.slot = 0;
	}
}

Iterator BTreeFile::begin() const {
	// @author Sam Gibson
	size_t id = root_id;

	Iterator root_iter = {*this, id, 0}; // point to root
	next(root_iter); // get first entry 
	return root_iter;
}

Iterator BTreeFile::end() const {
	return {*this, numPages, 0};
}
