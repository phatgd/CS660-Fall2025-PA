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
	// @author Phat Duong

	if (!td.compatible(t)) {
		throw std::logic_error("TupleDesc of tuple does not match TupleDesc of BTreeFile");
	}

	BufferPool &bufferPool = getDatabase().getBufferPool();

	// get key from tuple
	int t_key = std::get<int>(t.get_field(key_index));

	// list of parent index pages to update if split occurs
	std::list<PageId> parent_pages = {};

	// use key to traverse tree from root to leaf
	// get the root index page
	PageId pid = PageId{name, root_id};
	Page &root_page = bufferPool.getPage(pid);
	IndexPage root_ip = IndexPage(root_page);

	// get current index page
	IndexPage current_ip = root_ip;

	// if tree is empty, create first Leaf page and assign it to children of root
	if (numPages == 1){

		// create new Leaf page and insert tuple
		pid.page = numPages;
		LeafPage lp = LeafPage(bufferPool.getPage(pid), td, key_index);
		lp.insertTuple(t);

		bufferPool.markDirty(pid);

		// set root index page to point to new leaf
		root_ip.keys[0] = std::get<int>(lp.getTuple(0).get_field(key_index));
		root_ip.children[0] = numPages;

		// set header data
		root_ip.header->index_children = false;

		// increment page count
		numPages++;
		return;
	}

	// if parent_pages is empty, that means we are at root
	while(current_ip.header->index_children || parent_pages.empty()) {
		
		current_ip = IndexPage(bufferPool.getPage(pid));
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
	}

	// now at leaf page
	LeafPage lp = LeafPage(bufferPool.getPage(pid), td, key_index);
	
	// insert tuple into leaf page
	bufferPool.markDirty(pid);
	if (!lp.insertTuple(t)) {
		return;
	}

	if (pid.page == 340){
		std::cout << "Right most child of parent: " << current_ip.children[current_ip.header->size] << std::endl;
		std::cout << "next leaf page id for 340: "<< numPages << std::endl;
	}
	
	// Leaf page is full, need to split
	
	pid.page = numPages;
	LeafPage new_leaf_page = LeafPage(bufferPool.getPage(pid), td, key_index);
	
	int split_key = lp.split(new_leaf_page);
	lp.header->next_leaf = pid.page; // Re-assign reference to next leaf
	bufferPool.markDirty(pid);

	// propagate split up to parent index page
	
	// since current_ip is now leaf's parent, we can pop from front
	parent_pages.pop_front();

	// repeat until at root or no more split is needed
	while(!parent_pages.empty() && current_ip.insert(split_key, numPages)){
		
		numPages++;
		
		// parent index page is full, need to split
		pid.page = numPages;
		IndexPage new_ip = IndexPage(bufferPool.getPage(pid));

		// get split key to propagate up 
		split_key = current_ip.split(new_ip);
	
		// move up to next parent
		current_ip = IndexPage(bufferPool.getPage(parent_pages.front()));

		// reassign index_children to true
		current_ip.header->index_children = true;

		// mark used pages as dirty
		bufferPool.markDirty(pid);
		bufferPool.markDirty(parent_pages.front());

		// remove used parent page from list
		parent_pages.pop_front();
	};

	// increment page count for new leaf page created.
	numPages++;

	// if parent_pages is not empty, that means root was not split nor inserted
	if(!parent_pages.empty()){
		return;
	}

	// insert into root page
	if (!current_ip.insert(split_key, numPages-1)) {
		return;
	}

	// create new index page for old root contents (current_ip)
	pid.page = numPages;
	Page &root_left_child_page = bufferPool.getPage(pid);
	// move current root contents to new page
	memmove(root_left_child_page.data(), root_page.data(), DEFAULT_PAGE_SIZE);
	IndexPage root_child_left = IndexPage(root_left_child_page);

	pid.page = numPages + 1;
	IndexPage root_child_right = IndexPage(bufferPool.getPage(pid));

	// split current root index page into two new pages
	split_key = root_child_left.split(root_child_right);

	// root index page was split, need to reassign root
	// since root_id = 0 is constant, we need to overide the contents of root page
	root_ip.header->size = 0;
	root_ip.header->index_children = true;

	// assign children to new root_id
	root_ip.children[0] = numPages;
	root_ip.insert(split_key, numPages + 1);

	bufferPool.markDirty(PageId{name, root_id});
	bufferPool.markDirty(PageId{name, numPages});
	bufferPool.markDirty(PageId{name, numPages + 1});

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

	BufferPool &bufferPool = getDatabase().getBufferPool();

	LeafPage lp = LeafPage(bufferPool.getPage(PageId{name, it.page}), td, key_index);

	// // log for every page called
	// if (it.slot == 0 && it.page > 300) {
	// 	std::cout << "==========================================" << std::endl;
	// 	std::cout << "Current position - Page: " << it.page << ", First Leaf Key: " << std::get<int>(lp.getTuple(0).get_field(0)) << std::endl;
	// 	std::cout << "Next leaf: "<< lp.header->next_leaf << std::endl;
	// }

	// next slot in the same page
	if (++it.slot < lp.header->size) {
		return;
	}

	it.slot = 0;

	// if not same page then move to next leaf page
	// if next leaf points to nothing, end
	if (lp.header->next_leaf == '\0') {
		it.page = numPages;
		
		return;
	}

	it.page = lp.header->next_leaf;
}

Iterator BTreeFile::begin() const {
	// @author Sam Gibson

	BufferPool &bufferPool = getDatabase().getBufferPool();
	size_t id = root_id;

	Page &root_page = bufferPool.getPage(PageId{name, id});
	IndexPage parent_ip = IndexPage(root_page);
	
	while (parent_ip.header->index_children) {
		// traverse to leftmost index child
		std::cout<<"Traversing to index child page: " << parent_ip.children[0] << std::endl;

		id = parent_ip.children[0];
		Page &child_page = bufferPool.getPage(PageId{name, id});
		parent_ip = IndexPage(child_page);
	}

	// left most leaf children
	// check for empty tree
	size_t leaf_id = numPages == 1 ? numPages : parent_ip.children[0];

	std::cout<<"Left most leaf page id: " << leaf_id << std::endl;
	// std::cout<<"Right most leaf page id: " << parent_ip.children[parent_ip.header->size - 1] << std::endl;
	Iterator leaf_iter = {*this, leaf_id, 0}; // point to left most leaf
	return leaf_iter;
}

Iterator BTreeFile::end() const {
	return {*this, numPages, 0};
}
