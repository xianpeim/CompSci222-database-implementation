#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

struct EntryInfo {
    int offset;
    int length;
};

typedef struct {
    int pageNum;    // page number
    int slotNum;    // slot number in the page
} EID;

class Entry{
public:
    Entry(){
        total_size = 0;
        buffer = NULL;
    }
    Entry(const Attribute &attribute, const void* key_in, const RID& rid) {
        if(DEBUG) std::cout << "entry constructor 1"<< std::endl;
        total_size = 0;
        buffer = NULL;
        rid_num = 0;
        key.attr = attribute;
        switch (key.attr.type){
            case TypeInt:
                memcpy(&key.int_val, (char*)key_in, sizeof(int));
                total_size += sizeof(int);
                break;
            case TypeReal:
                memcpy(&key.real_val, (char*)key_in, sizeof(float));
                total_size += sizeof(float);
                break;
                break;
            case TypeVarChar:
                int length;
                memcpy(&length, (char*)key_in, sizeof(int));
                key.attr.length = length;
                key.varchar_val = (char*)malloc(length);
                total_size += sizeof(int);
                memcpy(key.varchar_val, (char*)key_in+sizeof(int), length);
                total_size += length;
                break;
            default:
                break;
        }
        rids.push_back(rid);
        total_size += sizeof(RID);
        rid_num += 1;
    }
    Entry(const Attribute &attribute, char* in_buffer, EntryInfo ei){
        //if(DEBUG) std::cout << "entry constructor 2"<< std::endl;
        //if(DEBUG) std::cout << ei.length<< std::endl;
        //if(DEBUG) std::cout << ei.offset<< std::endl;
        total_size = ei.length;
        key.attr = attribute;
        buffer = (char*) malloc(ei.length);
        memcpy(buffer, in_buffer+ei.offset, ei.length);
        int offset = 0;
        switch (key.attr.type){
            case TypeInt:
                memcpy(&key.int_val, (char*)buffer, sizeof(int));
                offset += sizeof(int);
                break;
            case TypeReal:
                memcpy(&key.real_val, (char*)buffer, sizeof(float));
                offset += sizeof(float);
                break;
                break;
            case TypeVarChar:
                int length;
                memcpy(&length, (char*)buffer, sizeof(int));
                key.attr.length = length;
                key.varchar_val = (char*)malloc(length);
                offset += sizeof(int);
                memcpy(key.varchar_val, (char*)buffer+sizeof(int), length);
                offset += length;
                break;
            default:
                break;
        }
        rid_num = (total_size - offset)/ sizeof(RID);
        //if(DEBUG) std::cout << "rid num calculated: " << rid_num << std::endl;
        if((total_size - offset)%sizeof(RID)!=0) std::cerr << "something wrong with entry buffer" << std::endl;
        for(int i=0; i<rid_num; i++){
            RID rid;
            memcpy(&rid, buffer+offset, sizeof(RID));
            offset += sizeof(RID);
            rids.push_back(rid);
        }
        //if(DEBUG) std::cout << "entry constructor 2 end"<< std::endl;
    }

    ~Entry(){};
    std::vector<RID> rids;
    Field key;
    int total_size;
    int rid_num;
    char* buffer;
    RC updateBuffer();
    RC getKeyAndRid(void* data_out, RID& rid, int rid_index);
    //RC getSpecificData(void *data_out, const std::vector<std::string> &attributeNames);
    RC freeMalloc(){
        if(buffer!=NULL){
            free(buffer);
            buffer = NULL;
            if(DEBUGFREE) std::cout << "    entry buffer freed"<< std::endl;
        }
        if(key.attr.type == TypeVarChar and key.varchar_val!=NULL){
            free(key.varchar_val);
            key.varchar_val = NULL;
        }
        if(DEBUGFREE) std::cout << "    entry keys freed"<< std::endl;
        return 0;
    }
};

class Node{
public:
    Node(){
        if(DEBUG) std::cout << "Node constructor 1"<< std::endl;
        buffer = NULL;
    }
    Node(char* buffer_in, const Attribute &attribute){
        if(DEBUG) std::cout << "Node constructor 2"<< std::endl;
        buffer = (char*)malloc(PAGE_SIZE);
        memcpy(buffer, buffer_in, PAGE_SIZE);
        int back_offset = PAGE_SIZE, offset = 0;
        back_offset -= sizeof(int);
        memcpy(&label, buffer+back_offset, sizeof(int));
        back_offset -= sizeof(int);
        memcpy(&entry_num, buffer+back_offset, sizeof(int));
        back_offset -= sizeof(int);
        memcpy(&next_page, buffer+back_offset, sizeof(int));
        back_offset -= sizeof(AttrType);
        memcpy(&key_type, buffer+back_offset, sizeof(AttrType));
        back_offset -= sizeof(int);
        memcpy(&free_byte, buffer+back_offset, sizeof(int));
        if(DEBUG) std::cout << "num of label: "<< label << std::endl;
        if(DEBUG) std::cout << "num of entry: "<< entry_num << std::endl;
        if(DEBUG) std::cout << "num of next page: "<< next_page << std::endl;
        if(DEBUG) std::cout << "num of type: "<< key_type << std::endl;
        if(DEBUG) std::cout << "num of free byte: "<< free_byte << std::endl;
        for(int i=0; i<entry_num; i++){
            EntryInfo ei;
            back_offset -= sizeof(EntryInfo);
            memcpy(&ei, buffer+back_offset, sizeof(EntryInfo));
            infos.push_back(ei);
            if(ei.offset <= -1){
                Entry entry;
                entrys.push_back(entry);
                continue;
            }
            Entry entry(attribute, buffer, ei);
            offset += ei.length;
            entrys.push_back(entry);
        }
        if(label == 0){
            for(int i=0; i<entry_num+1; i++){
                int tmp;
                memcpy(&tmp, buffer+offset, sizeof(int));
                offset += sizeof(int);
                page_ptrs.push_back(tmp);
            }
        }
    }
    ~Node(){}
    char* buffer;
    std::vector<int> page_ptrs;
    std::vector<Entry> entrys;
    std::vector<EntryInfo> infos;

    int label;      // 1 for leaf, 0 for root.
    int entry_num;
    int next_page;
    AttrType key_type;
    int free_byte;


    RC updateBuffer();
    RC freeMalloc(){
        if(DEBUG) std::cout << "Node free malloc " << std::endl;
        if(buffer!=NULL){
            if(DEBUGFREE) std::cout << "  node buffer freed" << std::endl;
            free(buffer);
            buffer = NULL;
        }
        for(int i=0; i< entrys.size(); i++){
            if(infos[i].offset <= -1) continue;
            entrys[i].freeMalloc();
        }
        if(DEBUGFREE) std::cout << "  node records freed" << std::endl;
        return 0;
    }
};





class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

    RC searchForInsertionEID(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
            int page, EID& eid, std::vector<int>& trace, int& flag, int& index);

    RC getLeftScanEID(IXFileHandle &ixFileHandle, const Attribute &attribute, int page,
                       const void *lowKey, bool lowKeyInclusive, EID& eid, std::vector<int>& trace);

    RC getRightScanEID(IXFileHandle &ixFileHandle, const Attribute &attribute, int page,
                       const void *highKey, bool highKeyInclusive, EID& eid, std::vector<int>& trace);

    RC searchForScanEID(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                      int page, EID& eid, std::vector<int>& trace, CompOp op);

    RC splitLeafNode(IXFileHandle& ixFileHandle, const Attribute& attribute, std::vector<int>& trace, Node& node);

    RC InsertKeyToParent(IXFileHandle& ixFileHandle,const Attribute& attribute, std::vector<int>& trace,
            Field& border_key,int new_leaf_page_num, int level_of_parent);

    RC splitRootNode(IXFileHandle& ixFileHandle, const Attribute& attribute, std::vector<int>& trace, Node& node, int level);

    RC DFS(IXFileHandle& ixFileHandle, const Attribute& attribute, int page_num, int level, int last_flag) const;

    RC printLeafNode(IXFileHandle& ixFileHandle, const Attribute& attribute, Node node, int level) const;

    RC printRootNode(IXFileHandle& ixFileHandle, const Attribute& attribute, Node node, int level) const;

    RC printTab(int level) const;

    RC printRids(std::vector<RID> rids) const;

    RC printRid(RID rid) const;

    bool float_eq(float a, float b){
        if(abs(a-b) < epsilon) return true;
        else return false;
    }

    int rootIndex;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

private:
    static PagedFileManager& _pf_manager;

};

class IXFileHandle: public FileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter = FileHandle::readPageCounter;
    unsigned ixWritePageCounter = FileHandle::writePageCounter;
    unsigned ixAppendPageCounter = FileHandle::appendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    static IXFileHandle& instance() {
        static IXFileHandle _ixf_handle = IXFileHandle();
        return _ixf_handle;
    }

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    int getRootIndex(){
        return FileHandle::rootIndex;
    }

    RC updateRootIndex(int new_rootIndex){
        FileHandle::rootIndex = new_rootIndex;
        return 1;
    }
};


class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator(){
        cur_index = 0;
        cur_rid_index = 0;
        last_rid_size = -1;
    };

    // Destructor
    ~IX_ScanIterator(){

    };

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key){

        EID eid;
        if(DEBUG) std::cout << "eids size: "<< eids.size() << std::endl;
        if(cur_index == eids.size()) return IX_EOF;
        eid.slotNum = eids[cur_index].slotNum;
        eid.pageNum = eids[cur_index].pageNum;
        if(DEBUG) std::cout<<"getting next record"<< std::endl;
        if(DEBUG) std::cout<<"eid scanned: " << eid.pageNum << " " << eid.slotNum << std::endl;
        char* buffer = (char*)malloc(PAGE_SIZE);
        ixFileHandle.readPage(eid.pageNum, buffer);
        Node node(buffer, attr);
        if(node.entry_num==0) return IX_EOF;
        //if(DEBUG) std::cout << "page infos size: " << page.infos.size() << std::endl;
        EntryInfo ei;
        memcpy(&ei, &node.infos[eid.slotNum], sizeof(EntryInfo));
        if(ei.offset == -1) return -1;
        /*if(ei.offset < -1){
            RID rid_point_to;
            rid_point_to.pageNum = ri.offset + 10000000;
            rid_point_to.slotNum = ri.length;
            return getNextRecord(rid_point_to, data);
        }*/
        Entry entry(attr, buffer, ei);
        if(DEBUG) std::cout << "rid num: "<< entry.rid_num << std::endl;
        if(DEBUG) std::cout << "rids.size(should be same): "<< entry.rids.size() << std::endl;
        if(entry.rid_num==0) return IX_EOF;

        if(last_rid_size == entry.rid_num+1){
            std::cout << "aaa: " << last_rid_size << " " << entry.rid_num << std::endl;
            last_rid_size = entry.rid_num;
            cur_rid_index -= 1;
        }

        entry.getKeyAndRid(key,rid,cur_rid_index);
        if(last_rid_size==-1){
            last_rid_size = entry.rid_num;
        }

        cur_rid_index += 1;

        if(cur_rid_index == entry.rid_num){
            cur_index += 1;
            cur_rid_index = 0;
            last_rid_size = -1;
        }
        free(buffer);
        buffer = NULL;
        node.freeMalloc();
        entry.freeMalloc();

        return 0;
    };

    // Clear stuff
    RC clear(){
        cur_index = 0;
        cur_rid_index = 0;
        last_rid_size = -1;
        eids.clear();
        return 0;
    }

    // Terminate index scan
    RC close(){
        clear();
        return 0;
    };

    RC setFileHandle(IXFileHandle &ixFileHandle){
        this->ixFileHandle = ixFileHandle;
        return 0;
    }

    RC setAttr(const Attribute attr){
        this->attr = attr;
        return 0;
    }

    Attribute attr;
    std::vector<EID> eids;
    int cur_index;
    int cur_rid_index;
    int last_rid_size;
    IXFileHandle ixFileHandle;
};


#endif