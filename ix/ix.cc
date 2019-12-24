#include "ix.h"

// #TODO: add ordering for rid inside same key

//FileHandle& IXFileHandle::_f_handle = FileHandle::instance();
PagedFileManager& IndexManager::_pf_manager = PagedFileManager::instance();

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
    if(_pf_manager.createFile(fileName) != 0) return -1;
    rootIndex = 0;
    return 0;
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return _pf_manager.destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    //ixFileHandle.updateRootIndex(123);
    //std::cout << ixFileHandle.rootIndex << std::endl;
    if(_pf_manager.openFile(fileName, ixFileHandle) != 0 ) return -1;
    // check to get num of page since sharing same rbfm
    if(ixFileHandle.getNumberOfPages() == 0){
    }
    else{
    }
    rootIndex = ixFileHandle.getRootIndex();
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    ixFileHandle.updateRootIndex(rootIndex);
    return _pf_manager.closeFile(ixFileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(DEBUG){
        std::cout<<"inserting index entry" << std::endl;
        switch (attribute.type){
            case TypeInt:
                int tmp1;
                memcpy(&tmp1, key, sizeof(int));
                std::cout<< "int key: " << tmp1 << std::endl;
                break;
            case TypeReal:
                float tmp2;
                memcpy(&tmp2, key, sizeof(float));
                std::cout<< "float key: " << tmp2 << std::endl;
                break;
            case TypeVarChar:
                std::cout<< "too lazy to print varchar key now" << std::endl;
                break;
        }
    }
    if(ixFileHandle.getFilePtr() == NULL) return -5;
    Entry entry(attribute, key, rid);
    entry.updateBuffer();
    //empty file case
    if(ixFileHandle.getNumberOfPages() == 0){
        if(DEBUG) std::cout << "new file" << std::endl;
        rootIndex = 0;

        Node node;
        node.label = 1;
        node.entry_num = 0;
        node.next_page = -1;
        node.key_type = entry.key.attr.type;

        EntryInfo ei;
        ei.length = entry.total_size;
        ei.offset = 0;
        node.entrys.push_back(entry);
        node.infos.push_back(ei);
        node.entry_num += 1;

        node.updateBuffer();
        ixFileHandle.appendPage(node.buffer);
        node.freeMalloc();
        return 0;
    }
    if(DEBUG) std::cout << "looking for page to be inserted: " << std::endl;
    EID eid;
    std::vector<int> trace;
    int flag = 0, index=0;
    searchForInsertionEID(ixFileHandle, attribute, key, rootIndex, eid, trace, flag, index);
    if(DEBUG) std::cout << "page to be inserted: " << eid.pageNum << " index: " << eid.slotNum << " flag: " << flag << std::endl;
    //if(DEBUG) std::cout << "trace length: " << trace.size() << std::endl;
    char* buffer = (char*)malloc(PAGE_SIZE);
    ixFileHandle.readPage(eid.pageNum, buffer);
    Node node(buffer, attribute);
    free(buffer);
    buffer = NULL;
    EntryInfo ei;
    // TODO: a huge key may result in extra split leaf node.
    if(node.free_byte > entry.total_size + sizeof(EntryInfo)){
        //if(DEBUG) std::cout << "rid num in entry: "<<node.entrys[eid.slotNum].rid_num << std::endl;
        switch(flag){
            case 0:
                // insert at the last
                node.entrys.push_back(entry);
                node.entry_num += 1;
                //if(DEBUG) std::cout << "insert rid: " <<entry.rids[0].pageNum<<" "<<entry.rids[0].slotNum<< std::endl;

                ei.length = entry.total_size;
                //ei.offset = eid.slotNum;
                ei.offset = node.infos[node.entry_num-2].offset+node.infos[node.entry_num-2].length;
                node.infos.push_back(ei);

                node.updateBuffer();
                ixFileHandle.writePage(eid.pageNum, node.buffer);
                node.freeMalloc();
                break;
            case 1:
                // insert at index and move other to index+1
                node.entrys.insert(node.entrys.begin()+index, entry);
                node.entry_num += 1;

                //if(DEBUG) std::cout << "insert rid: " <<entry.rids[0].pageNum<<" "<<entry.rids[0].slotNum<< std::endl;
                ei.length = entry.total_size;
                //ei.offset = eid.slotNum;
                ei.offset = node.infos[index].offset;
                node.infos.insert(node.infos.begin()+index, ei);
                for(int i=index+1; i<node.entry_num; i++){
                    node.infos[i].offset += ei.length;
                }

                node.updateBuffer();
                ixFileHandle.writePage(eid.pageNum, node.buffer);
                node.freeMalloc();

                break;
            case 2:
                // insert at index key, extra rid
                for(int j=0; j<node.entrys[eid.slotNum].rids.size(); j++){
                   if(node.entrys[eid.slotNum].rids[j].slotNum == rid.slotNum and node.entrys[eid.slotNum].rids[j].pageNum == rid.pageNum){
                       std::cerr << "inserting same rid" << std::endl;
                       return -1;
                   }
                }
                node.entrys[eid.slotNum].rids.push_back(rid);
                node.entrys[eid.slotNum].rid_num += 1;
                node.entrys[eid.slotNum].total_size += sizeof(RID);
                node.entrys[eid.slotNum].updateBuffer();
                node.infos[eid.slotNum].length += sizeof(RID);
                for(int k=eid.slotNum+1; k<node.entry_num;k++){
                    node.infos[k].offset += sizeof(RID);
                }

                node.updateBuffer();
                /*if(DEBUG) std::cout << node.entrys[eid.slotNum].rid_num << std::endl;
                for(int j=0;j<node.entrys[eid.slotNum].rid_num;j++){
                    if(DEBUG) std::cout << node.entrys[eid.slotNum].rids[j].pageNum << " "<<node.entrys[eid.slotNum].rids[j].slotNum << std::endl;
                }*/
                ixFileHandle.writePage(eid.pageNum, node.buffer);
                node.freeMalloc();
                break;
        }
        return  0;
    }else{
        // need to split page.

        //if(DEBUG) std::cout << "rid num in entry: "<<node.entrys[eid.slotNum].rid_num << std::endl;
        switch(flag){
            case 0:
                // insert at the last
                node.entrys.push_back(entry);
                ei.length = entry.total_size;
                //ei.offset = node.entry_num;
                ei.offset = node.infos[node.entry_num-2].offset+node.infos[node.entry_num-2].length;
                node.infos.push_back(ei);
                node.entry_num += 1;
                splitLeafNode(ixFileHandle, attribute, trace, node);

                node.freeMalloc();
                break;
            case 1:
                // insert at index and move other to index+1
                node.entrys.insert(node.entrys.begin()+index, entry);
                node.entry_num += 1;

                //if(DEBUG) std::cout << "insert rid: " <<entry.rids[0].pageNum<<" "<<entry.rids[0].slotNum<< std::endl;
                ei.length = entry.total_size;
                //ei.offset = eid.slotNum;
                ei.offset = node.infos[index].offset;
                node.infos.insert(node.infos.begin()+index, ei);
                for(int i=index+1; i<node.entry_num; i++){
                    node.infos[i].offset += ei.length;
                }

                splitLeafNode(ixFileHandle, attribute, trace, node);

                node.freeMalloc();
                break;
            case 2:
                // insert at index key, extra rid
                /*for(int j=0; j<node.entrys[eid.slotNum].rids.size(); j++){
                    if(node.entrys[i].rids[j].slotNum == rid.slotNum and node.entrys[i].rids[j].pageNum == rid.pageNum){
                        std::cerr << "inserting same rid" << std::endl;
                        return -1;
                    }
                }*/
                node.entrys[eid.slotNum].rids.push_back(rid);
                node.entrys[eid.slotNum].rid_num += 1;
                node.entrys[eid.slotNum].total_size += sizeof(RID);
                node.entrys[eid.slotNum].updateBuffer();
                node.infos[eid.slotNum].length += sizeof(RID);
                for(int k=eid.slotNum+1; k<node.entry_num;k++){
                    node.infos[k].offset += sizeof(RID);
                }

                splitLeafNode(ixFileHandle, attribute, trace, node);
                /*if(DEBUG) std::cout << node.entrys[eid.slotNum].rid_num << std::endl;
                for(int j=0;j<node.entrys[eid.slotNum].rid_num;j++){
                    if(DEBUG) std::cout << node.entrys[eid.slotNum].rids[j].pageNum << " "<<node.entrys[eid.slotNum].rids[j].slotNum << std::endl;
                }*/
                node.freeMalloc();
                break;
        }
        return  0;

    }

}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(ixFileHandle.getFilePtr() == NULL) return -5;
    IX_ScanIterator ix_ScanIterator;
    scan(ixFileHandle, attribute, key, key, true, true, ix_ScanIterator);
    if(DEBUG)std::cout << "deletion scanned eids size: " << ix_ScanIterator.eids.size() << std::endl;
    if(ix_ScanIterator.eids.size() == 0){
        if(DEBUG) std::cerr << "cannot find key" << std::endl;
        return -1;
    }
    if(ix_ScanIterator.eids.size() == 0){
        if(DEBUG) std::cerr << "found more than 1 keys" << std::endl;
        return -3;
    }
    EID eid = ix_ScanIterator.eids[0];
    if(DEBUG)std::cout << "eid: " << eid.pageNum << " " << eid.slotNum << std::endl;
    if(eid.slotNum == -1 or eid.pageNum == -1){
        std::cerr << "key doesnt exist" << std::endl;
        return -2;
    }
    char* buffer = (char*)malloc(PAGE_SIZE);
    ixFileHandle.readPage(eid.pageNum, buffer);
    Node node(buffer, attribute);
    Entry entry(attribute, node.buffer, node.infos[eid.slotNum]);
    if(DEBUG)std::cout << "entry: " << entry.key.real_val << " " << entry.rids[0].pageNum << " " << entry.rids[0].slotNum << std::endl;
    if(entry.rid_num==1){
        //delete entry from node
        if(DEBUG)std::cout << "delete whole entry" << std::endl;
        int length = node.infos[eid.slotNum].length;
        for(int i=eid.slotNum+1;i<node.entry_num;i++){
            node.infos[i].offset -= length;
        }
        node.entrys.erase(node.entrys.begin()+eid.slotNum);
        node.infos.erase(node.infos.begin()+eid.slotNum);
        node.entry_num -= 1;
        node.updateBuffer();
        ixFileHandle.writePage(eid.pageNum, node.buffer);
        node.freeMalloc();
        entry.freeMalloc();
        free(buffer);
        buffer = NULL;
        return 0;
    } else{
        //delete rid from entry
        if(DEBUG)std::cout << "delete rid from entry" << std::endl;
        for(int i =0; i< entry.rid_num; i++){
            if(node.entrys[eid.slotNum].rids[i].pageNum == rid.pageNum and node.entrys[eid.slotNum].rids[i].slotNum == rid.slotNum){
                node.entrys[eid.slotNum].rids.erase(node.entrys[eid.slotNum].rids.begin()+i);
                break;
            }
        }
        node.entrys[eid.slotNum].rid_num -= 1;
        node.entrys[eid.slotNum].total_size -= sizeof(RID);
        node.entrys[eid.slotNum].updateBuffer();
        node.infos[eid.slotNum].length -= sizeof(RID);
        node.updateBuffer();
        ixFileHandle.writePage(eid.pageNum, node.buffer);
        node.freeMalloc();
        entry.freeMalloc();
        free(buffer);
        buffer = NULL;
        return 0;
    }
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {

    // get eid for low and hight keys. then just go over everything in between
    if(ixFileHandle.getFilePtr() == NULL) return -5;
    if(ixFileHandle.getNumberOfPages()==0) return -6;
    ix_ScanIterator.setFileHandle(ixFileHandle);
    ix_ScanIterator.setAttr(attribute);
    EID left_eid, right_eid;
    std::vector<int> left_trace, right_trace;
    getLeftScanEID(ixFileHandle,attribute,rootIndex,lowKey,lowKeyInclusive,left_eid,left_trace);
    getRightScanEID(ixFileHandle,attribute,rootIndex,highKey,highKeyInclusive,right_eid,right_trace);
    if(DEBUG) std::cout << "left eid: " <<left_eid.pageNum<<" "<<left_eid.slotNum<< std::endl;
    if(DEBUG) std::cout << "right eid: " <<right_eid.pageNum<<" "<<right_eid.slotNum<< std::endl;

    /*char* buffer1 = (char*)malloc(PAGE_SIZE);
    ixFileHandle.readPage(right_eid.pageNum, buffer1);
    Node node(buffer1, attribute);
    if(DEBUG) std::cout << "right eid key: " << node.entrys[right_eid.slotNum].key.real_val << std::endl;
    return -1;*/

    char* buffer = (char*)malloc(PAGE_SIZE);
    //ixFileHandle.readPage(left_eid.pageNum, buffer);
    if(left_eid.pageNum<0 or left_eid.slotNum<0 or right_eid.slotNum<0 or right_eid.pageNum<0){
        // #TODO: check this condition, better solution if possible. prob empty page condition
        //if(DEBUG) std::cerr << "scan left or right eid has negative value" << std::endl;

        return 0;
    }
    int i, cur_page = left_eid.pageNum, flag = 0;
    while (1){
        if(cur_page==left_eid.pageNum) i = left_eid.slotNum;
        else i = 0;
        ixFileHandle.readPage(cur_page, buffer);
        Node node(buffer, attribute);
        for(;i<node.entry_num;i++){

            EID eid;
            eid.pageNum = cur_page;
            eid.slotNum = i;
            ix_ScanIterator.eids.push_back(eid);
            if(cur_page==right_eid.pageNum and i==right_eid.slotNum){
                flag = 1;
                break;
            }
        }
        if(flag==1){
            node.freeMalloc();
            return 0;
        }
        cur_page = node.next_page;
        if(cur_page == -1){
            std::cerr << "scan did not stop as expected" << std::endl;
            return -2;
        }
        node.freeMalloc();
    }

    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    DFS(ixFileHandle,attribute,rootIndex, 0, 1);

}

IXFileHandle::IXFileHandle() {
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    //readPageCount = ixReadPageCounter;
    //writePageCount = ixWritePageCounter;
    //appendPageCount = ixAppendPageCounter;
    readPageCount = FileHandle::readPageCounter;
    writePageCount = FileHandle::writePageCounter;
    appendPageCount = FileHandle::appendPageCounter;
    return 0;
}

RC Entry::updateBuffer(){
    if(buffer!=NULL){
        free(buffer);
        buffer = NULL;
    }
    buffer = (char*)malloc(total_size);
    int offset = 0;
    switch (key.attr.type){
        case TypeInt:
            memcpy(buffer+offset, &key.int_val, sizeof(int));
            offset += sizeof(int);
            break;
        case TypeReal:
            memcpy(buffer+offset, &key.real_val, sizeof(float));
            offset += sizeof(float);
            break;
        case TypeVarChar:
            memcpy(buffer+offset,&key.attr.length, sizeof(int));
            offset += sizeof(int);
            memcpy(buffer+offset,key.varchar_val, key.attr.length);
            offset += key.attr.length;
            break;
        default:
            break;
    }
    for(int i=0; i<rids.size();i++){
        memcpy(buffer+offset,&rids[i], sizeof(RID));
        offset += sizeof(RID);
    }
}

RC Node::updateBuffer(){
    if(buffer!=NULL) {
        free(buffer);
        buffer = NULL;
    }
    buffer = (char*)malloc(PAGE_SIZE);
    int offset = 0;
    int back_offset = PAGE_SIZE;
    for(int i=0; i<entrys.size(); i++){
        memcpy(buffer+offset, entrys[i].buffer, entrys[i].total_size);
        offset += entrys[i].total_size;
    }
    for(int i=0; i<page_ptrs.size(); i++){
        memcpy(buffer+offset, &page_ptrs[i], sizeof(int));
        offset += sizeof(int);
    }
    back_offset -= sizeof(int);
    memcpy(buffer+back_offset, &label, sizeof(int));
    back_offset -= sizeof(int);
    memcpy(buffer+back_offset, &entry_num, sizeof(int));
    back_offset -= sizeof(int);
    memcpy(buffer+back_offset, &next_page, sizeof(int));
    back_offset -= sizeof(AttrType);
    memcpy(buffer+back_offset, &key_type, sizeof(AttrType));
    back_offset -= sizeof(int);
    free_byte = back_offset - offset - entry_num*sizeof(EntryInfo);
    memcpy(buffer+back_offset, &free_byte, sizeof(int));
    for(int i=0; i<entry_num; i++){
        back_offset -= sizeof(EntryInfo);
        memcpy(buffer+back_offset, &infos[i], sizeof(EntryInfo));
    }
    return 0;
}

RC IndexManager::searchForInsertionEID(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
        int page, EID& eid, std::vector<int>& trace, int& flag, int& index){

    int tmp_page = page;
    trace.push_back(tmp_page);
    char* buffer;
    while(1){
        buffer = (char*)malloc(PAGE_SIZE);
        ixFileHandle.readPage(tmp_page, buffer);
        Node node(buffer, attribute);
        if(node.label == 1){

            flag = 0;       //flag = 0 insert at last. flag = 1 insert on left of index i. flag = 2 insert right here at index i
            int i = 0;
            for(; i< node.entry_num; i++){
                switch (node.key_type){
                    case TypeInt:
                        if(*(int*)key < node.entrys[i].key.int_val){
                            flag = 1;
                            index = i;
                            break;
                        }
                        if(*(int*)key == node.entrys[i].key.int_val){
                            flag = 2;
                            index = i;
                            break;
                        }
                        break;
                    case TypeReal:
                        if(*(float*)key < node.entrys[i].key.real_val){
                            flag = 1;
                            index = i;
                            break;
                        }
                        //if(*(float*)key == node.entrys[i].key.real_val){
                        if(float_eq(*(float*)key, node.entrys[i].key.real_val)){
                            flag = 2;
                            index = i;
                            break;
                        }
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy( &length, (char*)key, sizeof(int));
                        //std::cout << length << " " << node.entrys[i].key.attr.length << std::endl;
                        if(length < node.entrys[i].key.attr.length){
                            flag = 1;
                            index = i;
                            break;
                        }
                        if(length == node.entrys[i].key.attr.length) {
                            if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) < 0) {
                                flag = 1;
                                index = i;
                                break;
                            }
                            if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) == 0) {
                                flag = 2;
                                index = i;
                                break;
                            }
                        }
                        break;
                    default:
                        break;
                }
                if(flag==1){
                    break;
                }
                if(flag==2){
                    break;
                }
            }
            eid.pageNum = tmp_page;
            eid.slotNum = i;
            node.freeMalloc();
            free(buffer);
            buffer = NULL;
            return 0;

        }else{
            // search tree;
            int new_page, i;
            flag = 0;
            for(i=0; i<node.entry_num; i++){
                switch (node.key_type){     // left close, right open.
                    case TypeInt:
                        if(*(int*)key < node.entrys[i].key.int_val){
                            flag = 1;
                            new_page = node.page_ptrs[i];
                            break;
                        }
                        break;
                    case TypeReal:
                        if(*(float*)key < node.entrys[i].key.real_val){
                            flag = 1;
                            new_page = node.page_ptrs[i];
                            break;
                        }
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy( &length, key, sizeof(int));
                        if(length < node.entrys[i].key.attr.length){
                            flag = 1;
                            new_page = node.page_ptrs[i];
                            break;
                        }
                        if(length == node.entrys[i].key.attr.length) {
                            if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) < 0) {
                                flag = 1;
                                new_page = node.page_ptrs[i];
                                break;
                            }
                        }
                        break;
                    default:
                        break;
                }
                if(flag==1) break;
            }
            if(flag==0) new_page = node.page_ptrs[i];
            node.freeMalloc();
            free(buffer);
            buffer = NULL;
            searchForInsertionEID(ixFileHandle, attribute, key, new_page, eid, trace, flag, index);
            return 0;
        }
    }
}

RC IndexManager::getLeftScanEID(IXFileHandle &ixFileHandle, const Attribute &attribute, int page,
                                 const void *lowKey, bool lowKeyInclusive, EID& eid, std::vector<int>& trace){
    if(lowKey==NULL){
        int tmp_page = page;
        char* buffer;
        //while(1){
            buffer = (char*)malloc(PAGE_SIZE);
            ixFileHandle.readPage(tmp_page, buffer);
            Node node(buffer, attribute);
            if(node.label == 1){
                node.freeMalloc();
                free(buffer);
                buffer = NULL;
                eid.pageNum = tmp_page;
                eid.slotNum = 0;
                return 0;
            }else{
                // search tree;
                node.freeMalloc();
                free(buffer);
                buffer = NULL;
                return getLeftScanEID(ixFileHandle, attribute, node.page_ptrs[0], lowKey, lowKeyInclusive, eid, trace);
            }
        //}
    } else{
        if(lowKeyInclusive) return searchForScanEID(ixFileHandle,attribute,lowKey,page,eid,trace,GE_OP);
        else return searchForScanEID(ixFileHandle,attribute,lowKey,page,eid,trace,GT_OP);
    }
}

RC IndexManager::getRightScanEID(IXFileHandle &ixFileHandle, const Attribute &attribute, int page,
                                 const void *highKey, bool highKeyInclusive, EID& eid, std::vector<int>& trace){
    if(highKey==NULL){
        int tmp_page = page;
        char* buffer;
        //while(1){
            buffer = (char*)malloc(PAGE_SIZE);
            ixFileHandle.readPage(tmp_page, buffer);
            Node node(buffer, attribute);
            if(node.label == 1){
                node.freeMalloc();
                free(buffer);
                buffer = NULL;
                eid.pageNum = tmp_page;
                eid.slotNum = node.entry_num-1;
                return 0;
            }else{
                // search tree;
                node.freeMalloc();
                free(buffer);
                buffer = NULL;
                return getRightScanEID(ixFileHandle, attribute, node.page_ptrs[node.entry_num], highKey, highKeyInclusive, eid, trace);
            }
        //}
    } else{
        if(highKeyInclusive) return searchForScanEID(ixFileHandle,attribute,highKey,page,eid,trace,LE_OP);
        else return searchForScanEID(ixFileHandle,attribute,highKey,page,eid,trace,LT_OP);
    }
}

// GT - left not inclusive, GE - left inclusive, LT - right not inclusive, LE - right inclusive
RC IndexManager::searchForScanEID(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                       int page, EID& eid, std::vector<int>& trace, const CompOp compOp){
    if(DEBUG) std::cerr << "--------search for scan id---------" << std::endl;
    if(DEBUG) std::cout <<"new page search: " << page << std::endl;
    int tmp_page = page;
    trace.push_back(tmp_page);
    char* buffer;
    if(1){
        buffer = (char*)malloc(PAGE_SIZE);
        ixFileHandle.readPage(tmp_page, buffer);
        Node node(buffer, attribute);
        if(DEBUG){
            if(node.label==0){
                for(int x=0;x<node.page_ptrs.size();x++){
                    std::cout <<"root page ptr: " << node.page_ptrs[x] << std::endl;
                }
            }
        }
        if(node.label == 1){
            int flag = 0;
            int i;
            if(node.entry_num == 0){
                eid.pageNum = -1;
                eid.slotNum = -1;
                return 0;
            }
            switch (compOp) {
                case GT_OP:
                    for(i=0; i< node.entry_num; i++){
                        switch (node.key_type){
                            case TypeInt:
                                if(*(int*)key < node.entrys[i].key.int_val){
                                    flag = 1;
                                    break;
                                }
                                break;
                            case TypeReal:
                                if(*(float*)key < node.entrys[i].key.real_val){
                                    flag = 1;
                                    break;
                                }
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy( &length, key, sizeof(int));
                                if(length < node.entrys[i].key.attr.length){
                                    flag = 1;
                                    break;
                                }
                                if(length == node.entrys[i].key.attr.length) {
                                    if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) <
                                        0) {
                                        flag = 1;
                                        break;
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        if(flag==1){
                            break;
                        }
                    }
                    if(flag == 0){
                        eid.pageNum = node.next_page;
                        eid.slotNum = 0;
                    } else {
                        eid.pageNum = tmp_page;
                        eid.slotNum = i;
                    }
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    return 0;

                case GE_OP:
                    for(i=0; i< node.entry_num; i++){
                        switch (node.key_type){
                            case TypeInt:
                                if(*(int*)key <= node.entrys[i].key.int_val){
                                    flag = 1;
                                    break;
                                }
                                break;
                            case TypeReal:
                                if(*(float*)key <= node.entrys[i].key.real_val){
                                    flag = 1;
                                    break;
                                }
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy( &length, key, sizeof(int));
                                if(length < node.entrys[i].key.attr.length){
                                    flag = 1;
                                    break;
                                }
                                if(length == node.entrys[i].key.attr.length) {
                                    if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) <=
                                        0) {
                                        flag = 1;
                                        break;
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        if(flag==1){
                            break;
                        }
                    }
                    if(flag == 0) {
                        eid.pageNum = node.next_page;
                        eid.slotNum = 0;
                    } else {
                        eid.pageNum = tmp_page;
                        eid.slotNum = i;
                    }
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    return 0;

                case LE_OP:
                    for(i=node.entry_num-1; i>=0; i--){
                        switch (node.key_type){
                            case TypeInt:
                                if(*(int*)key >= node.entrys[i].key.int_val){
                                    flag = 1;
                                    break;
                                }
                                break;
                            case TypeReal:
                                if(*(float*)key >= node.entrys[i].key.real_val){
                                    flag = 1;
                                    break;
                                }
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy( &length, key, sizeof(int));
                                if(length > node.entrys[i].key.attr.length){
                                    flag = 1;
                                    break;
                                }
                                if(length == node.entrys[i].key.attr.length) {
                                    if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) >=
                                        0) {
                                        flag = 1;
                                        break;
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        if(flag==1){
                            break;
                        }
                    }
                    if(flag==0){
                        std::cerr << "this should not happen. didnt find right scan eid" << std::endl;
                        return -2;
                    }
                    eid.pageNum = tmp_page;
                    eid.slotNum = i;
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    return 0;

                    case LT_OP:
                        for(i=node.entry_num-1; i>=0; i--){
                            switch (node.key_type){
                                case TypeInt:
                                    if(*(int*)key > node.entrys[i].key.int_val){
                                        flag = 1;
                                        break;
                                    }
                                    break;
                                case TypeReal:
                                    if(*(float*)key > node.entrys[i].key.real_val){
                                        flag = 1;
                                        break;
                                    }
                                    break;
                                case TypeVarChar:
                                    int length;
                                    memcpy( &length, key, sizeof(int));
                                    if(length > node.entrys[i].key.attr.length){
                                        flag = 1;
                                        break;
                                    }
                                    if(length == node.entrys[i].key.attr.length) {
                                        if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) >
                                            0) {
                                            flag = 1;
                                            break;
                                        }
                                    }
                                    break;
                                default:
                                    break;
                            }
                            if(flag==1){
                                break;
                            }
                        }
                    if(flag==0){
                        std::cerr << "this should not happen. didnt find right scan eid" << std::endl;
                        return -2;
                    }
                    eid.pageNum = tmp_page;
                    eid.slotNum = i;
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    return 0;

                default:
                    return -1;
            }
        }else{
            // search tree;
            int new_page, flag=0, i;
            switch(compOp) {
                case GT_OP:
                    for (i = 0; i < node.entry_num; i++) {
                        switch (node.key_type) {     // left close, right open.
                            case TypeInt:
                                if (*(int *) key < node.entrys[i].key.int_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i];
                                    break;
                                }
                                break;
                            case TypeReal:
                                if (*(float *) key < node.entrys[i].key.real_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i];
                                    break;
                                }
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy(&length, key, sizeof(int));
                                if(length < node.entrys[i].key.attr.length){
                                    flag = 1;
                                    new_page = node.page_ptrs[i];
                                    break;
                                }
                                if(length == node.entrys[i].key.attr.length) {
                                    if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) <
                                        0) {
                                        flag = 1;
                                        new_page = node.page_ptrs[i];
                                        break;
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        if (flag == 1) break;
                    }
                    if (flag == 0) new_page = node.page_ptrs[i];
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    searchForScanEID(ixFileHandle, attribute, key, new_page, eid, trace, compOp);
                    return 0;

                case GE_OP:
                    for (i = 0; i < node.entry_num; i++) {
                        switch (node.key_type) {     // left close, right open.
                            case TypeInt:
                                if (*(int *) key < node.entrys[i].key.int_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i];
                                    break;
                                }
                                break;
                            case TypeReal:
                                if (*(float *) key < node.entrys[i].key.real_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i];
                                    break;
                                }
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy(&length, key, sizeof(int));
                                if(length < node.entrys[i].key.attr.length){
                                    flag = 1;
                                    new_page = node.page_ptrs[i];
                                    break;
                                }
                                if(length == node.entrys[i].key.attr.length) {
                                    if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) <
                                        0) {
                                        flag = 1;
                                        new_page = node.page_ptrs[i];
                                        break;
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        if (flag == 1) break;
                    }
                    if (flag == 0) new_page = node.page_ptrs[i];
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    searchForScanEID(ixFileHandle, attribute, key, new_page, eid, trace, compOp);
                    return 0;

                case LT_OP:
                    for (i = node.entry_num-1; i >=0 ; i--) {
                        switch (node.key_type) {     // left close, right open.
                            case TypeInt:
                                if (*(int *) key > node.entrys[i].key.int_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i+1];
                                    break;
                                }
                                break;
                            case TypeReal:
                                if (*(float *) key > node.entrys[i].key.real_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i+1];
                                    break;
                                }
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy(&length, key, sizeof(int));
                                if(length > node.entrys[i].key.attr.length){
                                    flag = 1;
                                    new_page = node.page_ptrs[i+1];
                                    break;
                                }
                                if(length == node.entrys[i].key.attr.length) {
                                    if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) >
                                        0) {
                                        flag = 1;
                                        new_page = node.page_ptrs[i + 1];
                                        break;
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        if (flag == 1) break;
                    }
                    if (flag == 0) new_page = node.page_ptrs[i+1];
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    searchForScanEID(ixFileHandle, attribute, key, new_page, eid, trace, compOp);
                    return 0;

                case LE_OP:
                    for (i = node.entry_num-1; i >=0 ; i--) {
                        switch (node.key_type) {     // left close, right open.
                            case TypeInt:
                                if (*(int *) key >= node.entrys[i].key.int_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i+1];
                                    break;
                                }
                                break;
                            case TypeReal:
                                if (*(float *) key >= node.entrys[i].key.real_val) {
                                    flag = 1;
                                    new_page = node.page_ptrs[i+1];
                                    break;
                                }
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy(&length, key, sizeof(int));
                                if(length > node.entrys[i].key.attr.length){
                                    flag = 1;
                                    new_page = node.page_ptrs[i+1];
                                    break;
                                }
                                if(length == node.entrys[i].key.attr.length) {
                                    if (memcmp((char *) key + sizeof(int), node.entrys[i].key.varchar_val, length) >=
                                        0) {
                                        flag = 1;
                                        new_page = node.page_ptrs[i + 1];
                                        break;
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                        if (flag == 1) break;
                    }
                    if (flag == 0) new_page = node.page_ptrs[i+1];
                    node.freeMalloc();
                    free(buffer);
                    buffer = NULL;
                    searchForScanEID(ixFileHandle, attribute, key, new_page, eid, trace, compOp);
                    return 0;

                default:
                    return -1;
            }
        }
    }
}

RC Entry::getKeyAndRid(void* data_out, RID& rid, int rid_index){
    switch (key.attr.type){
        case TypeInt:
            memcpy((char*)data_out, &key.int_val, sizeof(int));
            break;
        case TypeReal:
            memcpy((char*)data_out, &key.real_val, sizeof(float));
            break;
        case TypeVarChar:
            memcpy((char*)data_out, &key.attr.length, sizeof(int));
            memcpy((char*)data_out+sizeof(int), key.varchar_val, key.attr.length);
            break;
    }
    memcpy(&rid, &rids[rid_index], sizeof(RID));
    return 0;
}

RC IndexManager::splitLeafNode(IXFileHandle& ixFileHandle, const Attribute& attribute, std::vector<int>& trace, Node& node){
    if(DEBUG) std::cout << "Leaf Node split called." << std::endl;
    int half_length = node.entry_num/2;
    Field border_key = node.entrys[half_length].key;
    Node new_leaf_node;
    new_leaf_node.entry_num = 0;
    if(node.next_page!= -1) new_leaf_node.next_page = node.next_page;
    else new_leaf_node.next_page = -1;
    new_leaf_node.label = 1;
    new_leaf_node.key_type = node.key_type;
    int new_leaf_page_num = ixFileHandle.getNumberOfPages();
    node.next_page = new_leaf_page_num;
    int i = half_length, new_count = 0, new_offset = 0;
    if(DEBUG) std::cout << "half_length: " << half_length << std::endl;
    for(;i<node.entry_num;i++){
        new_leaf_node.entrys.push_back(node.entrys[i-new_count]);
        //if(DEBUG) std::cout << "entry size: " <<  node.entrys.size() << std::endl;
        node.entrys.erase(node.entrys.begin()+i-new_count);
        EntryInfo ei;
        ei.length = node.infos[i-new_count].length;
        //if(DEBUG) std::cout << "ei length: " << ei.length << std::endl;
        ei.offset = new_offset;
        new_leaf_node.infos.push_back(ei);
        node.infos.erase(node.infos.begin()+i-new_count);
        new_offset += ei.length;
        new_count += 1;
        new_leaf_node.entry_num += 1;
    }
    node.entry_num -= new_leaf_node.entry_num;
    if(DEBUG) std::cout << "split loop done, write next" << std::endl;
    if(DEBUG) std::cout << "node entry num: " <<  node.entry_num << std::endl;
    if(DEBUG) std::cout << "node entry size: " <<  node.entrys.size() << std::endl;
    node.updateBuffer();
    new_leaf_node.updateBuffer();
    ixFileHandle.writePage(trace[trace.size()-1], node.buffer);
    //ixFileHandle.writePage(new_leaf_page_num, new_leaf_node.buffer);
    ixFileHandle.appendPage(new_leaf_node.buffer);

    //leaf node done. next, go back along trace and change roots
    InsertKeyToParent(ixFileHandle, attribute, trace, border_key, new_leaf_page_num, 1);

}

RC IndexManager::InsertKeyToParent(IXFileHandle& ixFileHandle,const Attribute& attribute, std::vector<int>& trace,
                                   Field& border_key,int new_leaf_page_num, int level_of_parent){
    // #TODO: passed key may have a memory leak
    if(DEBUG) std::cout << "inserting key to parent" << std::endl;
    if(DEBUG) std::cout << "trace size: " << trace.size() << std::endl;
    if(DEBUG) std::cout << "level: " << level_of_parent << std::endl;
    if((int)trace.size()-1-level_of_parent < 0){
        if(DEBUG) std::cout << "inserting key to parent option 1" << std::endl;
        // gonna need a new top root. change root index.
        //if(level_of_parent == 1){
        if(true){
            // there was no prev top root, root was the same leaf.
            if(DEBUG) std::cout << "inserting key to parent bp0" << std::endl;
            int new_parent_node_num = ixFileHandle.getNumberOfPages();
            Node new_parent_node;
            new_parent_node.entry_num = 0;
            new_parent_node.next_page = -99;
            new_parent_node.label = 0;
            new_parent_node.key_type = attribute.type;
            Entry entry;
            entry.key.attr = border_key.attr;
            switch (entry.key.attr.type){
                case TypeInt:
                    entry.key.int_val = border_key.int_val;
                    entry.total_size += sizeof(int);
                    break;
                case TypeReal:
                    entry.key.real_val = border_key.real_val;
                    entry.total_size += sizeof(float);
                    break;
                case TypeVarChar:
                    entry.key.varchar_val = (char*)malloc(border_key.attr.length);
                    memcpy(entry.key.varchar_val, border_key.varchar_val, border_key.attr.length);
                    entry.total_size += sizeof(int) + border_key.attr.length;
                    break;
            }
            entry.rid_num = 0;
            entry.updateBuffer();
            new_parent_node.entrys.push_back(entry);
            new_parent_node.entry_num += 1;
            new_parent_node.page_ptrs.push_back(trace[trace.size()-1-level_of_parent+1]);
            new_parent_node.page_ptrs.push_back(new_leaf_page_num);
            EntryInfo ei;
            ei.offset = 0;
            ei.length = entry.total_size;
            new_parent_node.infos.push_back(ei);
            if(DEBUG) std::cout << "inserting key to parent bp1" << std::endl;
            new_parent_node.updateBuffer();
            if(DEBUG) std::cout << "inserting key to parent bp2" << std::endl;
            rootIndex = new_parent_node_num;
            ixFileHandle.appendPage(new_parent_node.buffer);
            new_parent_node.freeMalloc();
            //entry.freeMalloc();
            if(DEBUG) std::cout << "inserting key to parent done" << std::endl;
            return 0;
        }else{
            // gonna change split a root and make new top root.
            // not sure if i need this part.
        }
    } else{
        if(DEBUG) std::cout << "inserting key to parent option 2" << std::endl;
        //root exist, insert new leaf page num into parent root, see if need another root split.
        int parent_page_num = trace[trace.size()-1-level_of_parent];
        char* buffer = (char*)malloc(PAGE_SIZE);
        ixFileHandle.readPage(parent_page_num, buffer);
        Node node(buffer, attribute);

        Entry entry;
        entry.key.attr = border_key.attr;
        switch (entry.key.attr.type){
            case TypeInt:
                entry.key.int_val = border_key.int_val;
                entry.total_size += sizeof(int);
                break;
            case TypeReal:
                entry.key.real_val = border_key.real_val;
                entry.total_size += sizeof(float);
                break;
            case TypeVarChar:
                entry.key.varchar_val = (char*)malloc(border_key.attr.length);
                memcpy(entry.key.varchar_val, border_key.varchar_val, border_key.attr.length);
                entry.total_size += sizeof(int) + border_key.attr.length;
                break;
        }
        entry.rid_num = 0;
        entry.updateBuffer();


        int i = 0, flag = 0, index = 0;
        for(; i< node.entry_num; i++){
            switch (node.key_type){
                case TypeInt:
                    if(entry.key.int_val < node.entrys[i].key.int_val){
                        flag = 1;
                        index = i;
                        break;
                    }
                    break;
                case TypeReal:
                    if(entry.key.real_val< node.entrys[i].key.real_val){
                        flag = 1;
                        index = i;
                        break;
                    }
                    break;
                case TypeVarChar:
                    int length = entry.key.attr.length;
                    //std::cout << length << " " << node.entrys[i].key.attr.length << std::endl;
                    if(length < node.entrys[i].key.attr.length){
                        flag = 1;
                        index = i;
                        break;
                    }
                    if(length == node.entrys[i].key.attr.length) {
                        if (memcmp( entry.key.varchar_val, node.entrys[i].key.varchar_val, length) < 0) {
                            flag = 1;
                            index = i;
                            break;
                        }
                    }
                    break;
            }
            if(flag==1){
                break;
            }
        }
        if(flag == 0){
            node.entrys.push_back(entry);
            node.entry_num += 1;
            node.page_ptrs.push_back(new_leaf_page_num);
            EntryInfo ei;
            ei.offset = node.infos[node.entry_num-2].offset+node.infos[node.entry_num-2].length;
            ei.length = entry.total_size;
            node.infos.push_back(ei);
        } else{
            node.entrys.insert(node.entrys.begin()+index, entry);
            node.entry_num += 1;
            node.page_ptrs.insert(node.page_ptrs.begin()+index+1,new_leaf_page_num);

            EntryInfo ei;
            ei.offset = node.infos[index].offset;
            ei.length = entry.total_size;
            node.infos.insert(node.infos.begin()+index, ei);
            for(int k = index+1; k< node.infos.size(); k++){
                node.infos[k].offset += ei.length;
            }
        }

        if(node.free_byte >= entry.total_size + sizeof(EntryInfo)){
        //if(node.entry_num < 7){ //#TODO: delete this line back when done
            node.updateBuffer();
            ixFileHandle.writePage(parent_page_num,node.buffer);
            node.freeMalloc();
            //entry.freeMalloc();
            return 0;
        } else{
            if(DEBUG) std::cout << "split node and insert to parent";

            splitRootNode(ixFileHandle,attribute,trace,node, level_of_parent);

            return 0;
        }
    }
}

RC IndexManager::splitRootNode(IXFileHandle& ixFileHandle, const Attribute& attribute, std::vector<int>& trace, Node& node, int level){
    if(DEBUG) std::cout << "Leaf Node split called." << std::endl;
    int half_length = node.entry_num/2;
    Field up_key;
    up_key.attr.length = node.entrys[half_length].key.attr.length;
    up_key.attr.type = node.entrys[half_length].key.attr.type;
    up_key.attr.name = node.entrys[half_length].key.attr.name;
    switch (up_key.attr.type){
        case TypeInt:
            up_key.int_val = node.entrys[half_length].key.int_val;
            break;
        case TypeReal:
            up_key.real_val = node.entrys[half_length].key.real_val;
            break;
        case TypeVarChar:
            up_key.varchar_val = (char*)malloc(up_key.attr.length);
            memcpy(up_key.varchar_val, node.entrys[half_length].key.varchar_val, up_key.attr.length);
            break;
        default:
            break;
    }

    Node new_root_node;
    new_root_node.entry_num = 0;
    new_root_node.next_page = -99;
    new_root_node.label = 0;
    new_root_node.key_type = node.key_type;
    int new_root_page_num = ixFileHandle.getNumberOfPages();
    int i = half_length + 1, new_count = 0, new_offset = 0;
    if(DEBUG) std::cout << "half_length: " << half_length << std::endl;
    for(;i<node.entry_num;i++){
        new_root_node.entrys.push_back(node.entrys[i-new_count]);
        node.entrys.erase(node.entrys.begin()+i-new_count);
        new_root_node.page_ptrs.push_back(node.page_ptrs[i-new_count]);
        node.page_ptrs.erase(node.page_ptrs.begin()+i-new_count);

        EntryInfo ei;
        ei.length = node.infos[i-new_count].length;
        ei.offset = new_offset;
        new_root_node.infos.push_back(ei);
        node.infos.erase(node.infos.begin()+i-new_count);
        new_offset += ei.length;
        new_count += 1;
        new_root_node.entry_num += 1;
    }
    //inserting last page ptr.
    new_root_node.page_ptrs.push_back(node.page_ptrs[i-new_count]);
    node.page_ptrs.erase(node.page_ptrs.begin()+i-new_count);

    // remove key at half_length index and move it to upper level root.
    //node.entrys[half_length].freeMalloc();
    node.entrys.erase(node.entrys.begin()+half_length);

    node.entry_num -= new_root_node.entry_num + 1;

    if(node.entrys.size() != node.entry_num){
        std::cerr << "node split wrong -5";
        return -5;
    }
    if(node.entrys.size() != node.page_ptrs.size()-1){
        std::cerr << "node split wrong -6";
        return -6;
    }
    if(new_root_node.entrys.size() != new_root_node.entry_num){
        std::cerr << "node split wrong -7";
        return -7;
    }
    if(new_root_node.entrys.size() != new_root_node.page_ptrs.size()-1){
        std::cerr << "node split wrong -8";
        return -8;
    }

    if(DEBUG) std::cout << "split loop done, write next" << std::endl;
    node.updateBuffer();
    if(DEBUG) std::cout << "split node bp1" << std::endl;
    new_root_node.updateBuffer();
    if(DEBUG) std::cout << "split node bp2" << std::endl;
    ixFileHandle.writePage(trace[trace.size()-1-level], node.buffer);
    //ixFileHandle.writePage(new_root_page_num, new_root_node.buffer);
    ixFileHandle.appendPage(new_root_node.buffer);

    //leaf node done. next, go back along trace and change roots
    InsertKeyToParent(ixFileHandle, attribute, trace, up_key, new_root_page_num, level+1);

}


RC IndexManager::DFS(IXFileHandle& ixFileHandle, const Attribute& attribute, int page_num, int level, int last_flag) const{
    char* buffer = (char*)malloc(PAGE_SIZE);
    ixFileHandle.readPage(page_num, buffer);
    Node node(buffer, attribute);

    if(node.label == 0) {
        printTab(level);
        std::cout << "{";
        if (level == 0)std::cout << std::endl;
        std::cout << "\"keys\":[";
        for (int i = 0; i < node.entry_num; i++) {
            std::cout << "\"";
            switch (node.entrys[i].key.attr.type) {
                case TypeInt:
                    std::cout << node.entrys[i].key.int_val;
                    break;
                case TypeReal:
                    std::cout << node.entrys[i].key.real_val;
                    break;
                case TypeVarChar:
                    for (int j = 0; j < node.entrys[i].key.attr.length; j++) {
                        std::cout << *(node.entrys[i].key.varchar_val + j);
                    }
                    break;
            }
            std::cout << "\"";
            if (i != node.entry_num - 1) std::cout << ",";
        }
        std::cout << "]," << std::endl;
        printTab(level);
        if (level == 0) {
            std::cout << "\"children\":[" << std::endl;
        } else {
            std::cout << " \"children\":[" << std::endl;
        }

        for(int i=0; i<node.page_ptrs.size(); i++){
            if(i!=node.page_ptrs.size()-1) DFS(ixFileHandle, attribute, node.page_ptrs[i], level+1, 0);
            else DFS(ixFileHandle, attribute, node.page_ptrs[i], level+1, 1);
        }

        if (level == 0) {
            std::cout << "]" << std::endl << "}" << std::endl;
        } else {
            printTab(level);
            std::cout << "]}";
            if(last_flag==0) std::cout << ",";
            std::cout << std::endl;
        }
        return 0;
    }

    else{
        printLeafNode(ixFileHandle, attribute, node, level);
        if(last_flag==0) std::cout << ",";
        std::cout <<std::endl;
        return 0;
    }

}

RC IndexManager::printLeafNode(IXFileHandle& ixFileHandle, const Attribute& attribute, Node node, int level) const{
    printTab(level);
    std::cout << "{";
    if(level==0)std::cout << std::endl;
    std::cout << "\"keys\": [";
    for(int i=0; i<node.entry_num; i++){
        std::cout << "\"";
        switch(node.entrys[i].key.attr.type){
            case TypeInt:
                std::cout << node.entrys[i].key.int_val;
                break;
            case TypeReal:
                std::cout << node.entrys[i].key.real_val;
                break;
            case TypeVarChar:
                    for(int j=0;j<node.entrys[i].key.attr.length;j++) {
                        std::cout << *(node.entrys[i].key.varchar_val+j);
                    }
                break;
        }
        std::cout << ":";
        printRids(node.entrys[i].rids);
        std::cout << "\"";
        if(i!=node.entry_num-1) std::cout << ",";
    }
    std::cout << "]";
    if(level==0) std::cout << std::endl;
    std::cout << "}";
    return 0;
}

RC IndexManager::printTab(int level) const{
    for(int i=0; i< level; i++){
        std::cout<< "    ";
    }
    return 0;
}

RC IndexManager::printRids(std::vector<RID> rids) const{
    std::cout<< "[";
    for(int i=0;i<rids.size();i++){
        printRid(rids[i]);
        if(i!=rids.size()-1) std::cout<< ",";
    }
    std::cout<< "]";
    return 0;
}

RC IndexManager::printRid(RID rid) const{
    std::cout<< "(" << rid.pageNum << "," << rid.slotNum << ")";
    return 0;
}