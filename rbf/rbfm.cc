#include "rbfm.h"

//#TODO: modify tombstone tech if possible.

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;
PagedFileManager& RecordBasedFileManager::_pf_manager = PagedFileManager::instance();

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    if(_pf_manager.createFile(fileName) != 0) return -1;
    num_of_pageIndex = 0;
    cur_pageIndex = -1;
    return 0;
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return _pf_manager.destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    if(_pf_manager.openFile(fileName, fileHandle) != 0 ) return -1;
    // check to get num of page since sharing same rbfm
    if(fileHandle.getNumberOfPages() == 0){
        if(DEBUG) std::cout << "first pi added" << std::endl;
        PageIndex pi;
        if(pi.updateBuffer()!=0){
            std::cerr << "Updating page index buffer failed." << std::endl;
            return -1;
        }
        num_of_pageIndex = 1;
        cur_pageIndex = 0;
        fileHandle.appendPage(pi.buffer);
    }
    else{
        cur_pageIndex = 0;
        num_of_pageIndex = 0;
        int tmp = 0, count = 0;
        char* tmp_page = (char*)malloc(PAGE_SIZE);
        while(1){
            count++;
            fileHandle.readPage(tmp, tmp_page);
            PageIndex pi(tmp_page);
            if(pi.next == -1) break;
            tmp = pi.next;
            pi.freeMalloc();
        }
        free(tmp_page);
        tmp_page = NULL;
        num_of_pageIndex = count;
    }
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager.closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    if(DEBUG) std::cout << "inserting"<< std::endl;
    Record record(recordDescriptor, data);
    record.updateBuffer();
    char* tmp_page = (char*)malloc(PAGE_SIZE);
    if(DEBUG) std::cout << "current page index: " << cur_pageIndex << std::endl;
    fileHandle.readPage(cur_pageIndex, tmp_page);
    PageIndex pi(tmp_page);
    if(DEBUG) std::cout << "pi num: " << pi.num << std::endl;
    if(DEBUG) std::cout << "pi next: " << pi.next << std::endl;
    if(DEBUG) std::cout << "pi info size: " << pi.infos.size() << std::endl;
    free(tmp_page);
    tmp_page = NULL;
    int num_page = fileHandle.getNumberOfPages();   // including the hidden page.
    if(DEBUG) std::cout << "pageindexinfo num: " << pi.num<< std::endl;
    if(pi.num == ((PAGE_SIZE-2* sizeof(int))/ sizeof(PageIndexInfo))){    //check if pageindex page is full.
        if(DEBUG) std::cout << "creating new page index " << pi.num<< std::endl;
        pi.next = num_page;
        //pi.updateBuffer();
        memcpy(pi.buffer + sizeof(int), &pi.next, sizeof(int));
        fileHandle.writePage(cur_pageIndex, pi.buffer);
        addNewPageIndex(fileHandle, num_page);
    }
    if(num_page == num_of_pageIndex){     // new file only with hidden page and a page-index page.
        if(DEBUG) std::cout << "---new file option---" << std::endl;
        insertInNewPage(fileHandle, record, pi, rid, num_page);
        if(DEBUG) std::cout << "insertion done" << std::endl;
        if(DEBUG) std::cout << "data size: "<< record.total_size << std::endl;
        if(DEBUG) std::cout << "rid: "<< rid.pageNum << " " << rid.slotNum << std::endl;
        return 0;
    }
    int avail_page_num = -1;
    int found_pageIndex_index = findAvailPage(fileHandle,pi,record,avail_page_num);
    if( found_pageIndex_index != -1){
        // insert to avail_page_num. rewrite page and pageindex.
        if(DEBUG) std::cout << "---page found option---" << std::endl;
        if(DEBUG) std::cout << "avail page: " << avail_page_num << " found_index: " << found_pageIndex_index << std::endl;
        insertToExistingPage(fileHandle,recordDescriptor, avail_page_num,record, pi, found_pageIndex_index, rid);
        if(DEBUG) std::cout << "insertion done" << std::endl;
        if(DEBUG) std::cout << "data size: "<< record.total_size << std::endl;
        if(DEBUG) std::cout << "rid: "<< rid.pageNum << " " << rid.slotNum << std::endl;
        return 0;
    }else{
        // insert to a new page. write new page and rewrite pageindex.
        if(DEBUG) std::cout << "---page not found option---" << std::endl;
        insertInNewPage(fileHandle,record,pi,rid,num_page);
        if(DEBUG) std::cout << "insertion done" << std::endl;
        if(DEBUG) std::cout << "data size: "<< record.total_size << std::endl;
        if(DEBUG) std::cout << "rid: "<< rid.pageNum << " " << rid.slotNum << std::endl;
        return 0;
    }
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    if(DEBUG) std::cout << "reading"<< std::endl;
    char* buffer = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    Page page(buffer, recordDescriptor);
    if(DEBUG) std::cout << "page infos size: " << page.infos.size() << std::endl;
    RecordInfo ri;
    memcpy(&ri, &page.infos[rid.slotNum], sizeof(RecordInfo));
    if(ri.offset == -1) return -1;
    if(ri.offset < -1){
        RID rid_point_to;
        rid_point_to.pageNum = ri.offset + 10000000;
        rid_point_to.slotNum = ri.length;
        return readRecord(fileHandle, recordDescriptor, rid_point_to, data);
    }
    Record record(recordDescriptor, buffer, ri);
    record.getData(data);
    free(buffer);
    buffer = NULL;
    page.freeMalloc();
    record.freeMalloc();
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    //change page buffer and rewrite
    if(DEBUG) std::cout << "deleting record"<< std::endl;
    char* buffer = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    Page page(buffer, recordDescriptor);

    if(page.infos[rid.slotNum].offset<-1){
        RID rid_point_to;
        rid_point_to.pageNum = page.infos[rid.slotNum].offset + 10000000;
        rid_point_to.slotNum = page.infos[rid.slotNum].length;
        return deleteRecord(fileHandle, recordDescriptor, rid_point_to);
    }
    int data_length = page.infos[rid.slotNum].length;
    for(int i=rid.slotNum+1;i<page.infos.size();i++){
        if(page.infos[i].offset <= -1) continue;
        page.infos[i].offset -= data_length;
    }
    page.infos[rid.slotNum].length = -1;
    page.infos[rid.slotNum].offset = -1;
    page.updateBuffer();
    if(DEBUG) std::cout << "after deletion: " << rid.pageNum << " " << page.infos.size()<< " " <<page.num_record << std::endl;
    fileHandle.writePage(rid.pageNum, page.buffer);
    page.freeMalloc();

    //change free byte in page index
    RID rid_copy;
    rid_copy.slotNum = rid.slotNum;
    rid_copy.pageNum = rid.pageNum;
    if(DEBUG) std::cout << "record deleted: " << rid.pageNum << " " << rid.slotNum << std::endl;
    return updatePageIndex(fileHandle, rid_copy, data_length);
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    int null_size = ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char* null_val = (unsigned char*) malloc(sizeof(null_size));
    memcpy(null_val, data, null_size);
    if(null_size == 0 or null_val == NULL){
        std::cerr << "Null value cannot be read." << std::endl;
        return -1;
    }
    int offset = null_size;
    for(int i=0; i<recordDescriptor.size(); i++){
        int null_count_in_bit = i%8;
        int null_count_in_byte = i/8;
        if(!(null_val[null_count_in_byte] & (unsigned)1 << (unsigned)(7-null_count_in_bit))){   // check if corresponding bit is 1.
            switch (recordDescriptor[i].type){
                case TypeInt:{
                    int output;
                    memcpy(&output, (char*)data+offset, recordDescriptor[i].length);
                    std::cout << recordDescriptor[i].name << ": " << output << std::endl;
                    offset += recordDescriptor[i].length;
                    break;
                }
                case TypeReal:{
                    float output;
                    memcpy(&output, (char*)data+offset, recordDescriptor[i].length);
                    std::cout << recordDescriptor[i].name << ": " << output << std::endl;
                    offset += recordDescriptor[i].length;
                    break;
                }
                case TypeVarChar:{
                    int length;
                    memcpy(&length, (char*)data+offset, sizeof(int));
                    offset += 4* sizeof(char);
                    char* output = (char*)malloc(length);
                    memcpy(output, (char*)data+offset, length);
                    std::cout << recordDescriptor[i].name << ": ";
                    for(int j=0; j< length; j++){
                        std::cout << *(output+j);
                    }
                    std::cout << std::endl;
                    offset += length;
                    free(output);
                    break;
                }
                default:{
                    std::cerr << "Date type wrong. it should be int/real/varchar" << std::endl;
                    return -1;
                }
            }
        }
    }
    free(null_val);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    if(DEBUG) std::cout << "updating record"<< std::endl;
    char* buffer = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    Page page(buffer, recordDescriptor);
    int old_data_length = page.infos[rid.slotNum].length;
    Record record(recordDescriptor, data);
    record.updateBuffer();
    int new_data_length = record.total_size;

    if(page.infos[rid.slotNum].offset < -1){
        RID rid_point_to;
        rid_point_to.pageNum = page.infos[rid.slotNum].offset + 10000000;
        rid_point_to.slotNum = page.infos[rid.slotNum].length;
        updateRecord(fileHandle, recordDescriptor, data, rid_point_to);
    }

    if(old_data_length == new_data_length){
        if(DEBUG) std::cout << "new_data_length == old_data_length" << std::endl;
        page.records[rid.slotNum].freeMalloc();
        page.records[rid.slotNum] = record;

        page.updateBuffer();
        fileHandle.writePage(rid.pageNum, page.buffer);
        page.freeMalloc();
        return 0;
    }else if(old_data_length > new_data_length){
        if(DEBUG) std::cout << "new_data_length < old_data_length" << std::endl;
        page.records[rid.slotNum].freeMalloc();
        page.records[rid.slotNum] = record;
        page.infos[rid.slotNum].length = new_data_length;
        for(int i=rid.slotNum+1;i<page.infos.size();i++){
            if(page.infos[i].offset <= -1) continue;
            page.infos[i].offset -= old_data_length - new_data_length;
        }
        page.updateBuffer();
        fileHandle.writePage(rid.pageNum, page.buffer);
        page.freeMalloc();

        RID rid_copy;
        rid_copy.pageNum = rid.pageNum;
        rid_copy.slotNum = rid.slotNum;
        updatePageIndex(fileHandle,rid_copy,old_data_length-new_data_length);

        return 0;
    }else{
        int free_byte = 0;
        int flag = 0;
        int pi_page_index = 0;
        char* tmp_page = (char*)malloc(PAGE_SIZE);
        cur_pageIndex = 0;
        fileHandle.readPage(cur_pageIndex, tmp_page);
        PageIndex pi(tmp_page);
        free(tmp_page);
        tmp_page = NULL;
        while(1){
            for(int i=0; i<pi.infos.size(); i++){
                if(rid.pageNum == pi.infos[i].offset){
                    flag = 1;
                    pi_page_index = i;
                    free_byte = pi.infos[i].free_byte;
                    if(tmp_page!=NULL){
                        free(tmp_page);
                        tmp_page = NULL;
                    }
                    break;
                }
            }
            if(flag) break;
            if(pi.next == -1){
                std::cerr << "could not find this page on page index." << std::endl;
                return -1;
            }
            cur_pageIndex = pi.next;
            tmp_page = (char*)malloc(PAGE_SIZE);
            fileHandle.readPage(cur_pageIndex, tmp_page);
            PageIndex pi_new(tmp_page);
            pi.freeMalloc();
            pi = pi_new;
            //pi_new.buffer = NULL;
        }
        if(DEBUG) std::cout << "free byte: " << free_byte << std::endl;
        if(DEBUG) std::cout << "new size: " << new_data_length << std::endl;
        if(DEBUG) std::cout << "old size: " << old_data_length << std::endl;
        if(new_data_length-old_data_length<free_byte){
            if(DEBUG) std::cout << "new_data_length-old_data_length<free_byte" << std::endl;
            page.records[rid.slotNum].freeMalloc();
            page.records[rid.slotNum] = record;
            page.infos[rid.slotNum].length = new_data_length;
            for(int i=rid.slotNum+1;i<page.infos.size();i++){
                if(page.infos[i].offset <= -1) continue;
                page.infos[i].offset += new_data_length - old_data_length;
            }
            page.updateBuffer();
            fileHandle.writePage(rid.pageNum, page.buffer);
            page.freeMalloc();
            pi.infos[pi_page_index].free_byte += old_data_length - new_data_length;
            pi.updateBuffer();
            fileHandle.writePage(cur_pageIndex, pi.buffer);
            pi.freeMalloc();
            return 0;
        }
        else{
            //gonna need to insert in a new page and leave tombstone.
            if(DEBUG) std::cout << "tombstone option" << std::endl;
            RID rid_new;
            insertRecord(fileHandle,recordDescriptor, data,rid_new);
            if(DEBUG) std::cout << "tombstone option bp1" << std::endl;
            deleteRecordWithPage(fileHandle,recordDescriptor,rid,page);
            if(DEBUG) std::cout << "tombstone option bp2" << std::endl;
            page.infos[rid.slotNum].offset = rid_new.pageNum - 10000000;
            page.infos[rid.slotNum].length = rid_new.slotNum;
            page.updateBuffer();
            fileHandle.writePage(rid.pageNum, page.buffer);
            page.freeMalloc();

            return 0;
        }
    }
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {

    if(DEBUG) std::cout << "reading attribute"<< std::endl;
    char* buffer = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    Page page(buffer, recordDescriptor);
    RecordInfo ri;
    memcpy(&ri, &page.infos[rid.slotNum], sizeof(RecordInfo));
    if(ri.offset == -1) return -1;
    if(ri.offset < -1){
        RID rid_point_to;
        rid_point_to.pageNum = ri.offset + 10000000;
        rid_point_to.slotNum = ri.length;
        return readRecord(fileHandle, recordDescriptor, rid_point_to, data);
    }
    Record record(recordDescriptor, buffer, ri);

    for(int i=0; i<record.fields.size(); i++) {
        if(DEBUG) std::cout << record.fields[i].attr.name << attributeName << std::endl;
        if(record.fields[i].attr.name != attributeName) continue;
        switch (record.fields[i].attr.type) {
            case TypeInt: {
                memcpy((char*)data, &record.fields[i].int_val, sizeof(int));
                break;
            }
            case TypeReal: {
                memcpy((char*)data, &record.fields[i].real_val, sizeof(float));
                break;
            }
            case TypeVarChar: {
                memcpy((char*)data, &record.fields[i].attr.length, sizeof(int));
                memcpy((char*)data+sizeof(int), record.fields[i].varchar_val, record.fields[i].attr.length);
                break;
            }
            default: {
                std::cerr << "Date type wrong. it should be int/real/varchar" << std::endl;
            }
        }
        break;
    }
    record.freeMalloc();
    free(buffer);
    buffer = NULL;
    return 0;

}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    cur_pageIndex = 0;
    char* tmp_page = (char*)malloc(PAGE_SIZE);
    int done = 1, flag = 1;
    RID rid;
    rbfm_ScanIterator.clear();
    rbfm_ScanIterator.setAttrsNeed(attributeNames);
    rbfm_ScanIterator.setFileHandle(fileHandle);
    rbfm_ScanIterator.setRecordDescriptor(recordDescriptor);
    while(done == 1) {
        fileHandle.readPage(cur_pageIndex, tmp_page);
        PageIndex pi(tmp_page);
        for(int i=0; i<pi.infos.size(); i++) {
            if(pi.infos[i].free_byte == PAGE_SIZE) continue;
            fileHandle.readPage(pi.infos[i].offset, tmp_page);
            Page page(tmp_page,recordDescriptor);
            for(int j=0; j<page.records.size();j++){
                for(int k=0; k<page.records[j].attrs.size();k++){
                    if(page.records[j].attrs[k].name == conditionAttribute or compOp == NO_OP){
                        switch (compOp){
                            case EQ_OP:
                                switch (page.records[j].attrs[k].type){
                                    case TypeInt:
                                        if(page.records[j].fields[k].int_val == *(int*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeReal:
                                        if(page.records[j].fields[k].real_val == *(float*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeVarChar:
                                        int length;
                                        memcpy(&length, value, sizeof(int));
                                        if(length!=page.records[j].fields[k].attr.length) break;
                                        if(memcmp(page.records[j].fields[k].varchar_val, (char*) value+ sizeof(int), length) == 0){
                                            flag = 0;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                                break;
                            case LT_OP:
                                switch (page.records[j].attrs[k].type){
                                    case TypeInt:
                                        if(page.records[j].fields[k].int_val < *(int*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeReal:
                                        if(page.records[j].fields[k].real_val < *(float*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeVarChar:
                                        int length;
                                        memcpy(&length, value, sizeof(int));
                                        if(memcmp(page.records[j].fields[k].varchar_val, (char*)value+ sizeof(int), length) < 0){
                                            flag = 0;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                                break;
                            case LE_OP:
                                switch (page.records[j].attrs[k].type){
                                    case TypeInt:
                                        if(page.records[j].fields[k].int_val <= *(int*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeReal:
                                        if(page.records[j].fields[k].real_val <= *(float*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeVarChar:
                                        int length;
                                        memcpy(&length, value, sizeof(int));
                                        if(memcmp(page.records[j].fields[k].varchar_val, (char*)value+ sizeof(int), length) <= 0){
                                            flag = 0;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                                break;
                            case GT_OP:
                                switch (page.records[j].attrs[k].type){
                                    case TypeInt:
                                        if(page.records[j].fields[k].int_val > *(int*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeReal:
                                        if(page.records[j].fields[k].real_val > *(float*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeVarChar:
                                        int length;
                                        memcpy(&length, value, sizeof(int));
                                        if(memcmp(page.records[j].fields[k].varchar_val, (char*)value+ sizeof(int), length) > 0){
                                            flag = 0;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                                break;
                            case GE_OP:
                                switch (page.records[j].attrs[k].type){
                                    case TypeInt:
                                        if(page.records[j].fields[k].int_val >= *(int*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeReal:
                                        if(page.records[j].fields[k].real_val >= *(float*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeVarChar:
                                        int length;
                                        memcpy(&length, value, sizeof(int));
                                        if(memcmp(page.records[j].fields[k].varchar_val, (char*)value+ sizeof(int), length) >= 0){
                                            flag = 0;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                                break;
                            case NE_OP:
                                switch (page.records[j].attrs[k].type){
                                    case TypeInt:
                                        if(page.records[j].fields[k].int_val != *(int*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeReal:
                                        if(page.records[j].fields[k].real_val != *(float*)value){
                                            flag = 0;
                                        }
                                        break;
                                    case TypeVarChar:
                                        int length;
                                        memcpy(&length, value, sizeof(int));
                                        if(memcmp(page.records[j].fields[k].varchar_val, (char*)value+ sizeof(int), length) != 0){
                                            flag = 0;
                                        }
                                        break;
                                    default:
                                        break;
                                }
                                break;
                            case NO_OP:
                                flag = 0;
                                break;
                            default:
                                std::cerr << "should never enter this section" <<std::endl;
                                break;
                        }
                        if(flag==0){
                            rid.pageNum = pi.infos[i].offset;
                            rid.slotNum = j;
                            rbfm_ScanIterator.rids.push_back(rid);
                            flag = 1;
                        }
                        break;
                    }
                }
            }
            page.freeMalloc();
        }
        if(pi.next == -1){
            done = 0;
        }else{
            cur_pageIndex = pi.next;
        }
        pi.freeMalloc();
    }
    free(tmp_page);
    tmp_page = NULL;

    return 0;
}

RC Record::updateBuffer() {
    buffer = (char*)malloc(total_size);
    /*if(DEBUG and DEBUGUNIT) std::cout << "Record::updateBuffer bp1" << std::endl;
    if(DEBUG and DEBUGUNIT) std::cout << total_size << std::endl;
    if(DEBUG and DEBUGUNIT) std::cout << null_size << std::endl;
    if(DEBUG and DEBUGUNIT) std::cout << (null_val==NULL) << std::endl;
    if(DEBUG and DEBUGUNIT) std::cout << (buffer==NULL) << std::endl;*/
    memcpy(buffer, null_val, null_size);
    //if(DEBUG and DEBUGUNIT) std::cout << "Record::updateBuffer bp2" << std::endl;
    int offset = null_size;
    int null_count = 0;
    for(int i=0; i<attrs.size(); i++){
        int null_count_in_bit = i%8;
        int null_count_in_byte = i/8;
        if(!(null_val[null_count_in_byte] & (unsigned)1 << (unsigned)(7-null_count_in_bit))){   // check if corresponding bit is 1.
            switch (fields[i-null_count].attr.type){
                case TypeInt:{
                    //if(DEBUG and DEBUGUNIT) std::cout << "Record::updateBuffer switch int" << std::endl;
                    memcpy(buffer+offset, &(fields[i-null_count].int_val), fields[i-null_count].attr.length);
                   // memcpy(buffer+null_size+4*i, &offset, sizeof(int));
                    offset += fields[i-null_count].attr.length;
                    break;
                }
                case TypeReal:{
                    //if(DEBUG and DEBUGUNIT) std::cout << "Record::updateBuffer switch real" << std::endl;
                    memcpy(buffer+offset, &(fields[i-null_count].real_val), fields[i-null_count].attr.length);
                    //memcpy(buffer+null_size+4*i, &offset, sizeof(int));
                    offset += fields[i-null_count].attr.length;
                    break;
                }
                case TypeVarChar:{
                    //if(DEBUG and DEBUGUNIT) std::cout << "Record::updateBuffer switch varchar" << std::endl;
                    memcpy(buffer+offset, &fields[i-null_count].attr.length, sizeof(int));
                    offset += sizeof(int);
                    memcpy(buffer+offset, fields[i-null_count].varchar_val, fields[i-null_count].attr.length);
                    //memcpy(buffer+null_size+4*i, &offset, sizeof(int));
                    offset += fields[i-null_count].attr.length;
                    break;
                }
                default:{
                    std::cerr << "Date type wrong. it should be int/real/varchar" << std::endl;
                }
            }
        }
        else{
            offset += attrs[i].length;
            null_count += 1;
        }
    }
    return 0;
}

RC Record::getData(void *data_out) {
    memcpy((char*)data_out, null_val, null_size);
    int offset = null_size;
    for(int i=0; i<fields.size(); i++) {
        switch (fields[i].attr.type) {
            case TypeInt: {
                memcpy((char*)data_out+offset, &fields[i].int_val, sizeof(int));
                offset += sizeof(int);
                break;
            }
            case TypeReal: {
                memcpy((char*)data_out+offset, &fields[i].real_val, sizeof(float));
                offset += sizeof(float);
                break;
            }
            case TypeVarChar: {
                memcpy((char*)data_out+offset, &fields[i].attr.length, sizeof(int));
                offset += sizeof(int);
                memcpy((char*)data_out+offset, fields[i].varchar_val, fields[i].attr.length);
                offset += fields[i].attr.length;
                break;
            }
            default: {
                std::cerr << "Date type wrong. it should be int/real/varchar" << std::endl;
            }
        }
    }
    return 0;
}

RC PageIndex::updateBuffer(){
    memcpy(buffer, &num, sizeof(int));
    memcpy(buffer + sizeof(int), &next, sizeof(int));
    int offset = sizeof(int)*2;
    for(int i = 0; i < infos.size(); i++){
        memcpy(buffer + offset, &infos[i], sizeof(PageIndexInfo));
        offset += sizeof(PageIndexInfo);
    }
    return 0;
}

RC Page::updateBuffer() {
    int offset = PAGE_SIZE - 2* sizeof(int);
    int empty_count = 0;
    //std::cout << "Page::updateBuffer info size: " << infos.size() << std::endl;
    //std::cout << "Page::updateBuffer record size: " << records.size() << std::endl;
    for(int i=0; i<infos.size(); i++){
        //std::cout << "Page::updateBuffer info offset: "<< infos[i].offset << std::endl;
        if(infos[i].offset <= -1){
            //empty_count += 1;
        }
        else{
            records[i].updateBuffer();
            //std::cout << "Page::updateBuffer record size: " << records[i].total_size << std::endl;
            memcpy(buffer + infos[i].offset, records[i].buffer, records[i].total_size);
            free_offset = infos[i].offset + records[i].total_size;
        }
        offset -= sizeof(RecordInfo);
        memcpy(buffer+offset, &infos[i], sizeof(RecordInfo));
    }
    num_record = infos.size()-empty_count;
    memcpy(buffer+PAGE_SIZE- sizeof(int), &free_offset, sizeof(int));
    memcpy(buffer+PAGE_SIZE- 2*sizeof(int), &num_record, sizeof(int));
    //std::cout << "Page::updateBuffer done" << std::endl;
    return 0;
}

RC RecordBasedFileManager::insertInNewPage(FileHandle& fileHandle, Record& record, PageIndex& pi, RID& rid, int& num_page){
    Page page;
    page.records.push_back(record);
    /*record.buffer = NULL;
    record.null_val = NULL;
    record.fields.clear();*/
    rid.pageNum = num_page;
    rid.slotNum = page.infos.size();
    RecordInfo ri;
    ri.offset = 0;
    ri.length = record.total_size;
    page.infos.push_back(ri);
    page.updateBuffer();
    if(DEBUG and DEBUGUNIT) std::cout << "RecordBasedFileManager::insertInNewPage bp0" << std::endl;
    fileHandle.appendPage(page.buffer);
    num_page += 1;
    PageIndexInfo pii;
    pii.free_byte = PAGE_SIZE - ri.length - sizeof(RecordInfo) - 2*sizeof(int);
    // pii.offset = 1;
    pii.offset = num_page-1;
    pi.infos.push_back(pii);
    // pi.num = 1;
    pi.num = pi.infos.size();
    if(DEBUG and DEBUGUNIT) std::cout << "RecordBasedFileManager::insertInNewPage bp1" << std::endl;
    pi.updateBuffer();
    if(DEBUG and DEBUGUNIT) std::cout << "RecordBasedFileManager::insertInNewPage bp2" << std::endl;
    fileHandle.writePage(cur_pageIndex, pi.buffer);
    return 0;
}

RC RecordBasedFileManager::addNewPageIndex(FileHandle& fileHandle, int &num_page) {
    PageIndex pi;
    pi.updateBuffer();
    fileHandle.appendPage(pi.buffer);
    num_of_pageIndex += 1;
    if(DEBUG and DEBUGUNIT) std::cout << "added new index page. total number: " << num_of_pageIndex << std::endl;
    //cur_pageIndex = num_page;
    num_page += 1;
    return 0;
}

RC RecordBasedFileManager::findAvailPage(FileHandle& fileHandle, PageIndex& pi, Record& record, int& avail_page_num) {
    int space_needed = record.total_size + sizeof(RecordInfo);
    char* tmp_page = NULL;
    while(1){
        for(int i=0; i<pi.infos.size(); i++){
            if(space_needed < pi.infos[i].free_byte){
                avail_page_num = pi.infos[i].offset;
                if(tmp_page!=NULL){
                    free(tmp_page);
                    tmp_page = NULL;
                }
                return i;
            }
        }
        if(pi.next == -1) return -1;
        cur_pageIndex = pi.next;
        tmp_page = (char*)malloc(PAGE_SIZE);
        fileHandle.readPage(cur_pageIndex, tmp_page);
        PageIndex pi_new(tmp_page);
        pi = pi_new;
        //pi_new.buffer = NULL;
    }
}

RC RecordBasedFileManager::insertToExistingPage(FileHandle& fileHandle, const std::vector<Attribute> &recordDescriptor,
        int avail_page_num, Record& record, PageIndex& pi, int found_pageIndex_index, RID& rid) {
    char* tmp_page = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(avail_page_num, tmp_page);
    Page page(tmp_page, recordDescriptor);
    int flag = 0;
    int i = 0;
    for(; i<page.infos.size(); i++){
        if(page.infos[i].offset == -1){
            rid.slotNum = i;
            page.infos[i].length = record.total_size;
            flag = 1;
            if(i!=page.infos.size()-1) {
                page.infos[i].offset = page.infos[i+1].offset;
            }else{
                page.infos[i].offset = page.infos[i-1].offset + page.infos[i-1].length;
            }
            break;
        }
    }
    if(flag) {
        if (DEBUG) std::cout << "RecordBasedFileManager::insertToExistingPage page with empty slot bp1" << std::endl;
        if (DEBUG) std::cout << page.records.size() << std::endl;
        if (DEBUG) std::cout << page.infos.size() << std::endl;
        page.records[i] = record;
        i += 1;
        if (DEBUG) std::cout << "RecordBasedFileManager::insertToExistingPage page with empty slot bp1" << std::endl;
        for (; i < page.infos.size(); i++) {
            if(page.infos[i].offset <= -1) continue;
            page.infos[i].offset += record.total_size;
        }
    }
    else{
        rid.slotNum = page.infos.size();

        RecordInfo ri;
        if(page.infos.size()==0){
            ri.offset = 0;
        } else{
            for(int j = page.infos.size()-1;j>=0 ; j--){
                if(page.infos[j].offset<0) continue;
                ri.offset = page.infos[j].offset+page.infos[j].length;
                break;
            }
        }
        ri.length = record.total_size;
        page.infos.push_back(ri);
        page.records.push_back(record);
    }
    rid.pageNum = avail_page_num;
    /*record.buffer = NULL;
    record.null_val = NULL;
    record.fields.clear();*/
    if(DEBUG) std::cout << "RecordBasedFileManager::insertToExistingPage bp1" << std::endl;
    page.updateBuffer();
    if(DEBUG) std::cout << "RecordBasedFileManager::insertToExistingPage bp2" << std::endl;
    fileHandle.writePage(avail_page_num, page.buffer);
    if(flag){
        pi.infos[found_pageIndex_index].free_byte -= record.total_size;
    } else{
        pi.infos[found_pageIndex_index].free_byte -= (sizeof(PageIndexInfo) + record.total_size);
    }
    pi.updateBuffer();
    fileHandle.writePage(cur_pageIndex, pi.buffer);

    free(tmp_page);
    tmp_page = NULL;
    page.freeMalloc();
    pi.freeMalloc();
    return 0;
}

RC RecordBasedFileManager::updatePageIndex(FileHandle &fileHandle, RID& rid, int data_length){
    char* tmp_page = (char*)malloc(PAGE_SIZE);
    cur_pageIndex = 0;
    fileHandle.readPage(cur_pageIndex, tmp_page);
    PageIndex pi(tmp_page);
    free(tmp_page);
    tmp_page = NULL;
    while(1){
        for(int i=0; i<pi.infos.size(); i++){
            if(rid.pageNum == pi.infos[i].offset){
                pi.infos[i].free_byte += data_length;
                pi.updateBuffer();
                fileHandle.writePage(cur_pageIndex, pi.buffer);
                if(tmp_page!=NULL){
                    free(tmp_page);
                    tmp_page = NULL;
                }
                pi.freeMalloc();
                return 0;
            }
        }
        if(pi.next == -1){
            std::cerr << "could not find this page on page index." << std::endl;
            return -1;
        }
        cur_pageIndex = pi.next;
        tmp_page = (char*)malloc(PAGE_SIZE);
        fileHandle.readPage(cur_pageIndex, tmp_page);
        PageIndex pi_new(tmp_page);
        pi.freeMalloc();
        pi = pi_new;
        //pi_new.buffer = NULL;
    }
}

RC Record::getSpecificData(void *data_out, const std::vector<std::string> &attributeNames) {
    int cur_null_size = getActualByteForNullsIndicator(attributeNames.size());
    memcpy((char*)data_out, null_val, cur_null_size);
    if(DEBUG) std::cout << "cur_null_size: " << cur_null_size << std::endl;
    int offset = cur_null_size;
    int flag = 0;
    for(int i=0; i<fields.size(); i++) {
        for(int j=0; j< attributeNames.size(); j++){
            if(fields[i].attr.name == attributeNames[j]){
                flag = 1;
                continue;
            }
        }
        if(flag==0){
            continue;
        }else{
            flag = 0;
        }
        // #TODO: change null bit according to attrs needed
        switch (fields[i].attr.type) {
            case TypeInt: {
                memcpy((char*)data_out+offset, &fields[i].int_val, sizeof(int));
                offset += sizeof(int);
                break;
            }
            case TypeReal: {
                memcpy((char*)data_out+offset, &fields[i].real_val, sizeof(float));
                offset += sizeof(float);
                break;
            }
            case TypeVarChar: {
                memcpy((char*)data_out+offset, &fields[i].attr.length, sizeof(int));
                offset += sizeof(int);
                memcpy((char*)data_out+offset, fields[i].varchar_val, fields[i].attr.length);
                offset += fields[i].attr.length;
                break;
            }
            default: {
                std::cerr << "Date type wrong. it should be int/real/varchar" << std::endl;
            }
        }
    }
    return 0;
}

RC RecordBasedFileManager::deleteRecordWithPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid, Page &page) {
    //change page buffer and rewrite
    if(DEBUG) std::cout << "deleting record with page"<< std::endl;

    if(page.infos[rid.slotNum].offset<-1){
        RID rid_point_to;
        rid_point_to.pageNum = page.infos[rid.slotNum].offset + 10000000;
        rid_point_to.slotNum = page.infos[rid.slotNum].length;
        return deleteRecord(fileHandle, recordDescriptor, rid_point_to);
    }
    int data_length = page.infos[rid.slotNum].length;
    for(int i=rid.slotNum+1;i<page.infos.size();i++){
        if(page.infos[i].offset <= -1) continue;
        page.infos[i].offset -= data_length;
    }
    if(DEBUG) std::cout << "after deletion: " << rid.pageNum << " " << page.infos.size()<< " " <<page.num_record << std::endl;

    //change free byte in page index
    RID rid_copy;
    rid_copy.slotNum = rid.slotNum;
    rid_copy.pageNum = rid.pageNum;
    if(DEBUG) std::cout << "record deleted: " << rid.pageNum << " " << rid.slotNum << std::endl;
    return updatePageIndex(fileHandle, rid_copy, data_length);
}