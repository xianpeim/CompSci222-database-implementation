#ifndef _rbfm_h_
#define _rbfm_h_

#include <string.h>
#include <vector>
#include <cmath>

#include "pfm.h"

#define epsilon 0.00005 // for float comparison

#define DEBUG 0     // 1 show all debug msg. 0 dont show.
#define DEBUGUNIT 0     // 1 show all unit debug msg. 0 dont show.
#define DEBUGFREE 0     // 1 show all unit free msg. 0 dont show.

//# TODO: add func call to handle memory leak. free func already defined.

// Record ID
typedef struct {
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0,  // =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;

class Field{
public:
    Field(){
    }
    ~Field(){
    }
    Attribute attr;
    union {
        int int_val;
        float real_val;
        char* varchar_val;
    };
};

class IntField: public Field{
public:
    IntField():Field(){}
    ~IntField(){}
};

class RealField: public Field{
public:
    RealField():Field(){}
    ~RealField(){}
};

class VarcharField: public Field{
public:
    VarcharField():Field(){}
    ~VarcharField(){}
};

struct RecordInfo {
    int offset;
    int length;
};

class Record{
public:
    Record(){
        total_size = 0;
    }
    Record(const std::vector<Attribute> &recordDescriptor, const void* data){
        if(DEBUG) std::cout << "record constructor 1"<< std::endl;
        field_size = 0;
        total_size = 0;
        attrs = recordDescriptor;
        null_size = ceil((double) recordDescriptor.size() / CHAR_BIT);
        null_val = (unsigned char*) malloc(sizeof(null_size));
        buffer = NULL;
        memcpy(null_val, data,null_size);
        if(null_size == 0 or null_val == NULL){
            std::cerr << "Null value cannot be read." << std::endl;
        }
        int offset = null_size;
        for(int i=0; i<recordDescriptor.size(); i++){
            int null_count_in_bit = i%8;
            int null_count_in_byte = i/8;
            if(!(null_val[null_count_in_byte] & (unsigned)1 << (unsigned)(7-null_count_in_bit))){   // check if corresponding bit is 1.
                switch (recordDescriptor[i].type){
                    case TypeInt:{
                        IntField int_field;
                        int_field.attr = recordDescriptor[i];
                        memcpy(&int_field.int_val, (char*)data+offset, recordDescriptor[i].length);
                        fields.push_back(int_field);
                        field_size += recordDescriptor[i].length;
                        offset += recordDescriptor[i].length;
                        break;
                    }
                    case TypeReal:{
                        RealField real_field;
                        real_field.attr = recordDescriptor[i];
                        memcpy(&real_field.real_val, (char*)data+offset, recordDescriptor[i].length);
                        fields.push_back(real_field);
                        field_size += recordDescriptor[i].length;
                        offset += recordDescriptor[i].length;
                        break;
                    }
                    case TypeVarChar:{
                        int length;
                        memcpy(&length, (char*)data+offset, sizeof(int));
                        offset += sizeof(int);
                        VarcharField varchar_field;
                        varchar_field.attr = recordDescriptor[i];
                        varchar_field.attr.length = length;
                        varchar_field.varchar_val = (char*)malloc(length);
                        memcpy(varchar_field.varchar_val, (char*)data+offset, length);
                        fields.push_back(varchar_field);
                        field_size += length + sizeof(int);
                        offset += length;
                        break;
                    }
                    default:{
                        std::cerr << "Date type wrong. it should be int/real/varchar" << std::endl;
                    }
                }
            }
            else{
                field_size += recordDescriptor[i].length;
            }
        }
        total_size = null_size + field_size;
    }
    Record(const std::vector<Attribute> &recordDescriptor, char* in_buffer, RecordInfo ri){
        if(DEBUG) std::cout << "record constructor 2"<< std::endl;
        if(DEBUG) std::cout << ri.length<< std::endl;
        if(DEBUG) std::cout << ri.offset<< std::endl;
        field_size = 0;
        total_size = 0;
        attrs = recordDescriptor;
        buffer = (char*) malloc(ri.length);
        memcpy(buffer, in_buffer+ri.offset, ri.length);
        null_size = ceil((double) recordDescriptor.size() / CHAR_BIT);
        null_val = (unsigned char*) malloc(sizeof(null_size));
        memcpy(null_val, buffer, null_size);
        if(null_size == 0 or null_val == NULL){
            std::cerr << "Null value cannot be read." << std::endl;
        }
        int offset = null_size;
        for(int i=0; i<recordDescriptor.size(); i++){
            int null_count_in_bit = i%8;
            int null_count_in_byte = i/8;
            if(!(null_val[null_count_in_byte] & (unsigned)1 << (unsigned)(7-null_count_in_bit))){   // check if corresponding bit is 1.
                switch (recordDescriptor[i].type){
                    case TypeInt:{
                        //if(DEBUG) std::cout << "record constructor 2 bp0"<< std::endl;
                        IntField int_field;
                        int_field.attr = recordDescriptor[i];
                        memcpy(&int_field.int_val, (char*)buffer+offset, recordDescriptor[i].length);
                        fields.push_back(int_field);
                        field_size += recordDescriptor[i].length;
                        offset += recordDescriptor[i].length;
                        break;
                    }
                    case TypeReal:{
                        //if(DEBUG) std::cout << "record constructor 2 bp1"<< std::endl;
                        RealField real_field;
                        real_field.attr = recordDescriptor[i];
                        memcpy(&real_field.real_val, (char*)buffer+offset, recordDescriptor[i].length);
                        fields.push_back(real_field);
                        field_size += recordDescriptor[i].length;
                        offset += recordDescriptor[i].length;
                        break;
                    }
                    case TypeVarChar:{
                        int length;
                        memcpy(&length, (char*)buffer+offset, sizeof(int));
                        //if(DEBUG) std::cout << "record constructor 2 bp4"<< std::endl;
                        offset += sizeof(int);
                        VarcharField varchar_field;
                        varchar_field.attr = recordDescriptor[i];
                        varchar_field.attr.length = length;
                        varchar_field.varchar_val = (char*)malloc(length);
                        //if(DEBUG) std::cout << "record constructor 2 bp4.5: " << length << std::endl;
                        memcpy(varchar_field.varchar_val, (char*)buffer+offset, length);
                        //if(DEBUG) std::cout << "record constructor 2 bp5"<< std::endl;
                        fields.push_back(varchar_field);
                        field_size += length + sizeof(int);
                        offset += length;
                        break;
                    }
                    default:{
                        std::cerr << "Date type wrong. it should be int/real/varchar" << std::endl;
                    }
                }
            }
            else{
                field_size += recordDescriptor[i].length;
                offset += recordDescriptor[i].length;
            }
        }
        total_size = null_size+ field_size;
        if(DEBUG) std::cout << "record constructor 2 bp2"<< std::endl;
    }
    ~Record(){}
    std::vector<Attribute> attrs;
    std::vector<Field> fields;
    RID rid;
    int null_size;
    unsigned char* null_val;
    int total_size;
    int field_size;
    char* buffer;
    RC updateBuffer();
    RC getData(void* data_out);
    RC getSpecificData(void *data_out, const std::vector<std::string> &attributeNames);
    int getActualByteForNullsIndicator(int fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }
    RC freeMalloc(){
        if(buffer!=NULL){
            free(buffer);
            buffer = NULL;
            if(DEBUGFREE) std::cout << "    record buffer freed"<< std::endl;
        }
        if(null_val!=NULL){
            free(null_val);
            null_val = NULL;
            if(DEBUGFREE) std::cout << "    record null val freed"<< std::endl;
        }
        for(int i=0; i<fields.size(); i++){
            if(fields[i].attr.type == TypeVarChar and fields[i].varchar_val!=NULL){
                free(fields[i].varchar_val);
                fields[i].varchar_val = NULL;
            }
        }
        if(DEBUGFREE) std::cout << "    record fields freed"<< std::endl;
        return 0;
    }
};

struct PageIndexInfo{
    int offset;
    int free_byte;
};

class PageIndex{
public:
    PageIndex(){
        if(DEBUG) std::cout << "pageindex constructor 1"<< std::endl;
        num = 0;
        next = -1;
        buffer = (char*)malloc(PAGE_SIZE);
    }
    PageIndex(char* to_buffer){
        if(DEBUG) std::cout << "pageindex constructor 2"<< std::endl;
        buffer = (char*) malloc(PAGE_SIZE);
        memcpy(buffer, to_buffer, PAGE_SIZE);
        memcpy(&num, buffer, sizeof(int));
        memcpy(&next, buffer+ sizeof(int), sizeof(int));
        int offset = 2* sizeof(int);
        for(int i = 0; i < num; i++){
            PageIndexInfo pii;
            memcpy(&pii, buffer+offset, sizeof(PageIndexInfo));
            infos.push_back(pii);
            offset += sizeof(PageIndexInfo);
        }
    }
    ~PageIndex(){}
    int num;
    int next;
    std::vector<PageIndexInfo> infos;
    char* buffer;
    RC updateBuffer();
    RC freeMalloc(){
        if(buffer!=NULL){
            if(DEBUGFREE) std::cout << "  page index buffer freed" << std::endl;
            free(buffer);
            buffer = NULL;
        }
        return 0;
    }
};

class Page{
public:
    Page(){
        if(DEBUG) std::cout << "page constructor 1"<< std::endl;
        buffer = (char*)malloc(PAGE_SIZE);
    }
    Page(char* buffer_in, const std::vector<Attribute> &recordDescriptor){
        if(DEBUG) std::cout << "page constructor 2"<< std::endl;
        buffer = (char*)malloc(PAGE_SIZE);
        memcpy(buffer, buffer_in, PAGE_SIZE);
        memcpy(&free_offset, buffer+PAGE_SIZE- sizeof(int), sizeof(int));
        memcpy(&num_record, buffer+PAGE_SIZE- 2*sizeof(int), sizeof(int));
        if(DEBUG) std::cout << "num of record: "<< num_record << std::endl;
        int offset = PAGE_SIZE - 2*sizeof(int);
        for(int i=0; i<num_record; i++){
            RecordInfo ri;
            offset -= sizeof(RecordInfo);
            memcpy(&ri, buffer+offset, sizeof(RecordInfo));
            infos.push_back(ri);
            if(ri.offset <= -1){
                Record record;
                records.push_back(record);
                continue;
            }
            Record record(recordDescriptor, buffer, ri);
            records.push_back(record);
            /*record.buffer = NULL;
            record.null_val = NULL;
            record.fields.clear();*/
        }
    }
    ~Page(){}
    char* buffer;
    std::vector<Record> records;
    std::vector<RecordInfo> infos;
    int free_offset;
    int num_record;
    RC updateBuffer();
    RC freeMalloc(){
        if(buffer!=NULL){
            if(DEBUGFREE) std::cout << "  page buffer freed" << std::endl;
            free(buffer);
            buffer = NULL;
        }
        for(int i=0; i< records.size(); i++){
            if(infos[i].offset <= -1) continue;
            records[i].freeMalloc();
        }
        if(DEBUGFREE) std::cout << "  page records freed" << std::endl;
        return 0;
    }
};
/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
    RBFM_ScanIterator(){
        cur_index = 0;
    }

    ~RBFM_ScanIterator() = default;;

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data) {
        //if(DEBUG) std::cout << "rids size: "<< rids.size() << std::endl;
        if(cur_index == rids.size()) return RBFM_EOF;
        rid.slotNum = rids[cur_index].slotNum;
        rid.pageNum = rids[cur_index].pageNum;
        if(DEBUG) std::cout<<"getting next record"<< std::endl;
        if(DEBUG) std::cout<<"rid scanned: " << rid.pageNum << " " << rid.slotNum << std::endl;
        char* buffer = (char*)malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, buffer);
        Page page(buffer, recordDescriptor);
        //if(DEBUG) std::cout << "page infos size: " << page.infos.size() << std::endl;
        RecordInfo ri;
        memcpy(&ri, &page.infos[rid.slotNum], sizeof(RecordInfo));
        if(ri.offset == -1) return -1;
        if(ri.offset < -1){
            RID rid_point_to;
            rid_point_to.pageNum = ri.offset + 10000000;
            rid_point_to.slotNum = ri.length;
            return getNextRecord(rid_point_to, data);
        }
        Record record(recordDescriptor, buffer, ri);
        if(attrs_need.size() == 0){
            record.getData(data);
            free(buffer);
            buffer = NULL;
            page.freeMalloc();
            record.freeMalloc();
            cur_index += 1;
            return 0;
        }
        else{
            record.getSpecificData(data, attrs_need);
            free(buffer);
            buffer = NULL;
            page.freeMalloc();
            record.freeMalloc();
            cur_index += 1;
            return 0;
        }
    };

    RC setFileHandle(FileHandle &fileHandle){
        this->fileHandle = fileHandle;
        return 0;
    }

    RC setRecordDescriptor(const std::vector<Attribute> recordDescriptor){
        this->recordDescriptor = recordDescriptor;
        return 0;
    }

    RC clear(){
        cur_index = 0;
        rids.clear();
        attrs_need.clear();
        recordDescriptor.clear();
        return 0;
    }

    RC setAttrsNeed(const std::vector<std::string> &attributeNames){
        attrs_need = attributeNames;
        return 0;
    }

    RC close() {
        clear();
        return 0;
    };

    std::vector<RID> rids;
    int cur_index;
    FileHandle fileHandle;
    std::vector<Attribute> recordDescriptor;
    std::vector<std::string> attrs_need;
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new record-based file

    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

    RC insertInNewPage(FileHandle& fileHandle, Record& record, PageIndex& pi, RID& rid, int& num_page);

    RC addNewPageIndex(FileHandle& fileHandle, int &num_page);

    RC findAvailPage(FileHandle& fileHandle, PageIndex& pi, Record& record, int& avail_page_num);

    RC insertToExistingPage(FileHandle& fileHandle, const std::vector<Attribute> &recordDescriptor,
                            int avail_page_num, Record& record, PageIndex& pi, int found_pageIndex_index , RID& rid);

    RC updatePageIndex(FileHandle &fileHandle, RID& rid, int data_length);

    RC deleteRecordWithPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, Page &page);

public:
    int num_of_pageIndex;
    int cur_pageIndex;

protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment


private:
    static RecordBasedFileManager *_rbf_manager;
    static PagedFileManager& _pf_manager;
};

#endif