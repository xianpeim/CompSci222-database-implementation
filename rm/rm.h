#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator(){
        si.clear();
        si.cur_index = 0;
    };

    ~RM_ScanIterator() = default;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) {
        _rbf_manager.openFile(fileName, _f_handle);
        //if(DEBUG) std::cout << "num of rids: " <<si.rids.size() << std::endl;
        RC rc = si.getNextRecord(rid, data);
        _rbf_manager.closeFile(_f_handle);
        return rc;
    };

    RC close() {
        return si.close();
    };

    RBFM_ScanIterator si;
    static RecordBasedFileManager& _rbf_manager;
    static FileHandle& _f_handle;
    std::string fileName;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
    RM_IndexScanIterator(){
        si.clear();
        si.cur_index = 0;
    };

    ~RM_IndexScanIterator() = default;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextEntry(RID &rid, void *key) {
        _i_manager.openFile(fileName, _ixf_handle);
        RC rc = si.getNextEntry(rid, key);
        _i_manager.closeFile(_ixf_handle);
        return rc;
    };

    RC close() {
        return si.close();
    };                     // Terminate index scan


    IX_ScanIterator si;
    static IndexManager& _i_manager;
    static IXFileHandle& _ixf_handle;
    std::string fileName;
};


// Relation Manager
class RelationManager {
public:
    static RelationManager &instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC deleteTable(const std::string &tableName);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector<Attribute> &attrs, const void *data);

    RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const std::string &tableName,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    // Extra credit work (10 points)
    RC addAttribute(const std::string &tableName, const Attribute &attr);

    RC dropAttribute(const std::string &tableName, const std::string &attributeName);


    // Calculate actual bytes for nulls-indicator for the given field counts
    int getActualByteForNullsIndicator(int fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }
    //prepare data for tables catalog
    void prepareTableCatalogRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int id, const int tableNameLength, const std::string &tableName,
                                   const int fileNameLength, const std::string &fileName, void *buffer, int *recordSize) {
        int offset = 0;
        // Null-indicators
        bool nullBit = false;
        int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);
        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;
        // Is the id field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 7;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &id, sizeof(int));
            offset += sizeof(int);
        }
        // Is the table name field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 6;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &tableNameLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, tableName.c_str(), tableNameLength);
            offset += tableNameLength;
        }
        // Is the file name field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 5;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &fileNameLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, fileName.c_str(), fileNameLength);
            offset += fileNameLength;
        }

        *recordSize = offset;
    }

    void createTableCatalogRecordDescriptor(std::vector<Attribute> &recordDescriptor) {

        Attribute attr;
        attr.name = "Id";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "TableName";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 50;
        recordDescriptor.push_back(attr);

        attr.name = "FileName";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 50;
        recordDescriptor.push_back(attr);
    }

    //prepare data for tables catalog
    void prepareColumnCatalogRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int id, const int columnNameLength, const std::string &columnName,
                                    const int type, const int length, const int position, void *buffer, int *recordSize) {
        int offset = 0;
        // Null-indicators
        bool nullBit = false;
        int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);
        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;
        // Is the id field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 7;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &id, sizeof(int));
            offset += sizeof(int);
        }
        // Is the column name field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 6;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &columnNameLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, columnName.c_str(), columnNameLength);
            offset += columnNameLength;
        }
        // Is the type field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 5;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &type, sizeof(int));
            offset += sizeof(int);
        }
        // Is the column length field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 4;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &length, sizeof(int));
            offset += sizeof(int);
        }
        // Is the column position field not-NULL?
        nullBit = nullFieldsIndicator[0] & (unsigned) 1 << (unsigned) 3;
        if (!nullBit) {
            memcpy((char *) buffer + offset, &position, sizeof(int));
            offset += sizeof(int);
        }

        *recordSize = offset;
    }

    void createColumnCatalogRecordDescriptor(std::vector<Attribute> &recordDescriptor) {

        Attribute attr;
        attr.name = "Id";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "ColumnName";
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 50;
        recordDescriptor.push_back(attr);

        attr.name = "Type";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "Length";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "Position";
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);
    }

    // QE IX related
    RC createIndex(const std::string &tableName, const std::string &attributeName);

    RC destroyIndex(const std::string &tableName, const std::string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const std::string &tableName,
            const std::string &attributeName,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            RM_IndexScanIterator &rm_IndexScanIterator);


protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    static RelationManager *_relation_manager;

    static RecordBasedFileManager& _rbf_manager;
    static FileHandle& _f_handle;
    static IndexManager& _i_manager;
    static IXFileHandle& _ixf_handle;

    std::vector<Attribute> cur_attrs;
    std::string cur_tableName;
    Attribute ix_attr;
    std::string cur_ixName;
};

#endif