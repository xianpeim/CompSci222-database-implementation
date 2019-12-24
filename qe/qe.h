#ifndef _qe_h_
#define _qe_h_

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#include <map>

#define QE_EOF (-1)  // end of the index scan

#define BUFFSIZE 200

typedef enum {
    MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void *data;             // value
};

struct BNLInfo {
    int offset;
    int length;
    int attr_offset;
};

struct Condition {
    std::string lhsAttr;        // left-hand side attribute
    CompOp op;                  // comparison operator
    bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;

    virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;

    virtual ~Iterator() = default;

    int getActualByteForNullsIndicator(int fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }

    bool float_eq(float a, float b){
        if(abs(a-b) < epsilon) return true;
        else return false;
    }

    virtual void resetIter() = 0;
};

class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    std::string tableName;
    std::vector<Attribute> attrs;
    std::vector<std::string> attrNames;
    RID rid{};

    TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        for (Attribute &attr : attrs) {
            // convert to char *
            attrNames.push_back(attr.name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) override {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~TableScan() override {
        iter->close();
    };

    void resetIter() override{
        iter->si.cur_index = 0;
    };
};

class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    std::string tableName;
    std::string attrName;
    std::vector<Attribute> attrs;
    char key[PAGE_SIZE]{};
    RID rid{};

    IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL)
            : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;


        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) override {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName, rid, data);
        }
        return rc;
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;


        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~IndexScan() override {
        iter->close();
    };

    void resetIter() override{
        iter->si.cur_index = 0;
    };
};

class Filter : public Iterator {
    // Filter operator
public:
    std::vector<Attribute> attrs;
    Condition cond;
    Iterator *iter;

    Filter(Iterator *input,               // Iterator of input R
           const Condition &condition     // Selection condition
    );

    ~Filter() override = default;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    void resetIter() override{
        iter->resetIter();
    };

};

class Project : public Iterator {
    // Projection operator
public:
    std::vector<Attribute> attrs;
    std::vector<std::string> attrNames;
    Iterator *iter;
    Project(Iterator *input,                    // Iterator of input R
            const std::vector<std::string> &attrNames);   // std::vector containing attribute names
    ~Project() override = default;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    void resetIter() override{
        iter->resetIter();
    };
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
public:
    char* left_buffer;
    char* right_buffer;
    //char* output_buffer;
    Iterator *left_iter;
    TableScan *right_iter;
    std::vector<Attribute> left_attrs;
    std::vector<Attribute> right_attrs;
    std::vector<Attribute> total_attrs;
    Condition cond;
    bool right_reset, left_load_next, right_load_next, left_done, right_done;
    int left_offset, right_offset, cur_left_index, cur_right_index, left_size;
    int left_null_size, right_null_size, total_null_size, left_attr_index, right_attr_index;
    std::vector<BNLInfo> left_bi, right_bi;
    AttrType attrType;
    bool error;
    BNLJoin(Iterator *leftIn,            // Iterator of input R
            TableScan *rightIn,           // TableScan Iterator of input S
            const Condition &condition,   // Join condition
            const unsigned numPages       // # of pages that can be loaded into memory,
            //   i.e., memory block size (decided by the optimizer)
    );

    //~BNLJoin() override = default;;
    ~BNLJoin() override {
        if(left_buffer!=NULL){
            free(left_buffer);
            left_buffer = NULL;
        }
        if(right_buffer!=NULL){
            free(right_buffer);
            right_buffer = NULL;
        }
    };

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    void resetIter() override{
        left_iter->resetIter();
        right_iter->resetIter();
    };
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
public:
    char* left_buffer;
    char* right_buffer;
    Iterator *left_iter;
    IndexScan *right_iter;
    std::vector<Attribute> left_attrs;
    std::vector<Attribute> right_attrs;
    std::vector<Attribute> total_attrs;
    Condition cond;
    AttrType attrType;
    bool error;
    int left_attr_index, right_attr_index;
    int left_null_size, right_null_size, total_null_size, left_size, right_size;
    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    );

    //~INLJoin() override = default;
    ~INLJoin() override {
        if(left_buffer!=NULL){
            free(left_buffer);
            left_buffer = NULL;
        }
        if(right_buffer!=NULL){
            free(right_buffer);
            right_buffer = NULL;
        }
    };

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    void resetIter() override{
        left_iter->resetIter();
        right_iter->resetIter();
    };
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
public:
    char* left_buffer;
    char* right_buffer;
    Iterator *left_iter;
    IndexScan *right_iter;
    std::vector<Attribute> left_attrs;
    std::vector<Attribute> right_attrs;
    std::vector<Attribute> total_attrs;
    Condition cond;
    AttrType attrType;

    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    );

    ~GHJoin() override = default;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    void resetIter() override{
        left_iter->resetIter();
        right_iter->resetIter();
    };
};

class Aggregate : public Iterator {
    // Aggregation operator
public:
    Iterator* iter;
    AggregateOp op;
    Attribute aggAttr;
    Attribute gAttr;
    std::vector<Attribute> attrs;
    int null_size;
    unsigned char* null_val;
    int int_max, int_min, avg_num;
    float float_max, float_min, avg;
    bool done, max_init, min_init;
    float sum, count, group;
    std::map<int, float> int_hash;
    std::map<float, float> real_hash;
    std::map<std::string, float> varchar_hash;
    std::map<int, float>::iterator int_hash_iter;
    std::map<float, float>::iterator real_hash_iter;
    std::map<std::string, float>::iterator varchar_hash_iter;
    std::map<int, float> int_avg_num;
    std::map<float, float> real_avg_num;
    std::map<std::string, float> varchar_avg_num;
    bool hash_init;
    // Mandatory
    // Basic aggregation
    Aggregate(Iterator *input,          // Iterator of input R
              const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
              AggregateOp op            // Aggregate operation
    );

    // Optional for everyone: 5 extra-credit points
    // Group-based hash aggregation
    Aggregate(Iterator *input,             // Iterator of input R
              const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
              const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
              AggregateOp op              // Aggregate operation
    );

    //~Aggregate() override = default;
    ~Aggregate() override {
        if(null_val!=NULL){
            free(null_val);
            null_val = NULL;
        }
    };

    RC getNextTuple(void *data) override;

    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(std::vector<Attribute> &attrs) const override;

    void resetIter() override{
        iter->resetIter();
    };
};

#endif
