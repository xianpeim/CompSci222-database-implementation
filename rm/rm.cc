#include "rm.h"

RelationManager *RelationManager::_relation_manager = nullptr;

RecordBasedFileManager& RelationManager::_rbf_manager = RecordBasedFileManager::instance();
FileHandle& RelationManager::_f_handle = FileHandle::instance();
IndexManager& RelationManager::_i_manager = IndexManager::instance();
IXFileHandle& RelationManager::_ixf_handle = IXFileHandle::instance();

RecordBasedFileManager& RM_ScanIterator::_rbf_manager = RecordBasedFileManager::instance();
FileHandle& RM_ScanIterator::_f_handle = FileHandle::instance();

IndexManager& RM_IndexScanIterator::_i_manager = IndexManager::instance();
IXFileHandle& RM_IndexScanIterator::_ixf_handle = IXFileHandle::instance();

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() = default;

RelationManager::~RelationManager() { delete _relation_manager; }

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
    if (_rbf_manager.createFile("Tables") != 0) return -1;
    if(_rbf_manager.openFile("Tables", _f_handle) != 0) return -2;
    RID rid;
    int size;
    std::vector<Attribute> tableCatalogRecordDescriptor;
    createTableCatalogRecordDescriptor(tableCatalogRecordDescriptor);
    void *record = malloc(200);
    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(3);
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    prepareTableCatalogRecord(3, nullsIndicator, 0, 6, "Tables",
                              6, "Tables", record, &size);
    _rbf_manager.insertRecord(_f_handle, tableCatalogRecordDescriptor, record, rid);
    memset(record, 0, 200);
    prepareTableCatalogRecord(3, nullsIndicator, 1, 7, "Columns",
                              7, "Columns", record, &size);
    _rbf_manager.insertRecord(_f_handle, tableCatalogRecordDescriptor, record, rid);
    _rbf_manager.closeFile(_f_handle);

    if(_rbf_manager.createFile("Columns") != 0) return -3;
    if(_rbf_manager.openFile("Columns", _f_handle) != 0) return -4;
    std::vector<Attribute> columnCatalogRecordDescriptor;
    createColumnCatalogRecordDescriptor(columnCatalogRecordDescriptor);
    // NULL field indicator
    nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(5);
    nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 0, 8, "table-id",
                               0,4, 0, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 0, 10, "table-name",
                               2, 50, 1, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 0, 9, "file-name",
                               2,50, 2, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);

    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 1, 8, "table-id",
                               0, 4,0, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 1, 11, "column-name",
                               2,50, 1, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 1, 11, "column-type",
                               0, 4,2, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 1, 13, "column-length",
                               0, 4,3, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
    memset(record, 0, 200);
    prepareColumnCatalogRecord(5, nullsIndicator, 1, 15, "column-position",
                               0, 4,4, record, &size);
    _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
    _rbf_manager.closeFile(_f_handle);

    return 0;
}

RC RelationManager::deleteCatalog() {
    _rbf_manager.destroyFile("Columns");
    _rbf_manager.destroyFile("Tables");
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    if(_rbf_manager.createFile(tableName) != 0) return -1;

    if(_rbf_manager.openFile("Tables", _f_handle) != 0) return -2;

    std::vector<Attribute> tableCatalogRecordDescriptor;
    createTableCatalogRecordDescriptor(tableCatalogRecordDescriptor);
    RBFM_ScanIterator rbfmsi;
    std::vector<std::string> nothing;
    _rbf_manager.scan(_f_handle, tableCatalogRecordDescriptor,"TableName", EQ_OP, tableName.c_str(), nothing, rbfmsi);
    if(rbfmsi.rids.size() != 0) return 0;

    RID rid;
    char* tmp_page = (char*)malloc(PAGE_SIZE);
    _f_handle.readPage(_rbf_manager.cur_pageIndex, tmp_page);
    PageIndex pi(tmp_page);
    rid.pageNum = pi.infos[pi.infos.size()-1].offset;
    pi.freeMalloc();
    // get rid
    _f_handle.readPage(rid.pageNum, tmp_page);
    Page page(tmp_page,tableCatalogRecordDescriptor);
    rid.slotNum = page.infos.size()-1;
    page.freeMalloc();
    free(tmp_page);
    tmp_page = NULL;

    int* data = (int*)malloc(4);
    _rbf_manager.readAttribute(_f_handle, tableCatalogRecordDescriptor, rid, "Id", data);
    // std::cout << "id is: " << (*data) << std::endl;
    // write to Tables
    int new_id = (*data)+1;
    free(data);
    data = NULL;
    void *record = malloc(200);
    int size;
    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(3);
    auto *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    prepareTableCatalogRecord(3, nullsIndicator, new_id, tableName.length(), tableName,
                              tableName.length(), tableName, record, &size);
    _rbf_manager.insertRecord(_f_handle, tableCatalogRecordDescriptor, record, rid);
    _rbf_manager.closeFile(_f_handle);


    _rbf_manager.openFile("Columns", _f_handle);
    std::vector<Attribute> columnCatalogRecordDescriptor;
    createColumnCatalogRecordDescriptor(columnCatalogRecordDescriptor);
    // NULL field indicator
    nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memset(record, 0, 200);
    for(int i=0; i< attrs.size(); i++){
        prepareColumnCatalogRecord(5, nullsIndicator, new_id, attrs[i].name.length(), attrs[i].name,
                                   attrs[i].type,attrs[i].length, i, record, &size);
        _rbf_manager.insertRecord(_f_handle, columnCatalogRecordDescriptor, record, rid);
        memset(record, 0, 200);
    }
    _rbf_manager.closeFile(_f_handle);
    free(record);
    record = NULL;
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    if(tableName == "Tables" or tableName == "Columns") return -1;
    _rbf_manager.destroyFile(tableName);
    //#TODO: delete record in tables and columns
    return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    // based on tableName, go find table id from table catalog table.
    if(DEBUG) std::cout << "getting attr" << std::endl;
    _rbf_manager.openFile("Tables", _f_handle);
    RBFM_ScanIterator si;
    int length = tableName.length();
    std::vector<Attribute> tableCatalogRecordDescriptor;
    createTableCatalogRecordDescriptor(tableCatalogRecordDescriptor);
    char* scan_data = (char*)malloc(tableName.length()+ sizeof(int));
    memcpy(scan_data, &length, sizeof(int));
    memcpy(scan_data+ sizeof(int), tableName.c_str(), length);
    std::vector<std::string> attr_needed;
    //if(DEBUG) std::cout << "attr_need size out: " << attr_needed.size() << std::endl;
    _rbf_manager.scan(_f_handle, tableCatalogRecordDescriptor, "TableName", EQ_OP, scan_data, attr_needed , si);
    void* data = malloc(200);
    RID rid;
    si.getNextRecord(rid, data);
    Record record(tableCatalogRecordDescriptor, data);
    //if(DEBUG) std::cout << "get attr bp3" << std::endl;
    int id;
    for(int i=0;i<record.fields.size();i++){
        if(record.fields[i].attr.name == "Id") {
            //if(DEBUG)std::cout << "id found:  "<< record.fields[i].int_val << std::endl;
            id = record.fields[i].int_val;
        }
        /*if(record.fields[i].attr.name == "TableName") {
            std::cout << "name found:  ";
            for(int j=0;j<record.fields[i].attr.length;j++){
                std::cout << *(record.fields[i].varchar_val + j);
            }
            std::cout<<std::endl;
        }*/
    }
    free(scan_data);
    record.freeMalloc();
    _rbf_manager.closeFile(_f_handle);
    //based on id. go find attrs in columns catalog table.
    _rbf_manager.openFile("Columns", _f_handle);
    std::vector<Attribute> columnCatalogRecordDescriptor;
    createColumnCatalogRecordDescriptor(columnCatalogRecordDescriptor);
    scan_data = (char*)malloc(sizeof(int));
    memcpy(scan_data, &id, sizeof(int));
    _rbf_manager.scan(_f_handle, columnCatalogRecordDescriptor, "Id", EQ_OP, scan_data, attr_needed , si);
    for(int i=0; i< si.rids.size(); i++){
        si.getNextRecord(rid, data);
        Record record(columnCatalogRecordDescriptor, data);
        Attribute tmp_attr;
        tmp_attr.name = std::string(record.fields[1].varchar_val, record.fields[1].varchar_val + record.fields[1].attr.length);
        tmp_attr.type = AttrType(record.fields[2].int_val);
        tmp_attr.length = record.fields[3].int_val;
        attrs.push_back(tmp_attr);
        record.freeMalloc();
    }
    free(scan_data);
    scan_data = NULL;
    free(data);
    data = NULL;
    _rbf_manager.closeFile(_f_handle);
    if(DEBUG) std::cout << "got attr" << std::endl;

    return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    //std::cout << "inserting tuple" << std::endl;
    std::vector<Attribute> attrs;
    if(cur_tableName != tableName){
        getAttributes(tableName, attrs);
        cur_attrs = attrs;
        cur_tableName = tableName;
    }else{
        attrs = cur_attrs;
    }
    if(_rbf_manager.openFile(tableName, _f_handle) == -1) return -1;
    RC rc = _rbf_manager.insertRecord(_f_handle, attrs, data, rid);
    _rbf_manager.closeFile(_f_handle);
    //std::cout << "insertion done" << std::endl;
    return rc;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    //std::cout << "deleting tuple" << std::endl;
    std::vector<Attribute> attrs;
    if(cur_tableName != tableName){
        getAttributes(tableName, attrs);
        cur_attrs = attrs;
        cur_tableName = tableName;
    }else{
        attrs = cur_attrs;
    }
    if(_rbf_manager.openFile(tableName, _f_handle) == -1) return -1;
    RC rc = _rbf_manager.deleteRecord(_f_handle,attrs,rid);
    _rbf_manager.closeFile(_f_handle);
    //std::cout << "deletion done" << std::endl;
    return rc;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    //std::cout << "updating tuple" << std::endl;
    std::vector<Attribute> attrs;
    if(cur_tableName != tableName){
        getAttributes(tableName, attrs);
        cur_attrs = attrs;
        cur_tableName = tableName;
    }else{
        attrs = cur_attrs;
    }
    if(_rbf_manager.openFile(tableName, _f_handle) == -1) return -1;
    RC rc = _rbf_manager.updateRecord(_f_handle,attrs,data,rid);
    _rbf_manager.closeFile(_f_handle);
    //std::cout << "update done" << std::endl;
    return rc;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    std::vector<Attribute> attrs;
    if(cur_tableName != tableName){
        getAttributes(tableName, attrs);
        cur_attrs = attrs;
        cur_tableName = tableName;
    }else{
        attrs = cur_attrs;
    }
    if(_rbf_manager.openFile(tableName, _f_handle) == -1) return -1;
    RC rc = _rbf_manager.readRecord(_f_handle, attrs, rid, data);
    _rbf_manager.closeFile(_f_handle);
    return rc;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return _rbf_manager.printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    std::vector<Attribute> attrs;
    if(cur_tableName != tableName){
        getAttributes(tableName, attrs);
        cur_attrs = attrs;
        cur_tableName = tableName;
    }else{
        attrs = cur_attrs;
    }
    int position;
    for(int i=0; i< attrs.size(); i++){
        if(attrs[i].name == attributeName){
            position = i;
            //std::cout << "position: " << position << std::endl;
            break;
        }
    }
    char* tmp_data = (char*)malloc(4000);
    if(_rbf_manager.openFile(tableName, _f_handle) == -1){
        free(tmp_data);
        tmp_data = NULL;
        return -1;
    }
    RC rc = _rbf_manager.readRecord(_f_handle, attrs, rid, tmp_data);
    _rbf_manager.closeFile(_f_handle);
    Record record(attrs, tmp_data);
    for(int i=0; i<record.fields.size(); i++){
        std::cout << record.fields[i].attr.name << std::endl;
        std::cout << record.fields[i].attr.length << std::endl;
    }
    //null bit
    memcpy(data, tmp_data, 1);
    //actual data
    switch (record.attrs[position].type){
        case TypeInt: {
            memcpy((char*)data+1, &record.fields[position].int_val, sizeof(int));
            break;
        }
        case TypeReal: {
            memcpy((char*)data+1, &record.fields[position].real_val, sizeof(float));
        }
        case TypeVarChar: {
            memcpy((char*)data+1, &record.fields[position].attr.length, sizeof(int));
            memcpy((char*)data+1+ sizeof(int), record.fields[position].varchar_val, record.fields[position].attr.length);
            break;
        }
        default:{

        }
    }
    record.freeMalloc();
    free(tmp_data);
    tmp_data = NULL;
    return 0;
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {

    //std::cout << "scanning" << std::endl;
    std::vector<Attribute> attrs;
    if(cur_tableName != tableName){
        getAttributes(tableName, attrs);
        cur_attrs = attrs;
        cur_tableName = tableName;
    }else{
        attrs = cur_attrs;
    }
    if(_rbf_manager.openFile(tableName, _f_handle) == -1) return -1;
    RC rc = _rbf_manager.scan(_f_handle, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.si);
    _rbf_manager.closeFile(_f_handle);
    rm_ScanIterator.fileName = tableName;
    //std::cout << "scan done" << std::endl;
    return rc;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
    // got to catalog get table attri info.
    std::vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    for(int i=0; i< attrs.size(); i++){
        if(attrs[i].name == attributeName){
            ix_attr.name = attrs[i].name;
            ix_attr.length = attrs[i].length;
            ix_attr.type = attrs[i].type;
            break;
        }
    }
    cur_ixName = tableName;
    // scan record into index.
    RM_ScanIterator si;
    std::vector<std::string> attr_needed;
    // here only grabbing indexed attribute.
    attr_needed.push_back(attributeName);

    scan(tableName, attributeName, NO_OP, NULL, attr_needed, si);

    void* data = malloc(4000);
    void* data_without_null = malloc(4000);
    RID rid;
    RC rc = 0;
    //IXFileHandle ixFileHandle;

    std::string index_name = tableName+"_index_"+attributeName;
    _i_manager.createFile(index_name);
    if(DEBUG) std::cout << "rm create index bp6" << std::endl;
    while(1) {
        rc = si.getNextTuple(rid, data);
        if(rc==-1) break;
        memcpy(data_without_null, (char*)data+1 , 3999);
        _i_manager.openFile(index_name, _ixf_handle);
        _i_manager.insertEntry(_ixf_handle,ix_attr,data_without_null,rid);
        _i_manager.closeFile(_ixf_handle);
    }


    free(data);
    data = NULL;

    return 0;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {

    std::string index_name = tableName+"_index_"+attributeName;

    return _i_manager.destroyFile(index_name);
}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {

    Attribute attr;
    if(cur_ixName != tableName){
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        for(int i=0; i< attrs.size(); i++){
            if(attrs[i].name == attributeName){
                ix_attr.name = attrs[i].name;
                ix_attr.length = attrs[i].length;
                ix_attr.type = attrs[i].type;
                break;
            }
        }
    }else{
        attr = ix_attr;
    }
    /*if(_i_manager.openFile(tableName+"_index_"+attributeName, _ixf_handle) == -1) return -1;
    if(_ixf_handle.getNumberOfPages() == 0){
        _i_manager.closeFile(_ixf_handle);
        std::string tmp = tableName+"_index_"+attributeName;
        remove(tmp.c_str());
        createIndex(tableName, attributeName);
        if(_i_manager.openFile(tableName+"_index_"+attributeName, _ixf_handle) == -1) return -1;
    }*/
    std::string tmp = tableName+"_index_"+attributeName;
    remove(tmp.c_str());
    createIndex(tableName, attributeName);
    if(_i_manager.openFile(tableName+"_index_"+attributeName, _ixf_handle) == -1) return -1;
    RC rc = _i_manager.scan(_ixf_handle, ix_attr, lowKey,highKey,lowKeyInclusive,highKeyInclusive, rm_IndexScanIterator.si);
    rm_IndexScanIterator.si.cur_index = 0;
    _i_manager.closeFile(_ixf_handle);
    rm_IndexScanIterator.fileName = tableName+"_index_"+attributeName;
    //std::cout << "scan done" << std::endl;
    return rc;

}

