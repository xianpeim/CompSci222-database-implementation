#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

RC PagedFileManager::createFile(const std::string &fileName) {
    if (ifExists(fileName)){    //check if file exists
        //std::cerr << "File already exists, file not created." << std::endl; // tcl
        return -1;
    }else {
        FILE *ifileptr = fopen(fileName.c_str(), "wb");
        if(ifileptr==NULL){
            //std::cerr << "File creation failed" << std::endl;
            return -1;
        }
        if(fclose(ifileptr)!=0){
            //std::cerr << "Closing file failed" << std::endl;
            return -1;
        }
        return 0;
    }
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    if (ifExists(fileName)){    //check if file exists
        remove(fileName.c_str());
        return 0;
    }else{
        //std::cerr << "File does not exist." << std::endl;   // tcl
        return -1;
    }
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    if (!ifExists(fileName)){    //check if file exists
        //std::cerr << "File does not exist." << std::endl;   // tcl
        return -1;
    }
    FILE* ifile = fopen (fileName.c_str(),"rb+ab");
    if (ifile == NULL){
        //std::cerr << "File open failed." << std::endl;  // tcl
        return -1;
    }
    if (fileHandle.getFilePtr() != NULL){
        //std::cerr << "File handler already open with another file." << std::endl;  // tcl
        return -1;
    }
    fileHandle.setFilePtr(ifile);
    fileHandle.setFileName(fileName);
    if (fileHandle.getNumberOfPages() == unsigned(0-1)){
        void *data = malloc(PAGE_SIZE);

        fileHandle.appendPage(data);
        fileHandle.appendPageCounter = 0;
        fileHandle.writeHiddenPage();
        fflush(ifile);
        free(data);
        fileHandle.appendPageCounter = 0;
    }else fileHandle.readHiddenPage();
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (!ifExists(fileHandle.getFileName())){    //check if file exists
        //std::cerr << "File does not exist." << std::endl;   // tcl
        return -1;
    }
    fileHandle.writeHiddenPage();
    FILE* ifileptr = fileHandle.getFilePtr();
    if (fclose(ifileptr)!=0){
        //std::cerr << "File close failed." << std::endl;  // tcl
        return -1;
    }
    fileHandle.setFilePtr(NULL);
    return 0;
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fileptr = NULL;
    rootIndex = -10;
    // reserving 1st page to store counters. every read write append action will be preformed on counter+1 page.
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum>getNumberOfPages()){
        //std::cerr << "PageNum larger than actual page number count." << std::endl;  // tcl
        return -1;
    }
    FILE* ifileptr = getFilePtr();
    if(fseek(fileptr,(pageNum+1)*PAGE_SIZE,SEEK_SET)!=0){
        //std::cerr << "Seek page failed" << std::endl;  // tcl
        return -1;
    }
    fread(data, sizeof(byte),PAGE_SIZE,ifileptr);
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum>getNumberOfPages()){
        //std::cerr << "PageNum larger than actual page number count." << std::endl;  // tcl
        return -1;
    }
    FILE* ifileptr = getFilePtr();
    if(fseek(fileptr,(pageNum+1)*PAGE_SIZE,SEEK_SET)!=0){
        //std::cerr << "Seek page failed" << std::endl;  // tcl
        return -1;
    }
    fwrite(data, sizeof(byte),PAGE_SIZE,ifileptr);
    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    FILE *ifileptr = getFilePtr();
    fseek(ifileptr,0,SEEK_END);
    if (fwrite(data, sizeof(byte), PAGE_SIZE, ifileptr) != PAGE_SIZE){
        std::cerr << "Append page failed." << std::endl;  // tcl
        return -1;
    }
    fflush(ifileptr);
    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    FILE* ifileptr = getFilePtr();
    if(ifileptr==NULL){
        std::cerr << "No file associated" << std::endl;  // tcl
        return -2;
    }
    long fsize = getFileSize();
    unsigned pagenum = (fsize%PAGE_SIZE)==0 ? fsize/PAGE_SIZE : fsize%PAGE_SIZE+1;
    return pagenum-1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}