#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef unsigned char byte;

#define PAGE_SIZE 4096

#include <string>
#include <cstring>
#include <string.h>
#include <climits>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <sys/stat.h>

class FileHandle;

class PagedFileManager {
public:
    static PagedFileManager &instance();                                // Access to the _pf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new file
    RC destroyFile(const std::string &fileName);                        // Destroy a file
    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
    RC closeFile(FileHandle &fileHandle);                               // Close a file

protected:
    PagedFileManager();                                                 // Prevent construction
    ~PagedFileManager();                                                // Prevent unwanted destruction
    PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
    PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    inline bool ifExists (const std::string& filename) {
        struct stat buffer;
        return (stat (filename.c_str(), &buffer) == 0);
    }

private:
    static PagedFileManager *_pf_manager;
};

class FileHandle {
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    int rootIndex;

    FileHandle();                                                       // Default constructor
    ~FileHandle();                                                      // Destructor

    static FileHandle& instance() {
        static FileHandle _f_handle = FileHandle();
        return _f_handle;
    }
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                            unsigned &appendPageCount);                 // Put current counter values into variables

    RC setFilePtr(FILE* inputfileptr){
        fileptr = inputfileptr;
        return 0;
    }
    FILE* getFilePtr(){
        return fileptr;
    }
    RC setFileName(std::string input){
        filename = input;
        return 0;
    }
    std::string getFileName(){
        return filename;
    }
    long getFileSize(){
        long f_begin =0, f_end = 0;
        std::fstream ifile;
        ifile.open(filename);
        f_begin = ifile.tellg();
        ifile.seekg( 0, std::ios::end );
        f_end = ifile.tellg();

        return f_end - f_begin;
    }
    void writeHiddenPage(){
        char* buffer = (char*)malloc(PAGE_SIZE);
        int offset = 0;
        memcpy(buffer+offset, &readPageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(buffer+offset, &writePageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(buffer+offset, &appendPageCounter, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(buffer+offset, &rootIndex, sizeof(int));
        offset += sizeof(int);

        fseek(fileptr,0,SEEK_SET);
        fwrite(buffer, sizeof(byte), PAGE_SIZE, fileptr);
        free(buffer);
    }
    void readHiddenPage(){
        char* buffer = (char*)malloc(PAGE_SIZE);
        fread(buffer, sizeof(byte), PAGE_SIZE, fileptr);

        int offset = 0;
        memcpy(&readPageCounter,buffer+offset, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(&writePageCounter,buffer+offset, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(&appendPageCounter,buffer+offset, sizeof(unsigned));
        offset += sizeof(unsigned);
        memcpy(&rootIndex,buffer+offset, sizeof(int));
        offset += sizeof(int);

        free(buffer);
    }

private:
    FILE* fileptr;
    std::string filename;
};

#endif