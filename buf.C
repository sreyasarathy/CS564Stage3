#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

int BufHashTbl::hash(const File* file, const int pageNo) {
    return (std::hash<const File*>()(file) + pageNo) % HTSIZE;
}

Status BufHashTbl::insert(const File* file, const int pageNo, const int frameNo) {
    int currentIndex = hash(file, pageNo);
    hashBucket* currentEntry = ht[currentIndex];

    while(currentEntry != nullptr) {
        if(currentEntry->file == file && currentEntry->pageNo == pageNo) {
            return HASHTBLERROR;
            //already exists so error
        }

        currentEntry = currentEntry->next;
    }

    hashBucket* newEntry = new hashBucket();
    //it wouldn't let me just do = file due to const issues
    newEntry->file = const_cast<File*>(file); 
    newEntry->pageNo = pageNo;
    newEntry->frameNo = frameNo;
    newEntry->next = ht[currentIndex];
    ht[currentIndex] = newEntry;

    return OK;



}


int BufHashTbl::hash(const File* file, const int pageNo) {
    return (std::hash<const File*>()(file) + pageNo) % HTSIZE;
}

Status BufHashTbl::insert(const File* file, const int pageNo, const int frameNo) {
    int currentIndex = hash(file, pageNo);
    hashBucket* currentEntry = ht[currentIndex];

    while(currentEntry != nullptr) {
        if(currentEntry->file == file && currentEntry->pageNo == pageNo) {
            return HASHTBLERROR;
            //already exists so error
        }

        currentEntry = currentEntry->next;
    }

    hashBucket* newEntry = new hashBucket();
    //it wouldn't let me just do = file due to const issues
    newEntry->file = const_cast<File*>(file); 
    newEntry->pageNo = pageNo;
    newEntry->frameNo = frameNo;
    newEntry->next = ht[currentIndex];
    ht[currentIndex] = newEntry;

    return OK;



}

Status BufHashTbl::lookup(const File* file, const int pageNo, int& frameNo)  {
    int currentIndex = hash(file, pageNo);
    hashBucket* currentEntry = ht[currentIndex];

    while(currentEntry != nullptr) {
        if(currentEntry->file == file && currentEntry->pageNo == pageNo) {
            frameNo = currentEntry->frameNo;
            return OK;
            //spec is confusing, do we return frameNo or OK
        }
        currentEntry = currentEntry->next;
    }

    return HASHNOTFOUND;
}

Status BufHashTbl::remove(const File* file, const int pageNo) {
    int currentIndex = hash(file, pageNo);
    hashBucket* currentEntry = ht[currentIndex];
    hashBucket* prev = nullptr;

    while(currentEntry != nullptr) {
        if(currentEntry->file == file && currentEntry->pageNo == pageNo) {
            if(prev == nullptr) {
                ht[currentIndex] = currentEntry->next;
            } else {
                prev->next = currentEntry->next;
            }
            delete currentEntry;
            return OK;
        }

        prev = currentEntry;
        currentEntry = currentEntry->next;
    }

    return HASHNOTFOUND;

}

Status BufHashTbl::lookup(const File* file, const int pageNo, int& frameNo)  {
    int currentIndex = hash(file, pageNo);
    hashBucket* currentEntry = ht[currentIndex];

    while(currentEntry != nullptr) {
        if(currentEntry->file == file && currentEntry->pageNo == pageNo) {
            frameNo = currentEntry->frameNo;
            return OK;
            //spec is confusing, do we return frameNo or OK
        }
        currentEntry = currentEntry->next;
    }

    return HASHNOTFOUND;
}

Status BufHashTbl::remove(const File* file, const int pageNo) {
    int currentIndex = hash(file, pageNo);
    hashBucket* currentEntry = ht[currentIndex];
    hashBucket* prev = nullptr;

    while(currentEntry != nullptr) {
        if(currentEntry->file == file && currentEntry->pageNo == pageNo) {
            if(prev == nullptr) {
                ht[currentIndex] = currentEntry->next;
            } else {
                prev->next = currentEntry->next;
            }
            delete currentEntry;
            return OK;
        }

        prev = currentEntry;
        currentEntry = currentEntry->next;
    }

    return HASHNOTFOUND;

}


// Bufdesc class
class BufDesc {
    friend class BufMgr; 
private:
    File* file;     
    int pageNo;     
    int frameNo;    
    int pinCnt;     
    bool dirty;     
    bool valid;     
    bool refbit;    

public:
    BufDesc() { 
        Clear(); 
    }

    void Clear() {
        pinCnt = 0;
        file = NULL;
        pageNo = -1;
        dirty = false;
        refbit = false;
        valid = false;
    }

    void Set(File* filePtr, int pageNum) {
        file = filePtr;
        pageNo = pageNum;
        pinCnt = 1;
        dirty = false;
        refbit = true;
        valid = true;
    }
};







BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

const Status BufMgr::allocBuf(int &frame) {
    int startingPoint = clockHand;
    bool foundFrame = false;

    while (!foundFrame) {
        advanceClock();
        BufDesc &bufDesc = bufTable[clockHand];

        // Check if this frame is free to use
        if (!bufDesc.valid || bufDesc.pinCnt == 0) {
            if (bufDesc.dirty) {
                // If the frame is dirty, flush it before replacing
                Status status = bufDesc.file->writePage(bufDesc.pageNo, &bufPool[clockHand]);
                if (status != OK) return status;
                bufDesc.dirty = false;
            }

            // Clear buffer entry and make it available
            bufDesc.Clear();
            frame = clockHand;
            foundFrame = true;
        }

        // Stop if we've completed a full rotation without finding a frame
        if (clockHand == startingPoint) return BUFFERFULL;
    }

    return OK;
}

const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status == OK) {
        // Page found in buffer pool
        BufDesc &bufDesc = bufTable[frameNo];
        bufDesc.pinCnt++;
        page = &bufPool[frameNo];
    } else if (status == NOMOREMEMORY) {
        // Page not in buffer, allocate a new frame for it
        Status allocStatus = allocBuf(frameNo);
        if (allocStatus != OK) return allocStatus;

        // Read page from file into allocated frame
        Status readStatus = file->readPage(PageNo, &bufPool[frameNo]);
        if (readStatus != OK) return readStatus;

        // Update buffer metadata
        hashTable->insert(file, PageNo, frameNo);
        bufTable[frameNo].Set(file, PageNo);
        bufTable[frameNo].pinCnt = 1;
        page = &bufPool[frameNo];
    } else {
        return status;
    }

    return OK;
}

const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK) return status;

    BufDesc &bufDesc = bufTable[frameNo];

    // Check if the page is already unpinned
    if (bufDesc.pinCnt == 0) return PAGENOTPINNED;

    // Decrement pin count and set dirty flag if needed
    bufDesc.pinCnt--;
    if (dirty) bufDesc.dirty = true;

    return OK;
}

const Status BufMgr::allocPage(File* file, int& PageNo, Page*& page) {
    // Allocate a new page in the file
    Status status = file->allocatePage(PageNo);
    if (status != OK) return status;

    // Allocate a buffer for the new page
    int frameNo;
    status = allocBuf(frameNo);
    if (status != OK) return status;

    // Initialize page in buffer
    memset(&bufPool[frameNo], 0, sizeof(Page));
    hashTable->insert(file, PageNo, frameNo);
    bufTable[frameNo].Set(file, PageNo);
    bufTable[frameNo].pinCnt = 1;
    page = &bufPool[frameNo];

    return OK;
}

const Status BufMgr::flushFile(File* file) {
    for (int i = 0; i < numBufs; i++) {
        BufDesc &bufDesc = bufTable[i];
        
        // Check if the frame is valid and belongs to the specified file
        if (bufDesc.valid && bufDesc.file == file) {
            if (bufDesc.pinCnt > 0) return PAGEPINNED;

            if (bufDesc.dirty) {
                Status status = bufDesc.file->writePage(bufDesc.pageNo, &bufPool[i]);
                if (status != OK) return status;
                bufDesc.dirty = false;
            }

            // Remove the page from hashTable and mark frame as invalid
            hashTable->remove(file, bufDesc.pageNo);
            bufDesc.Clear();
        }
    }

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


