#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"
#include "error.h"

#define ASSERT(c)  { if (!(c)) { \
                       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
                     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs) {
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize);

    clockHand = bufs - 1;
}

BufMgr::~BufMgr() {
    for (int i = 0; i < numBufs; i++) {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid && tmpbuf->dirty) {
            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

//----------------------------------------
// Buffer Allocation
//----------------------------------------
const Status BufMgr::allocBuf(int &frame) {
    int startingPoint = clockHand;
    bool foundFrame = false;

    while (!foundFrame) {
        advanceClock();
        BufDesc &bufDesc = bufTable[clockHand];

        if (!bufDesc.valid || bufDesc.pinCnt == 0) {
            if (bufDesc.dirty) {
                Status status = bufDesc.file->writePage(bufDesc.pageNo, &bufPool[clockHand]);
                if (status != OK) return status;
                bufDesc.dirty = false;
            }
            bufDesc.Clear();
            frame = clockHand;
            foundFrame = true;
            cout << "Allocated buffer frame: " << frame << " for new page" << endl;
        }

        if (clockHand == startingPoint) return BUFFEREXCEEDED;
    }

    return OK;
}

//----------------------------------------
// Read Page
//----------------------------------------
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
    cout << "Reading page: File=" << file << ", PageNo=" << PageNo << endl;
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status == OK) {
        BufDesc &bufDesc = bufTable[frameNo];
        bufDesc.pinCnt++;
        page = &bufPool[frameNo];
        cout << "Page found in buffer pool. FrameNo=" << frameNo << ", PinCnt=" << bufDesc.pinCnt << endl;
    } else if (status == BUFFEREXCEEDED) {
        Status allocStatus = allocBuf(frameNo);
        if (allocStatus != OK) return allocStatus;

        Status readStatus = file->readPage(PageNo, &bufPool[frameNo]);
        if (readStatus != OK) return readStatus;

        hashTable->insert(file, PageNo, frameNo);
        bufTable[frameNo].Set(file, PageNo);
        bufTable[frameNo].pinCnt = 1;
        page = &bufPool[frameNo];
        cout << "Page loaded into buffer. File=" << file << ", PageNo=" << PageNo << ", FrameNo=" << frameNo << endl;
    } else {
        return status;
    }

    return OK;
}

//----------------------------------------
// Unpin Page
//----------------------------------------
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK) return status;

    BufDesc &bufDesc = bufTable[frameNo];

    if (bufDesc.pinCnt == 0) return PAGENOTPINNED;

    cout << "Unpinning page: File=" << file << ", PageNo=" << PageNo 
         << ", Dirty=" << dirty << ", PinCnt=" << bufDesc.pinCnt << endl;

    if (dirty) {
        bufDesc.dirty = true;
        cout << "Page marked as dirty: File=" << file << ", PageNo=" << PageNo << endl;
    }

    bufDesc.pinCnt--;

    return OK;
}

//----------------------------------------
// Allocate Page
//----------------------------------------
const Status BufMgr::allocPage(File* file, int& PageNo, Page*& page) {
    Status status = file->allocatePage(PageNo);
    if (status != OK) return status;

    int frameNo;
    status = allocBuf(frameNo);
    if (status != OK) return status;

    memset(&bufPool[frameNo], 0, sizeof(Page));
    hashTable->insert(file, PageNo, frameNo);
    bufTable[frameNo].Set(file, PageNo);
    bufTable[frameNo].pinCnt = 1;
    page = &bufPool[frameNo];

    cout << "Allocated new page: File=" << file << ", PageNo=" << PageNo << ", FrameNo=" << frameNo << endl;

    return OK;
}

//----------------------------------------
// Flush File
//----------------------------------------
const Status BufMgr::flushFile(const File* file) {
    for (int i = 0; i < numBufs; i++) {
        BufDesc &bufDesc = bufTable[i];

        if (bufDesc.valid && bufDesc.file == file) {
            if (bufDesc.pinCnt > 0) return PAGEPINNED;

            if (bufDesc.dirty) {
                Status status = bufDesc.file->writePage(bufDesc.pageNo, &bufPool[i]);
                if (status != OK) return status;
                bufDesc.dirty = false;
            }

            hashTable->remove(file, bufDesc.pageNo);
            bufDesc.Clear();
        }
    }

    return OK;
}

//----------------------------------------
// Print Buffer State
//----------------------------------------
void BufMgr::printSelf(void) {
    BufDesc* tmpbuf;

    cout << endl << "Printing buffer status...\n";
    for (int i = 0; i < numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << "Frame " << i << ": PageNo=" << tmpbuf->pageNo 
             << ", PinCnt=" << tmpbuf->pinCnt << ", Dirty=" << tmpbuf->dirty
             << ", Valid=" << tmpbuf->valid << endl;
    }
}
