/**
* Names: Sreya Sarathy, Harshet Anand, Abhishek Koul
* Student IDs respectively: 9083114760, 9083536384, 9083645797
* Purpose of the file: This file implements the buffer manager (BufMgr) class
*  This is responsible for managing the buffer pool, which temporarily stores pages read from the
* disk to improve performance. The functions implemented by us are allocPage, allocBuf, readPage and unPinPage.
* */

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
                      cerr << "At line " << _LINE_ << ":" << endl << "  "; \
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




/**
This function is meant to allocate a buffer frame for a new page using the clock system that was outlined in the spec
It looks at existing frames and checks if they are dirty or not to determine if they are replaceable
Once the correct frame is found, then the code breaks and the
Output is either OK if a frame that can be used was found
or if a frame wasn't found and we have gone through all options then BUFFEREXCEEDED is returned or UNIXERR if unix error happens
The paramater(input) given is a address reference to where the frame is supposed to be.
*/
const Status BufMgr::allocBuf(int &frame) {
   int start = clockHand;
   bool correctFrame = false;
   while (true) {
       //uses clock algo here thru advanceClock
       advanceClock();
       BufDesc &bufDesc = bufTable[clockHand];
       if (!bufDesc.valid || bufDesc.pinCnt == 0) {
           if (bufDesc.valid) {
               if (bufDesc.dirty) {
                   Status status = bufDesc.file->writePage(bufDesc.pageNo, &bufPool[clockHand]);
                   if (status != OK) {
                       return UNIXERR;
                   }
                   //not dirty anymore cuz it's been write
                   bufDesc.dirty = false;
               }
             
               hashTable->remove(bufDesc.file, bufDesc.pageNo);     
           }
           bufDesc.Clear();
           frame = clockHand;
           correctFrame = true;
           break;
       }
       if (clockHand == start) {
           return BUFFEREXCEEDED;
       }
   }
   return OK;
}


/*
* The following function readPage retrieves a page from the buffer pool if it is
* already loaded. If not, the page is read from the disk into a newly allocated buffer frame.
* The function first checks if the requested page is already in the buffer pool by using a hashtable
* lookup method. If it is found, it then increments the pin count and sets the reference bit to indicate
* recent access. If not found, a new buffer frame is allocated and the page is read from the disk.
* Then, the hash table is updated.
* Inputs:
* File: A pointer to the File object from which the page is being read.
* PageNo: The page number to be retrieved.
* page: A reference to a pointer that will point to the page in the buffer.
* Outputs:
* page: If the page is found or successfully read, this will point to the page in the buffer pool.
* */

const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
   int frameNum;
   // to check if the page is already in the buffer pool.
   Status status = hashTable->lookup(file, PageNo, frameNum);
 
   if (status == OK) {
       // if page found in buffer pool, update reference and pin count.
       BufDesc &bufDesc = bufTable[frameNum];
       bufDesc.refbit = true;
       bufDesc.pinCnt++;
       page = &bufPool[frameNum];
   } else {
       // if page not found then allocate a new buffer frame.
       Status allocStatus = allocBuf(frameNum);
       if (allocStatus != OK) {
           return allocStatus;
       }

       if(allocStatus == BUFFEREXCEEDED) {
           return BUFFEREXCEEDED;
       }

        // read the page from disk into allocated buffer frame.
       Status readStatus = file->readPage(PageNo, &bufPool[frameNum]);
     
       if (readStatus != OK) {
           return UNIXERR;
       }

       // now, insert the page into the hash table for future lookups.
       if(hashTable->insert(file, PageNo, frameNum) != OK) {
           return HASHTBLERROR;
       }
     
       // setting up the buffer frame with the new page.
       bufTable[frameNum].Set(file, PageNo);
       bufTable[frameNum].pinCnt = 1;
       bufTable[frameNum].refbit = true;
       page = &bufPool[frameNum];
   }
   return OK;
}

/**
* Unpins a page in the buffer pool, marking it as unpinned and updating its status if needed.
*
* file - A pointer to the File object representing the file containing the page.
* PageNo - The page number of the page that needs to be unpinned.
* dirty - A boolean flag indicating whether the page has been modified where it outputs "true" if modified and "false" if not modified.
*
* Returns a status code indicating the success or failure of the operation:
* - OK: The page was successfully unpinned.
* - HASHNOTFOUND: The page was not found in the hash table.
* - PAGENOTPINNED: The page was not pinned in the buffer.
*
* The function unpins a page if it is currently pinned in the buffer pool. If the page has been marked as
* dirty, it will be updated in the buffer descriptor. The function decreases the pin count for the page, and if
* the pin count reaches zero, the page is considered unpinned.
*/

const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {
   int frameNum;
   Status status = hashTable->lookup(file, PageNo, frameNum);

   // Check if page exists in the hash table
   if (status != OK) return HASHNOTFOUND;

   BufDesc &bufDesc = bufTable[frameNum];

   // Check if page is already unpinned
   if (bufDesc.pinCnt == 0) return PAGENOTPINNED;

   // Update the dirty flag if necessary
   if (dirty) bufDesc.dirty = true;
 
   bufDesc.pinCnt--;

   return OK;
}


/**
* Creates a new page and puts it into the buffer, it puts an empty page in first and then once the buffer frame is retrived
* then it is inserted into the hashtable and then put into the buffer table. Pin Count is set to 1 because the page is currently in use
* Inputs are file which is where the page is inserted into, pageNo is the page number meant for the new allocated page
* and page is the pointer to the buffer frame where the page specified by page number is
* Output is OK for no errors, BUFFEREXCEEDEDE if all buffer frames have been used, UNIXERR if a unix error happens and HASHTBLERROR if the insert fails
*/

const Status BufMgr::allocPage(File* file, int& PageNo, Page*& page) {
   Status status = file->allocatePage(PageNo);
   if (status != OK) {
       return UNIXERR;
   }


   int frameNum;
   status = allocBuf(frameNum);

   if(status == BUFFEREXCEEDED) {
       return BUFFEREXCEEDED;
   }

   if (status != OK) {
       return status;
   }

   memset(&bufPool[frameNum], 0, sizeof(Page));


   if(hashTable->insert(file, PageNo, frameNum) != OK) {
       return HASHTBLERROR;
   }

   bufTable[frameNum].Set(file, PageNo);
   bufTable[frameNum].pinCnt = 1;
   page = &bufPool[frameNum];

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