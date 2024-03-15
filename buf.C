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


/** Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. 
 * Returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an error when a dirty page was being written to disk and OK otherwise.  
 * This private method will get called by the readPage() and allocPage() methods described below.
 * Make sure that if the buffer frame allocated has a valid page in it, that you remove the appropriate entry from the hash table. 
*/
const Status BufMgr::allocBuf(int &frame) {
    int pinCount = 0;

    while (true){ // some condition here
        if (clockHand == 0) {
            printf("PINCOUNT RESET\n");
            pinCount = 0;
        }

        clockHand = (clockHand + 1) % numBufs;
        

        BufDesc* bd = &bufTable[clockHand];

        if (bd->valid){
            if (bd->refbit){
                bd->refbit = 0;
                continue;
            }
            else{
                if (bd->pinCnt > 0){
                    pinCount++;
                    printf("%d\n", pinCount);
                    if (pinCount >= numBufs) return BUFFEREXCEEDED;
                    continue;
                }

                if (bd->dirty){
                    // flush to disk
                    Status status = bd->file->writePage(bd->pageNo, &(bufPool[clockHand]));
                    if (status != OK) {
                        return UNIXERR; // Error writing page to disk
                    }
                }

                if (bd->file != nullptr && bd->pageNo != -1) {
                    hashTable->remove(bd->file, bd->pageNo);
                }

            }
        }
        // Set
        // bd->Set(bd->file, bd->pageNo);
        bd->Clear();
        // Use Frame
        frame = clockHand;

        return OK;

    }
}


// const Status BufMgr::allocBuf(int &frame) {
//     int numAttempts = 0;

//     while (numAttempts < numBufs) {
//         // Step 1: Advance the clock hand pointer
//         clockHand = (clockHand + 1) % numBufs;
        
//         BufDesc *bufDesc = &bufTable[clockHand];

//         // Step 2: Check if the valid bit is set
//         if (bufDesc->valid) {
//             // Step 3: Check if the refBit is set
//             if (bufDesc->refbit) {
//                 // Step 4: Set reference bit to false
//                 bufDesc->refbit = false;
//             } else {
//                 // Step 5: Check if the page is pinned
//                 if (bufDesc->pinCnt > 0) {
//                     // Restart from Step 1
//                     continue;
//                 }
                
//                 // Step 6: Check if the dirty bit is set
//                 if (bufDesc->dirty) {
//                     // Step 7: Flush page to disk
//                     Status status = bufDesc->file->writePage(bufDesc->pageNo, &(bufPool[clockHand]));
//                     if (status != OK) {
//                         return UNIXERR; // Error writing page to disk
//                     }
//                 }

//                 // Step 8: Remove entry from hash table if valid page exists
//                 if (bufDesc->file != nullptr && bufDesc->pageNo != -1) {
//                     hashTable->remove(bufDesc->file, bufDesc->pageNo);
//                 }
                
//                 // Step 9: Reset buffer descriptor
//                 bufDesc->Clear();
                
//                 // Step 10: Set the frame and return
//                 frame = clockHand;
//                 return OK;
//             }
//         } else {
//             // Step 9: Reset buffer descriptor
//             bufDesc->Clear();
            
//             // Step 10: Set the frame and return
//             frame = clockHand;
//             return OK;
//         }

//         numAttempts++;
//     }

//     return BUFFEREXCEEDED; // All buffer frames are pinned
// }

/**
 * First check whether the page is already in the buffer pool by invoking the lookup() method on the hashtable to get a frame number.  
 * There are two cases to be handled depending on the outcome of the lookup() call:
 * 
 * Case 1)  Page is not in the buffer pool.  
 *          Call allocBuf() to allocate a buffer frame and then call the method file->readPage() to read the page from disk into the buffer pool frame. 
 *          Next, insert the page into the hashtable. 
 *          Finally, invoke Set() on the frame to set it up properly. 
 *          Set() will leave the pinCnt for the page set to 1.  
 *          Return a pointer to the frame containing the page via the page parameter.
 * 
 * Case 2)  Page is in the buffer pool.  
 *          In this case set the appropriate refbit, increment the pinCnt for the page, and then return a pointer to the frame containing the page via the page parameter.
 * 
 * Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table error occurred. 
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
    int frameNo;

    // Check if (file, PageNo) is currently in the buffer pool
    Status lookupStatus = hashTable->lookup(file, PageNo, frameNo);

    // Case 1: Page is not in the buffer pool
    if (lookupStatus == HASHNOTFOUND) {
        // Allocate a buffer frame
        Status allocStatus = allocBuf(frameNo);
        if (allocStatus != OK) {
            return allocStatus; // BUFFEREXCEEDED or UNIXERR
        }

        // Read the page from disk into the buffer pool frame
        Status readStatus = file->readPage(PageNo, &(bufPool[frameNo]));
        if (readStatus != OK) {
            return readStatus; // UNIXERR
        }

        // Insert the page into the hashtable
        Status insertStatus = hashTable->insert(file, PageNo, frameNo);
        if (insertStatus != OK) {
            return insertStatus; // HASHTBLERROR
        }

        // Set up the frame properly
        bufTable[frameNo].Set(file, PageNo);

        // Return a pointer to the frame containing the page
        page = &(bufPool[frameNo]);

        return OK;
    }

    // Case 2: Page is in the buffer pool
    // Set the appropriate refbit
    bufTable[frameNo].refbit = true;
    // Increment the pinCnt for the page
    bufTable[frameNo].pinCnt++;

    // Return a pointer to the frame containing the page
    page = &(bufPool[frameNo]);

    return OK;
}


/**
 * Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.  
 * Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table, PAGENOTPINNED if the pin count is already 0. 
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
    int frameNo;
    Status lookup = hashTable->lookup(file, PageNo, frameNo);

    if (lookup == HASHNOTFOUND) return HASHNOTFOUND;
    else if (bufTable[frameNo].pinCnt == 0) return PAGENOTPINNED;
    else {
        BufDesc frame = bufTable[frameNo];
        frame.pinCnt--;
        if (dirty) frame.dirty = 1;
        return OK;
    }
}

/**
 * This call is kind of weird. 
 * The first step is to to allocate an empty page in the specified file by invoking the file->allocatePage() method. 
 * This method will return the page number of the newly allocated page.  
 * Then allocBuf() is called to obtain a buffer pool frame. 
 * Next, an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly.  
 * The method returns both the page number of the newly allocated page to the caller via the pageNo parameter and a pointer to the buffer frame allocated for the page via the page parameter. 
 * Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred.  
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status allocPageStat = file->allocatePage(pageNo);
    if (allocPageStat != OK) return allocPageStat;

    int frameNo;

    Status allocFrame = allocBuf(frameNo);
    if (allocFrame != OK) return allocFrame;

    Status htInsert = hashTable->insert(file, pageNo, frameNo);
    if (htInsert == HASHTBLERROR) return HASHTBLERROR;

    bufTable[frameNo].Set(file, pageNo);

    page = &bufPool[frameNo];

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

// TODO: do we have to call Clear() on the page frame?

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


