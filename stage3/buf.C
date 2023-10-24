#include "buf.h"
#include "error.h"
#include "page.h"
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


// if error condition is met, trigger an early return with a specified error
#define ERR_RET(cond, err)                                                     \
  do {                                                                         \
    if ((cond)) {                                                              \
      return (err);                                                            \
    }                                                                          \
  } while (0)

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
  hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

  clockHand = bufs - 1;
}

BufMgr::~BufMgr() {

  // flush out all unwritten pages
  for (int i = 0; i < numBufs; i++) {
    BufDesc *tmpbuf = &bufTable[i];
    if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
      cout << "flushing page " << tmpbuf->pageNo << " from frame " << i << endl;
#endif

      tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
    }
  }

  delete[] bufTable;
  delete[] bufPool;
  delete hashTable;
}

// \@brief: Use the clock algorithm to allocate a frame in the frame buffer pool
// \@parm: The allocated frame number is passed through the frame reference.
// \@error:
// BUFFEREXCEEDED: if all pages in the buffer are pinned
// UNIXERR: if when error occurred when write back dirty page
const Status BufMgr::allocBuf(int &frame) {
  // run for 2 rounds is sufficient to allocate a frame if it is possible.
  for (int _runs = 0; _runs < 2; _runs++) {
    for (int _i = 0; _i < numBufs; _i++) {
      BufDesc &bd = bufTable[clockHand++];
      clockHand %= numBufs;

      if (bd.valid) {
        // a pinned page (currently being used) do not evict it
        if (bd.pinCnt > 0) {
          continue;
        }
        // a recently used page, give it a second chance
        if (bd.refbit) {
          bd.refbit = false;
          continue;
        }
        // on evicting a dirty page: write back to disk
        if (bd.dirty) {
          bd.dirty = false;
          ERR_RET(bd.file->writePage(bd.pageNo, bufPool + bd.frameNo) != OK,
                  UNIXERR);
          bufStats.accesses++;
          bufStats.diskwrites++;
          bufStats.accesses++;
        }
        // hash table maintenance
        ASSERT(hashTable->remove(bd.file, bd.pageNo) == OK);
      }
      // prepare the buffer description
      frame = bd.frameNo;
      bd.Clear();
      return OK;
    }
  }
  return BUFFEREXCEEDED;
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page) {}

const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty) {

}

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page) {}

const Status BufMgr::disposePage(File *file, const int pageNo) {
  // see if it is in the buffer pool
  Status status = OK;
  int frameNo = 0;
  status = hashTable->lookup(file, pageNo, frameNo);
  if (status == OK) {
    // clear the page
    bufTable[frameNo].Clear();
  }
  status = hashTable->remove(file, pageNo);

  // deallocate it in the file
  return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file) {
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc *tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
        return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
        cout << "flushing page " << tmpbuf->pageNo << " from frame " << i
             << endl;
#endif
        if ((status = tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]))) !=
            OK)
          return status;

        tmpbuf->dirty = false;
      }

      hashTable->remove(file, tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }

  return OK;
}

void BufMgr::printSelf(void) {
  BufDesc *tmpbuf;

  cout << endl << "Print buffer...\n";
  for (int i = 0; i < numBufs; i++) {
    tmpbuf = &(bufTable[i]);
    cout << i << "\t" << (char *)(&bufPool[i])
         << "\tpinCnt: " << tmpbuf->pinCnt;

    if (tmpbuf->valid == true)
      cout << "\tvalid\n";
    cout << endl;
  };
}
