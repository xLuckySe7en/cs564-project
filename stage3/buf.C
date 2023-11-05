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
#include <utility>

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

template <typename T> struct Range {
  const T left, right;
  struct Iterator {
    T cur;
    const T end;
    bool valid() const { return cur < end; }
    void next() { cur++; }
  };
  Iterator iter() const { return Iterator{left, right}; }
};
enum IterStat {
  ITER_STOP, // early return
  ITER_CONT, // continue iterating
  ITER_END,  // loop ended
};
// range over T
// iteration function F: takes T, returns R
template <typename T, typename F, typename R>
std::pair<IterStat, R> ForEach(Range<T> rng, F iter, const R normal_end_ret) {
  for (auto i = rng.iter(); i.valid(); i.next()) {
    auto pr = iter(i.cur);
    if (pr.first == ITER_STOP) {
      return pr;
    }
  }
  return std::make_pair(ITER_END, normal_end_ret);
}
template <typename GetFile, typename GetPageNo, typename GetFrameNo,
          typename GetPin, typename GetDirty, typename GetValid,
          typename GetRef, typename CallClear>
class ClockIter {
private:
  GetFile getFile;
  GetPageNo getPageNo;
  GetFrameNo getFrameNo;
  GetPin getPin;
  GetDirty getDirty;
  GetValid getValid;
  GetRef getRef;
  CallClear callClear;

  const int frames;
  unsigned int &hand;
  BufDesc *bufDescs;
  BufHashTbl *bufHash;
  BufStats &bufStats;
  Page *pool;

  int &ret;

public:
  ClockIter(GetFile getFile, GetPageNo getPageNo, GetFrameNo getFrameNo,
            GetPin getPin, GetDirty getDirty, GetValid getValid, GetRef getRef,
            CallClear callClear, int frames, unsigned int &hand,
            BufDesc *bufDescs, BufHashTbl *bufHash, BufStats &bufStats,
            Page *pool, int &ret)
      : getFile(getFile), getPageNo(getPageNo), getFrameNo(getFrameNo),
        getPin(getPin), getDirty(getDirty), getValid(getValid), getRef(getRef),
        callClear(callClear), frames(frames), hand(hand), bufDescs(bufDescs),
        bufHash(bufHash), bufStats(bufStats), pool(pool), ret(ret) {}
  std::pair<IterStat, Status> operator()(int) {
    // try allocate the current frame
    BufDesc &bd = bufDescs[hand++];
    // advance clock hand
    hand %= frames;

    // Only check for valid page. Invalid ones can be directly used.
    if (getValid(bd)) {
      // a pinned page (currently being used) do not evict it
      if (getPin(bd) != 0) {
        // continue iteration: try next slot
        return std::make_pair(ITER_CONT, OK);
      }
      // a recently used page, give it a second chance
      if (getRef(bd)) {
        getRef(bd) = false;
        // continue iteration: try next slot
        return std::make_pair(ITER_CONT, OK);
      }

      // past this line: this slot is available

      // on evicting a dirty page: write back to disk
      if (getDirty(bd)) {
        getDirty(bd) = false;
        ERR_RET(getFile(bd)->writePage(getPageNo(bd), pool + getFrameNo(bd)) !=
                    OK,
                std::make_pair(ITER_STOP, UNIXERR));
        bufStats.accesses++;
        bufStats.diskwrites++;
        bufStats.accesses++;
      }
      // hash table maintenance
      ASSERT(bufHash->remove(getFile(bd), getPageNo(bd)) == OK);
    }
    // prepare the buffer description
    ret = getFrameNo(bd);
    callClear(bd);
    // stop iterator, we've prepared a buffer slot
    return std::make_pair(ITER_STOP, OK);
  }
};

// \@brief: Use the clock algorithm to allocate a frame in the frame buffer pool
// \@parm: The allocated frame number is passed through the frame reference.
// \@error:
// BUFFEREXCEEDED: if all pages in the buffer are pinned
// UNIXERR: if when error occurred when write back dirty page
const Status BufMgr::allocBuf(int &frame) {
  // run for 2 rounds is sufficient to allocate a frame if it is possible.
  return ForEach(
             Range<int>{0, 2},
             [this, &frame](int) {
               auto getFile = [](BufDesc &bd) { return bd.file; };
               auto getPageNo = [](BufDesc &bd) { return bd.pageNo; };
               auto getFrameNo = [](BufDesc &bd) { return bd.frameNo; };
               auto getPin = [](BufDesc &bd) { return bd.pinCnt; };
               auto getDirty = [](BufDesc &bd) -> bool & { return bd.dirty; };
               auto getValid = [](BufDesc &bd) { return bd.valid; };
               auto getRef = [](BufDesc &bd) -> bool & { return bd.refbit; };
               auto callClear = [](BufDesc &bd) { return bd.Clear(); };

               return ForEach(Range<int>{0, numBufs},
                              ClockIter<decltype(getFile), decltype(getPageNo),
                                        decltype(getFrameNo), decltype(getPin),
                                        decltype(getDirty), decltype(getValid),
                                        decltype(getRef), decltype(callClear)>(
                                  getFile, getPageNo, getFrameNo, getPin,
                                  getDirty, getValid, getRef, callClear,
                                  this->numBufs, this->clockHand,
                                  this->bufTable, this->hashTable,
                                  this->bufStats, this->bufPool, frame),
                              OK);
             },
             BUFFEREXCEEDED)
      .second;
}

const Status BufMgr::readPage(File *file, const int pageNo, Page *&page) {
  Status status;
  int frameNo;

  // First, check if the page is already in the buffer pool by looking it up in
  // the hash table.
  status = hashTable->lookup(file, pageNo, frameNo);

  if (status == OK) {
    // If found, point the page pointer to the appropriate page in the buffer
    // pool.
    page = &bufPool[frameNo];

    // Increment the pin count for the page as it is now being used.
    bufTable[frameNo].pinCnt++;

    // Mark the reference bit as true, indicating that the page has been
    // accessed.
    bufTable[frameNo].refbit = true;

    return OK;
  } else if (status == HASHNOTFOUND) {
    // If the page is not in the buffer pool, allocate a frame for it.
    status = allocBuf(frameNo);
    if (status != OK) {
      // If unable to allocate a buffer, return the error (could be
      // BUFFEREXCEEDED or UNIXERR).
      return status;
    }

    // Load the page into the buffer pool frame.
    status = file->readPage(pageNo, &bufPool[frameNo]);
    if (status != OK) {
      // If reading the page fails, return the error.
      return status;
    }

    // Point the page pointer to the new page in the buffer pool.
    page = &bufPool[frameNo];

    // Update the buffer descriptor for the new page.
    bufTable[frameNo].Set(file, pageNo);

    // Insert the page into the hash table so it can be found next time.
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK) {
      // If insertion fails, return the error.
      return status;
    }
    return OK;
  } else {
    // If the lookup returned an error other than HASHNOTFOUND, return that
    // error.
    return status;
  }
}

const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty) {
  int frameNo;
  Status status =
      hashTable->lookup(file, PageNo, frameNo); // Lookup the frame number
  if (status != OK) {
    // Handle the error, e.g., if the page is not found in the hash table.
    return status;
  }

  // Assuming lookup was successful, we now have the frame number in frameNo.
  BufDesc *bufDesc =
      &bufTable[frameNo]; // Get the buffer descriptor for the frame.

  // Check if the frame is actually pinned.
  if (bufDesc->pinCnt <= 0) {
    return PAGENOTPINNED; // or some other appropriate error handling
  }

  // Decrement the pin count as the page is being unpinned.
  bufDesc->pinCnt--;

  // If the dirty parameter is true, set the dirty bit for the frame.
  if (dirty) {
    bufDesc->dirty = true;
  }

  return OK;
}

/*
    @param: file    A file pointer that we will use to allocate teh page into
    @param pageNo   A reference int that we will return the page number of the
   newly alloced page
    @param page     A pinter to the newly alloced page
    @return A status indicating whether the operation finished successfully (OK)
   or had some error associated with it (UNIXERROR/HASHTBLERROR/BUFFEREXCEEDED)
*/
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page) {
  if (file == nullptr)
    return UNIXERR;
  auto status = file->allocatePage(pageNo);
  if (status != OK)
    return status;

  int frameNo = -1;
  // obtain a buffer pool frame
  status = allocBuf(frameNo);
  if (status != OK)
    return status;

  // an entry is inserted into the hash table
  if (hashTable == nullptr)
    return HASHTBLERROR;

  status = hashTable->insert(file, pageNo, frameNo);
  if (status != OK)
    return status;

  // The BufDesc class is used to keep track of the state of each frame in the
  // buffer pool. thus we have to update the correct frame in the buff table
  if (bufTable == nullptr)
    return UNIXERR;

  bufTable[frameNo].Set(file, pageNo);
  // iffy on this line
  page = &bufPool[frameNo];
  return OK;
}

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
