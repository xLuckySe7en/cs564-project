#include <string>
#include "page.h"
#include "db.h"
#include "heapfile.h"
#include "error.h"
#include <cstring>

#define ERR_RET(err_cond, res, clean_up)                                       \
  do                                                                           \
    if ((err_cond)) {                                                          \
      clean_up;                                                                \
      return res;                                                              \
    }                                                                          \
  while (0)

// initialize the heap file header for a newly created file
static inline void initFileHdr(const string &fn, FileHdrPage *hdr, int dataPN) {
  memcpy(hdr->fileName, fn.c_str(), fn.length() + 1);
  hdr->recCnt = 0;
  hdr->pageCnt = 1;
  hdr->firstPage = hdr->lastPage = dataPN;
}

// routine to create a heapfile
const Status createHeapFile(const string fileName) {
  Status status;
  File *file;
  // test if a file with the provided `fileName` already exists.
  ERR_RET((status = db.openFile(fileName, file)) == OK, FILEEXISTS, {});
  // safe to create a heapfile now
  ERR_RET((status = db.createFile(fileName)) != OK, status, {});
  ERR_RET((status = db.openFile(fileName, file)) != OK, status,
          { db.destroyFile(fileName); });

  // allocate an empty header page and data page.
  int hdrPN;
  Page *hdrPage;
  ERR_RET((status = bufMgr->allocPage(file, hdrPN, hdrPage)) != OK, status, {
    db.closeFile(file);
    db.destroyFile(fileName);
  });
  int dataPN;
  Page *dataPage;
  ERR_RET((status = bufMgr->allocPage(file, dataPN, dataPage)) != OK, status, {
    // unpin the header page so that it can be removed
    bufMgr->unPinPage(file, hdrPN, false);
    db.closeFile(file);
    db.destroyFile(fileName);
  });
  dataPage->init(2);
  initFileHdr(fileName, (FileHdrPage *)hdrPage, dataPN);
  // done, both the header page and the data page are modified,
  // so mark them dirty
  bufMgr->unPinPage(file, hdrPN, true);
  bufMgr->unPinPage(file, dataPN, true);
  return OK;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName) {
  return (db.destroyFile(fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus) {
  Status status;
  headerPage = NULL, curPage = NULL;
  curRec = NULLRID;

  cout << "opening file " << fileName << endl;

  // open the file and read in the header page and the first data page
  if ((status = db.openFile(fileName, filePtr)) != OK) {
    cerr << "open of heap file failed\n";
    returnStatus = status;
    return;
  }
  // find the heap file header page number, it is the first page of this file.
  if ((status = filePtr->getFirstPage(headerPageNo)) != OK) {
    cerr << "cannot get the header page number";
    returnStatus = status;
    return;
  }
  // header page, should be pinned upon readPage returns.
  Page *pagePtr;
  if ((status = bufMgr->readPage(filePtr, headerPageNo, pagePtr)) != OK) {
    cerr << "cannot read the header page";
    returnStatus = status;
    return;
  }
  headerPage = (FileHdrPage *)pagePtr, hdrDirtyFlag = false;
  // data page, should be pinned upon readPage returns.
  curPageNo = headerPage->firstPage;
  if ((status = bufMgr->readPage(filePtr, curPageNo, pagePtr)) != OK) {
    cerr << "cannot read the header page";
    returnStatus = status;
    return;
  }
  curPage = pagePtr, curDirtyFlag = false;
  returnStatus = OK;
}

// the destructor closes the file
HeapFile::~HeapFile() {
  Status status;
  cout << "invoking heapfile destructor on file " << headerPage->fileName
       << endl;

  // see if there is a pinned data page. If so, unpin it
  if (curPage != NULL) {
    status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    curPage = NULL;
    curPageNo = 0;
    curDirtyFlag = false;
    if (status != OK)
      cerr << "error in unpin of date page\n";
  }

  // unpin the header page
  status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
  if (status != OK)
    cerr << "error in unpin of header page\n";

  status = bufMgr->flushFile(filePtr);
  // make sure all pages of the file are flushed to disk
  if (status != OK)
    cerr << "error in flushFile call\n";
  // before close the file
  status = db.closeFile(filePtr);
  if (status != OK) {
    cerr << "error in closefile call\n";
    Error e;
    e.print(status);
    exit(-1);
  }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const { return headerPage->recCnt; }

const Status HeapFile::getRecord(const RID &rid, Record &rec) {
  Status status = OK;
  // if in current page
  if (curPage && curPageNo == rid.pageNo) {
    return curPage->getRecord(rid, rec);
  }
  // before switch to another page, cleanup
  if (curPage) {
    bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
  }
  curPageNo = -1, curPage = NULL, curDirtyFlag = false;
  // check if record id page number is valid
  ERR_RET(rid.pageNo > headerPage->pageCnt, BADRID, {});
  curPageNo = rid.pageNo;
  // try to switch to that page
  ERR_RET((status = bufMgr->readPage(filePtr, rid.pageNo, curPage)) != OK,
          status, {});
  ERR_RET((status = curPage->getRecord(rid, rec)) != OK, BADRID, {});
  return OK;
}

HeapFileScan::HeapFileScan(const string &name, Status &status)
    : HeapFile(name, status) {
  filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_, const int length_,
                                     const Datatype type_, const char *filter_,
                                     const Operator op_) {
  if (!filter_) { // no filtering requested
    filter = NULL;
    return OK;
  }

  if ((offset_ < 0 || length_ < 1) ||
      (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
      ((type_ == INTEGER && length_ != sizeof(int)) ||
       (type_ == FLOAT && length_ != sizeof(float))) ||
      (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT &&
       op_ != NE)) {
    return BADSCANPARM;
  }

  offset = offset_;
  length = length_;
  type = type_;
  filter = filter_;
  op = op_;

  return OK;
}

const Status HeapFileScan::endScan() {
  Status status;
  // generally must unpin last page of the scan
  if (curPage != NULL) {
    status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    curPage = NULL;
    curPageNo = 0;
    curDirtyFlag = false;
    return status;
  }
  return OK;
}

HeapFileScan::~HeapFileScan() { endScan(); }

const Status HeapFileScan::markScan() {
  // make a snapshot of the state of the scan
  markedPageNo = curPageNo;
  markedRec = curRec;
  return OK;
}

const Status HeapFileScan::resetScan() {
  Status status;
  if (markedPageNo != curPageNo) {
    if (curPage != NULL) {
      status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
      if (status != OK)
        return status;
    }
    // restore curPageNo and curRec values
    curPageNo = markedPageNo;
    curRec = markedRec;
    // then read the page
    status = bufMgr->readPage(filePtr, curPageNo, curPage);
    if (status != OK)
      return status;
    curDirtyFlag = false; // it will be clean
  } else
    curRec = markedRec;
  return OK;
}

static inline bool operator==(const RID &l, const RID &r) {
  return l.pageNo == r.pageNo && l.slotNo == r.slotNo;
}
static inline bool operator!=(const RID &l, const RID &r) { return !(l == r); }

const Status HeapFileScan::scanNext(RID &outRid) {
  outRid = NULLRID;
  // no page? it must mean that we've reached the end
  if (curPage == NULL) {
    return FILEEOF;
  }
  // invariant: current page is pinned

  // see if we still have unchecked records on this page
  bool endOfPage;
  if (curRec == NULLRID) {
    endOfPage = (curPage->firstRecord(curRec) != OK);
  } else {
    RID nextRid;
    endOfPage = (curPage->nextRecord(curRec, nextRid) != OK);
    if (!endOfPage) {
      curRec = nextRid;
    }
  }
  // try find record on this page
  while (!endOfPage) {
    Record rec;
    curPage->getRecord(curRec, rec);
    if (matchRec(rec)) {
      outRid = curRec;
      return OK;
    }
    RID nextRid;
    if (curPage->nextRecord(curRec, nextRid) == OK) {
      curRec = nextRid;
    } else {
      endOfPage = true;
    }
  }
  // this page is exhausted, we must go to the next page
  bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
  curPage->getNextPage(curPageNo);
  curPage = NULL;
  bufMgr->readPage(filePtr, curPageNo, curPage);
  curDirtyFlag = false;
  curRec = NULLRID;
  // recursively scan next page
  return scanNext(outRid);
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec) {
  return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord() {
  Status status;

  // delete the "current" record from the page
  status = curPage->deleteRecord(curRec);
  curDirtyFlag = true;

  // reduce count of number of records in the file
  headerPage->recCnt--, hdrDirtyFlag = true;
  return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty() {
  curDirtyFlag = true;
  return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const {
  // no filtering requested
  if (!filter)
    return true;

  // see if offset + length is beyond end of record
  // maybe this should be an error???
  if ((offset + length - 1) >= rec.length)
    return false;

  float diff = 0; // < 0 if attr < fltr
  switch (type) {

  case INTEGER:
    int iattr, ifltr; // word-alignment problem possible
    memcpy(&iattr, (char *)rec.data + offset, length);
    memcpy(&ifltr, filter, length);
    diff = iattr - ifltr;
    break;

  case FLOAT:
    float fattr, ffltr; // word-alignment problem possible
    memcpy(&fattr, (char *)rec.data + offset, length);
    memcpy(&ffltr, filter, length);
    diff = fattr - ffltr;
    break;

  case STRING:
    diff = strncmp((char *)rec.data + offset, filter, length);
    break;
  }

  switch (op) {
  case LT:
    if (diff < 0.0)
      return true;
    break;
  case LTE:
    if (diff <= 0.0)
      return true;
    break;
  case EQ:
    if (diff == 0.0)
      return true;
    break;
  case GTE:
    if (diff >= 0.0)
      return true;
    break;
  case GT:
    if (diff > 0.0)
      return true;
    break;
  case NE:
    if (diff != 0.0)
      return true;
    break;
  }

  return false;
}

InsertFileScan::InsertFileScan(const string &name, Status &status)
    : HeapFile(name, status) {
  // Do nothing. Heapfile constructor will bread the header page and the first
  //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan() {
  Status status;
  // unpin last page of the scan
  if (curPage != NULL) {
    status = bufMgr->unPinPage(filePtr, curPageNo, true);
    curPage = NULL;
    curPageNo = 0;
    if (status != OK)
      cerr << "error in unpin of data page\n";
  }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid) {
  Status status;
  if (curPage == NULL) {
    curPageNo = headerPage->lastPage;
    ERR_RET((status = bufMgr->readPage(filePtr, curPageNo, curPage)) != OK,
            status, {});
  }

  // check for very large records
  // will never fit on a page, so don't even bother looking
  ERR_RET((unsigned int)rec.length > PAGESIZE - DPFIXED, INVALIDRECLEN, {});

  // if it fits in current page, we are done
  if (curPage->insertRecord(rec, outRid) == OK) {
    curDirtyFlag = true;
    headerPage->recCnt++, hdrDirtyFlag = true;
    return OK;
  }
  // invariant: current page is the last page

  // allocate a new page to accommodate the record
  Page *page;
  int pn;
  ERR_RET((status = bufMgr->allocPage(filePtr, pn, page)) != OK, status, {});
  page->init(pn);

  // append the new page onto the heap file page linked list
  curPage->setNextPage(pn), curDirtyFlag = true;
  ERR_RET((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK,
          status, {});
  headerPage->pageCnt++, headerPage->lastPage = pn, hdrDirtyFlag = true;

  // this is now the current page
  curPage = page, curPageNo = pn, curDirtyFlag = false;
  return insertRecord(rec, outRid);
}
