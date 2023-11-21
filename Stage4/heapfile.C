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
  File *file;
  Status status;
  FileHdrPage *hdrPage;
  int hdrPageNo;
  Page *newPage;

  // Try to open the file. This should return an error.
  status = db.openFile(fileName, file);
  if (status != OK) {
    // File doesn't exist. First create it.
    status = db.createFile(fileName);
    if (status != OK)
      return status;

    // Open the newly created file
    status = db.openFile(fileName, file);
    if (status != OK)
      return status;

    // Allocate an empty header page
    Page *hdrPage_;
    status = bufMgr->allocPage(file, hdrPageNo, hdrPage_);
    if (status != OK)
      return status;
    hdrPage = (FileHdrPage *)hdrPage_;

    // Initialize the header page values
    strcpy(hdrPage->fileName, fileName.c_str());
    hdrPage->pageCnt = 2;
    hdrPage->recCnt = 0;

    // Allocate the first data page and explicitly set its number to 2
    int firstDataPageNo = 2;
    status = bufMgr->allocPage(file, firstDataPageNo, newPage);
    if (status != OK) {
      bufMgr->unPinPage(file, hdrPageNo, true);
      return status;
    }

    newPage->init(firstDataPageNo); // Initialize data page contents

    // Update FileHdrPage with data page information
    hdrPage->firstPage = firstDataPageNo;
    hdrPage->lastPage = firstDataPageNo;

    // Unpin pages and mark them as dirty
    status = bufMgr->unPinPage(file, hdrPageNo, true);
    if (status != OK)
      return status;

    // uhpin the data page, which is also dirty
    status = bufMgr->unPinPage(file, firstDataPageNo, true);
    if (status != OK)
      return status;
	// close the file
    return db.closeFile(file);
  }

  return FILEEXISTS;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName) {
  return (db.destroyFile(fileName));
}

// constructor opens the underlying file
// reads and pins the header page in the buffer pool
// and then also reads and pins the first datapage
// into the buffer pool, setting curRec to NULLRID
// and initializing the hdrPageNo and curPageNo respectively
// Finally, sets the dirty flags which are false to begin with
HeapFile::HeapFile(const string &fileName, Status &returnStatus) {
  Status status;
  Page *pagePtr;

  cout << "opening file " << fileName << endl;
  // open the file and read in the header page and the first data page
  if ((status = db.openFile(fileName, filePtr)) != OK) {
    cerr << "open of heap file failed\n";
    returnStatus = status;
    return;
  }
  // get the header page (first page) from the file
  if ((status = filePtr->getFirstPage(headerPageNo)) != OK) {
    cerr << "could not get first page of file\n";
    returnStatus = status;
    db.closeFile(filePtr); // close file if we cannot read the header page
    return;
  }
  // read and pin the header page into the buffer pool
  if ((status = bufMgr->readPage(filePtr, headerPageNo, pagePtr)) != OK) {
    cerr << "could not get read header page into buffer manager\n";
    returnStatus = status;
    return;
  }
  // be sure to properly set the private header page variable
  headerPage = (FileHdrPage *)pagePtr;
  curPageNo = headerPage->firstPage;
  // read and pin the first data page of the file to the buffer pool
  if ((status = bufMgr->readPage(filePtr, curPageNo, pagePtr)) != OK) {
    cerr << "could not get read header page into buffer manager\n";
    returnStatus = status;
    return;
  }
  curRec = NULLRID;
  curPage = pagePtr;
  curDirtyFlag = false;
  hdrDirtyFlag = false;
  // cout << "current page no: " << curPageNo
  //      << "; header page no: " << headerPageNo << endl;
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

  // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file
  // are flushed to disk if (status != OK) cerr << "error in flushFile call\n";
  // before close the file
  status = db.closeFile(filePtr);
  if (status != OK) {
    cerr << "error in closefile call\n";
    Error e;
    e.print(status);
  }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const { return headerPage->recCnt; }

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &rid, Record &rec) {
  int pageNo = rid.pageNo;

  // If curPage is NULL or the record is not on the current page
  if (curPage == NULL || curPageNo != pageNo) {
    // Unpin the current page if it's not NULL
    if (curPage != NULL) {
      bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    }

    // Read the page with the requested record into the buffer
    Status status = bufMgr->readPage(filePtr, pageNo, curPage);
    if (status != OK)
      return status;

    // Update bookkeeping information
    curPageNo = pageNo;
    curDirtyFlag = false; // Assuming reading a record doesn't modify the page
  }

  // Retrieve the record
  return curPage->getRecord(rid, rec);
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

// @brief: continue linear scan on the heapfile,
// attempting to find the next record that matches the filtering criterion.
// @parameter: set outRid to the next matched record id if such record exists
// @return: OK if we found a matched record,
// FILEEOF if we cannot find such record before reaching the end of file.
// @assumption: read/pin/unpin page always success.
// current page is always pinned if exists.
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
    // test the current record
    Record rec;
    curPage->getRecord(curRec, rec);
    if (matchRec(rec)) {
      outRid = curRec;
      return OK;
    }
    // move on to the next record, if not reaching end of this page
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
  // switch to the next page: on error, curPage remains NULL
  // if read success, the page should be pinned
  curPage = NULL;
  bufMgr->readPage(filePtr, curPageNo, curPage);
  curDirtyFlag = false;
  curRec = NULLRID;
  // recursively scan the next page
  return scanNext(outRid);
}

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
  headerPage->recCnt--;
  hdrDirtyFlag = true;
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

// @brief: Insert a record into a heapfile
// @parameter:
// rec: the record data
// outRid: store the record at at which this record is inserted
// @return: status
// OK if insert success
// UNIXERR if page allocation, page read/write failed
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid) {
  Status status;
  // by default, we insert record at the end
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
    // current page is modified
    // header metadata is also updated
    curDirtyFlag = true;
    headerPage->recCnt++, hdrDirtyFlag = true;
    return OK;
  }
  // invariant: current page is the last page

  // current page does not have enough free space
  // allocate a new page to accommodate the record
  Page *page;
  int pn;
  ERR_RET((status = bufMgr->allocPage(filePtr, pn, page)) != OK, status, {});
  page->init(pn);

  // append the new page onto the heap file page linked list
  curPage->setNextPage(pn), curDirtyFlag = true;
  ERR_RET((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK,
          status, {});
  // header metadata update
  headerPage->pageCnt++, headerPage->lastPage = pn, hdrDirtyFlag = true;

  // move on to the newly allotted page, try insert into that page
  curPage = page, curPageNo = pn, curDirtyFlag = false;
  return insertRecord(rec, outRid);
}
