#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *     OK on success
 *     an error code otherwise
 */

const Status QU_Delete(const string &relation, const string &attrName,
                       const Operator op, const Datatype type,
                       const char *attrValue) {
  cout << "Doing QU_Delete " << endl;
  Status status;
  HeapFileScan *hfs = new HeapFileScan(relation, status);
  if (status != OK) {
    return status; // Error in opening the file
  }

  if (!attrName.empty() && attrValue != nullptr) {
    // Retrieve attribute information
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) {
      delete hfs;
      return status; // Error in fetching attribute information
    }

    int offset = attrDesc.attrOffset;
    int length = attrDesc.attrLen;

    // Prepare the value for scanning
    void *valPtr;
    int intVal;
    float floatVal;
    switch (type) {
    case INTEGER:
      intVal = atoi(attrValue);
      valPtr = &intVal;
      break;
    case FLOAT:
      floatVal = atof(attrValue);
      valPtr = &floatVal;
      break;
    case STRING:
      valPtr = (void *)attrValue;
      break;
      // handle other types if necessary
    }

    // Start scan with filter
    status = hfs->startScan(offset, length, type,
                            reinterpret_cast<const char *>(valPtr), op);
    if (status != OK) {
      delete hfs;
      return status; // Error in starting the scan
    }

    RID outRid;
    Record rec;
    while ((status = hfs->scanNext(outRid)) == OK) {
      status = hfs->getRecord(rec);
      if (status != OK) {
        break; // Error in getting record
      }

      status = hfs->deleteRecord();
      if (status != OK) {
        break; // Error in deleting record
      }
    }
  } else {
    // Handle deletion of all records
    status = hfs->startScan(0, 0, type, nullptr, op);
    if (status != OK) {
      delete hfs;
      return status;
    }

    RID outRid;
    while ((status = hfs->scanNext(outRid)) == OK) {
      status = hfs->deleteRecord();
      if (status != OK) {
        break;
      }
    }
  }

  // Clean up
  if (status != FILEEOF) {
    hfs->endScan();
    delete hfs;
    return status;
  }

  status = hfs->endScan();
  delete hfs;
  return status;
}
