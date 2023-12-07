#include "catalog.h"
#include "error.h"
#include "heapfile.h"
#include "page.h"
#include "query.h"
#include <cstdlib>

// forward declaration
const Status ScanSelect(const string &result, const int projCnt,
                        const AttrDesc proj[], const AttrDesc *attrDesc,
                        const Operator op, const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result, const int projCnt,
                       const attrInfo projNames[], const attrInfo *attr,
                       const Operator op, const char *attrValue) {
  // Qu_Select sets up things and then calls ScanSelect to do the actual work
  cout << "Doing QU_Select " << endl;

  // For each attribute to be extracted,
  // consult the attribute catalog for offset, length, and type.
  AttrDesc projAttrs[projCnt], filterAttr;
  for (auto i = 0; i < projCnt; i++) {
    ASSERT(attrCat->getInfo(projNames[i].relName, projNames[i].attrName,
                            projAttrs[i]) == OK);
  }
  // get output record length from attrdesc structures
  int reclen = 0;
  for (auto i = 0; i < projCnt; i++) {
    reclen += projAttrs[i].attrLen;
  }

  char *filter = nullptr;
  char filter_values[PAGESIZE];
  if (attr != nullptr) { // we do have a filter
    ASSERT(attrCat->getInfo(attr->relName, attr->attrName, filterAttr) == OK);

    // enum Datatype { STRING, INTEGER, FLOAT };
    if (attr->attrType == FLOAT) {
      *(float *)filter_values = std::atof(attrValue);
    } else if (attr->attrType == INTEGER) {
      *(int *)filter_values = std::atoi(attrValue);
    } else { // this is a string
      strcpy(filter_values, attrValue);
    }
    filter = filter_values;
  }
  return ScanSelect(result, projCnt, projAttrs, &filterAttr, op, filter,
                    reclen);
}

const Status ScanSelect(const string &result, const int projCnt,
                        const AttrDesc proj[], const AttrDesc *attrDesc,
                        const Operator op, const char *filter,
                        const int reclen) {
  Status st = OK;
  cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
  // input and output stream
  HeapFileScan input(proj[0].relName, st);
  ASSERT(st == OK);
  InsertFileScan output(result, st);
  ASSERT(st == OK);

  // scan with filter
  if (filter) {
    auto offset = attrDesc->attrOffset;
    auto len = attrDesc->attrLen;
    auto dtype = (Datatype)attrDesc->attrType;
    ASSERT(input.startScan(offset, len, dtype, filter, op) == OK);
  } else {
    ASSERT(input.startScan(0, 0, STRING, NULL, op) == OK);
  }

  char outData[reclen];
  RID inRid, outRid;
  Record inRec, outRec = {
                    .data = (void *)outData,
                    .length = reclen,
                };
  while (input.scanNext(inRid) == OK) {
    ASSERT(input.getRecord(inRec) == OK);
    for (auto i = 0, offset = 0; i < projCnt; offset += proj[i++].attrLen) {
      memcpy(outData + offset, (char *)inRec.data + proj[i].attrOffset,
             proj[i].attrLen);
    }
    ASSERT(output.insertRecord(outRec, outRid) == OK);
  }
  return OK;
}
