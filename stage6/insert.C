#include "catalog.h"
#include "error.h"
#include "query.h"
#include "heapfile.h"
extern AttrCatalog *attrCat;
/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

void print(const attrInfo& ai) {
  Datatype t = (Datatype)ai.attrType;

  std::cout << "Relname: " << ai.relName << std::endl
            << "Type: " << (t == INTEGER ? 'i' : (t == FLOAT ? 'f' : 's'))
            << std::endl
            << "Len: " << ai.attrLen << std::endl
            << "Name: " << ai.attrName << std::endl
            << "Value: ";
  switch(t) {
    case INTEGER:
      std::cout << atoi((char*)ai.attrValue) << std::endl;
      break;
    case FLOAT:
      std::cout << atof((char *)(ai.attrValue)) << std::endl;
      break;
    default:
      std::cout << ((char *)(ai.attrValue)) << std::endl;
      break;
  }
  std::cout << std::endl;
}

const Status QU_Insert(const string &relation, const int attrCnt,
                       const attrInfo attrList[]) {
  // part 6
  Status stat = OK;
  AttrDesc *attrs;
  RelCatalog rc(stat);
  attrInfo CorrectOrder[attrCnt];
  int _ = attrCnt;
  int length = 0;
  if (attrCat == NULL || (stat = attrCat->getRelInfo(relation, _, attrs)) != OK)
    return stat;
  for(int i = 0; i < attrCnt; i++)
    length += attrs[i].attrLen;
  std::cout << "LENGTH: " << length << std::endl;
  rc.help(relation);
  for (int i = 0; i < attrCnt; i++) {
    print(attrList[i]);
  }
  for(int i = 0; i < attrCnt; i++) {
    //see if this is a valid spot
    if (strcmp(attrs[i].relName, attrList[i].relName) == 0 &&
        strcmp(attrs[i].attrName, attrList[i].attrName) == 0) {
      // good position, we can continue;
      CorrectOrder[i] = attrList[i];
    } else {
      // bad position, find the correct attr for this position
      for(int j = 0; j < attrCnt; j++) {
        if (strcmp(attrs[i].relName, attrList[j].relName) == 0 &&
            strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
          // good position, we can continue;
          CorrectOrder[i].attrLen = attrList[j].attrLen;
          CorrectOrder[i].attrValue = attrList[j].attrValue;
          CorrectOrder[i].attrType = attrList[j].attrType;
          strcpy(CorrectOrder[i].attrName, attrList[j].attrName);
          strcpy(CorrectOrder[i].relName, attrList[j].relName);
          break;
        }
      }
    }
  }

  
  Status st = OK;
  InsertFileScan output(CorrectOrder[0].relName, st);
  ASSERT(st == OK);
  //except for the weird values, this is in the right order, so now we can go ahead and insert it
  //into the table
  char outData[length];
  RID outRID;
  Record outRec = {
  .data = (void *)outData,
  .length = length};
  int offset = 0;

  for(int i = 0; i < attrCnt; i++) {
    auto ai = CorrectOrder[i];
    Datatype t = (Datatype)ai.attrType;
    int temp;
    float temp2;
    //char* temp3;
    switch (t) {
    case INTEGER:
    temp = atoi((char *)ai.attrValue);
      memcpy(outData + offset, (void*)(&temp), attrs[i].attrLen);
      break;
    case FLOAT:
      temp2 = atof((char *)(ai.attrValue));
          memcpy(outData + offset, (void*)(&temp2), attrs[i].attrLen);
      break;
    default:
      memcpy(outData + offset, CorrectOrder[i].attrValue, attrs[i].attrLen);
    }
    
    offset += attrs[i].attrLen;
  }
  
  output.insertRecord(outRec, outRID);
  return st;
}
