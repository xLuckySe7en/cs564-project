#include "catalog.h"
#include "error.h"
#include "query.h"
#include "heapfile.h"
//for getting catalog information
extern AttrCatalog *attrCat;


/*
  Correctly order the attribute list according to the relational schema
  The output CorrectOrder has the attributes in the correct order
*/
void order_attributes(attrInfo CorrectOrder[], int attrCnt, const attrInfo attrList[], AttrDesc *attrs) {
  for (int i = 0; i < attrCnt; i++) {
    // see if this is a valid spot
    if (strcmp(attrs[i].relName, attrList[i].relName) == 0 &&
        strcmp(attrs[i].attrName, attrList[i].attrName) == 0) {
      // good position, we can continue;
      CorrectOrder[i] = attrList[i];
      continue;
    }
    
    // bad position, find the correct attr for this position
    for (int j = 0; j < attrCnt; j++) {
      if (strcmp(attrs[i].relName, attrList[j].relName) == 0 &&
          strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
        // good position, we can continue after copying the data into our
        // helper array
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

/*
 * Inserts a record into the specified relation.
 * 
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status QU_Insert(const string &relation, const int attrCnt,
                       const attrInfo attrList[]) {
  cout << "Doing QU_Insert " << endl;
  // part 6
  Status st = OK;
  AttrDesc *attrs;
  RelCatalog rc(st);
  ASSERT(st == OK);
  attrInfo CorrectOrder[attrCnt];
  int _ = attrCnt;
  int length = 0;
  //Get the relation's info from the realtion catalog and put those
  //descriptions into attrs
  if (attrCat == NULL || (st = attrCat->getRelInfo(relation, _, attrs)) != OK)
    return st;
  //get teh length that an insert will occupy
  for(int i = 0; i < attrCnt; i++)
    length += attrs[i].attrLen;
  
  order_attributes(CorrectOrder, attrCnt, attrList, attrs);
  
  InsertFileScan output(CorrectOrder[0].relName, st);
  ASSERT(st == OK);
  //Initialize data structures for heapfile insert
  char outData[length];
  RID outRID;
  Record outRec = {
  .data = (void *)outData,
  .length = length};
  int offset = 0;
  //cpy the attributes' values to the outdata for later writing
  for(int i = 0; i < attrCnt; i++) {
    auto ai = CorrectOrder[i];
    Datatype t = (Datatype)ai.attrType;
    //the attrvalue is given as strings, so we have to do the appropriate cast
    //to have the correct byte data that we expect per the schema
    int temp;
    float temp2;
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
      //a character* doesn't need to be cast, we can just copy its bytes
      memcpy(outData + offset, CorrectOrder[i].attrValue, attrs[i].attrLen);
    }
    //to not overwrite previously copied data in outdata
    offset += attrs[i].attrLen;
  }
  
  st = output.insertRecord(outRec, outRID);
  ASSERT(st == OK);
  return OK;
}
