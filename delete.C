#include "catalog.h"
#include "query.h"
#include "heapfile.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
                       const string & attrName, 
                       const Operator op,
                       const Datatype type, 
                       const char *attrValue) {

	cout << "Executing QU_Delete..." << endl;

    // Open the heap file
    Status status;
    HeapFileScan *fileScan = new HeapFileScan(relation, status);
    if (status != OK) {
        delete fileScan;
		fileScan = NULL;
        return status;
    }

	AttrCatalog *attrCat = new AttrCatalog(status);
	if (status != OK) {
		delete fileScan;
		fileScan = NULL;
		return status;
	}

	AttrDesc attrDesc;
	status = attrCat->getInfo(relation, attrName, attrDesc);
	if (status != OK) {
		delete fileScan;
		fileScan = NULL;
		return status;
	}

	int offset = attrDesc.attrOffset;
	int length = attrDesc.attrLen;
	int intAttrValue;
	float floatAttrValue;

	// Cast attrValue properly
	if(type == INTEGER) {
		intAttrValue = atoi(attrValue);
		status = fileScan->startScan(offset, length, type, reinterpret_cast<char *>(&intAttrValue), op);
	} else if  (type == FLOAT) {
		floatAttrValue = atof(attrValue);
		status = fileScan->startScan(offset, length, type, reinterpret_cast<char *>(&floatAttrValue), op);
	} else if (type == STRING) {
		status = fileScan->startScan(offset, length, type, attrValue, op);
	}

	if (status != OK) {
		delete fileScan;
		fileScan = NULL;
		return status;
	}

    RID rid;

    // Iterate over each tuple in the heap file
    while ((status = fileScan->scanNext(rid)) == OK) {
		// Delete the record if it matches the predicate
		status = fileScan->deleteRecord();
		if (status != OK) {
			delete fileScan;
			fileScan = NULL;
			return status;
		}
    }
	if (status != FILEEOF) error.print(status);

    // End the scan
    status = fileScan->endScan();
    if (status != OK) {
        delete fileScan;
		fileScan = NULL;
        return status;
    }

    // Clean up and return
    delete fileScan;
	fileScan = NULL;
    return OK;
}



// scan1 = new HeapFileScan("dummy.02", status);
//     if (status != OK) error.print(status);
//     else 
//     {
//         scan1->startScan(0, 0, STRING, NULL, EQ);
// 		i = 0;
// 		deleted = 0;
// 		while ((status = scan1->scanNext(rec2Rid)) != FILEEOF)
// 		{
// 			// cout << "processing record " << i << i << endl;
// 			if (status != OK) error.print(status);
// 			if ((i % 2) != 0)
// 			{
// 				//printf("deleting record %d with rid(%d.%d)\n",i,rec2Rid. pageNo, rec2Rid.slotNo);
// 				status = scan1->deleteRecord(); 
// 				deleted++;
// 				if ((status != OK)  && ( status != NORECORDS))
// 				{
//                     cout << "err0r status return from deleteRecord" << endl;
//                     error.print(status);
// 				}
// 			}
// 			i++;
// 		}
// 		if (status != FILEEOF) error.print(status);
// 		cout << "deleted " << deleted << " records" << endl;
// 		if (deleted != num / 2)
//             cout << "Err0r.   should have deleted " << num / 2 << " records!" << endl;
// 		scan1->endScan();
//     }
//     delete scan1;
//     scan1 = NULL;