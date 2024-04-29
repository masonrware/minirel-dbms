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

    // Open the heap file
    Status status;
    HeapFileScan *fileScan = new HeapFileScan(relation, status);
    if (status != OK) {
        delete fileScan;
        return status;
    }

    // Start the scan
	// TODO what is length supposed to be, can it be 0?
    status = fileScan->startScan(0, 0, type, attrValue, op);
    if (status != OK) {
        delete fileScan;
        return status;
    }

    RID rid;
    Record rec;

    // Iterate over each tuple in the heap file
    while ((status = fileScan->scanNext(rid)) == OK) {
        // Get the record
        status = fileScan->getRecord(rec);
        if (status != OK) {
            delete fileScan;
            return status;
        }

        // Delete the record if it matches the predicate
		// TODO I think I have to use scanNext not matchRec (matchRec is private)
        if (fileScan->matchRec(rec)) {
            status = fileScan->deleteRecord();
            if (status != OK) {
                delete fileScan;
                return status;
            }
        }
    }

    // End the scan
    status = fileScan->endScan();
    if (status != OK) {
        delete fileScan;
        return status;
    }

    // Clean up and return
    delete fileScan;
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