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

	Status status;

	if(attrName.empty() && attrValue == nullptr) {
		#include "catalog.h"

		Status status;

		// Open relation catalog
		RelCatalog relCatalog(status);
		if (status != OK)
			return status;

		// Open attribute catalog
		AttrCatalog attrCatalog(status);
		if (status != OK)
			return status;

		// Get information about the relation
		RelDesc relDesc;
		status = relCatalog.getInfo(relation, relDesc);
		if (status != OK)
			return status;

		// Get attributes of the relation
		AttrDesc* attrs = nullptr;
		int attrCnt = 0;
		status = attrCatalog.getRelInfo(relation, attrCnt, attrs);
		if (status != OK)
			return status;

		// Open the heap file associated with the relation
		HeapFileScan *fileScan = new HeapFileScan(relation, status);
		if (status != OK)
			return status;

		// Scan through the heap file and delete each tuple
		Record record;
		RID rid;
		status = fileScan->startScan(attrs[0].attrOffset, attrs[0].attrLen, Datatype(attrs[0].attrType), "", NE);
		if (status != OK)
			return status;

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

		status = fileScan->endScan();
		if (status != OK) {
			delete fileScan;
			fileScan = NULL;
			return status;
		}

		// Clean up and return
		delete fileScan;
		fileScan = NULL;
		delete[] attrs;
	} else {
		// Open the heap file
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
	}

	return OK;
}

