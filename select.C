#include "catalog.h"
#include "query.h"

// Pre-declaration of the function for scanning and selecting
const Status ScanSelect(const string & result, 
                        const int projCnt, 
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc, 
                        const Operator op, 
                        const char *filterValue,
                        const int totalRecordLength);

/**
 * Executes record selection based on projection and filter parameters.
 *
 * @param result Relation name where selection results will be stored.
 * @param projCnt Number of projection attributes.
 * @param projNames Information array of projection attributes.
 * @param attr Details of the attribute used for applying the filter.
 * @param op Operator used in filtering comparison.
 * @param attrValue Value to compare against the attribute.
 * @return Status indicating success or failure of the selection operation.
 */
const Status QU_Select(const string & result, 
                       const int projCnt, 
                       const attrInfo projNames[],
                       const attrInfo *attr, 
                       const Operator op, 
                       const char *attrValue)
{
    cout << "Executing QU_Select..." << endl;

    // Setup for selecting attributes and attribute descriptions
    Status status;
    AttrDesc attributeDetails;
    AttrDesc* projectionAttributes = new AttrDesc[projCnt];
    int recordLength = 0; 
    Operator comparisonOperator;
    const char* comparisonValue;

    // Retrieve attributes from the catalog
    for (int idx = 0; idx < projCnt; idx++) {
        status = attrCat->getInfo(projNames[idx].relName, projNames[idx].attrName, projectionAttributes[idx]);
        recordLength += projectionAttributes[idx].attrLen;
        if (status != OK)
            return status;
    }

    // Set up the filtering process
    if (attr != nullptr)
    {
        float floatFilterValue;
        int intFilterValue;
        
        status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attributeDetails);

        if (attr->attrType == INTEGER)
        {
            intFilterValue = atoi(attrValue);
            comparisonValue = reinterpret_cast<char *>(&intFilterValue);
        }
        else if (attr->attrType == FLOAT)
        {
            floatFilterValue = atof(attrValue);
            comparisonValue = reinterpret_cast<char *>(&floatFilterValue);
        }
        else if (attr->attrType == STRING)
        {
            comparisonValue = attrValue;
        }

        comparisonOperator = op;
    } 
    else {
        attributeDetails = {projNames[0].relName, projNames[0].attrName, 0, 0, STRING};
        comparisonValue = nullptr;
        comparisonOperator = EQ;
    }
    
    // Perform the selection via a scan
    status = ScanSelect(result, projCnt, projectionAttributes, &attributeDetails, comparisonOperator, comparisonValue, recordLength);
    if (status != OK) 
        return status;

    return OK; // Return OK if selection is successful
}

/**
 * Executes the scanning and selection of records from a heap file based on filtering criteria.
 *
 * @param result Relation name for the scan result storage.
 * @param projCnt Count of attributes to project.
 * @param projNames Descriptions of projection attributes.
 * @param attrDesc Description of the filtering attribute.
 * @param op Operator for the comparison in the filter.
 * @param filterValue Value used for filtering in the scan.
 * @param reclen Length of each record in the scan.
 * @return Status indicating success or failure of the scan.
 */
const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
                        const int projCnt, 
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc, 
                        const Operator op, 
                        const char *filterValue,
                        const int reclen)
{
    cout << "Performing ScanSelect operation..." << endl;

    // Initialize necessary variables for the output record handling
    int totalTuples = 0; // Counter for records processed
    Status status;
    char outputBuffer[reclen];
    Record outputRecord;
    RID scanRID;
    Record scannedRecord;
    InsertFileScan resultRelation(result, status);
    if (status != OK) 
        return status;

    outputRecord.data = static_cast<void *>(outputBuffer);
    outputRecord.length = reclen;
    
    HeapFileScan heapScan(attrDesc->relName, status);
    if (status != OK) { 
        return status;
    }

    status = heapScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), filterValue, op);
    if (status != OK) { 
        return status;
    }

    while (heapScan.scanNext(scanRID) == OK) {
        int offset = 0;
        RID outputRID;
        status = heapScan.getRecord(scannedRecord);
        ASSERT(status == OK);
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputBuffer + offset, static_cast<char *>(scannedRecord.data) + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        status = resultRelation.insertRecord(outputRecord, outputRID);
        ASSERT(status == OK);
        totalTuples++;
    }
    return OK; // Return OK if scanning is successful
}
