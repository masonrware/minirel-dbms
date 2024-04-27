#include "catalog.h"
#include "query.h"

/**
 * Performs the insertion of a new record into a given database table.
 * This function takes the supplied attribute values, converts them to the correct data formats,
 * and inserts them into the designated table. It supports various data types
 * such as INTEGER, FLOAT, and STRING, ensuring that the values adhere to the 
 * table's schema and data type requirements.
 *
 * @param tableName Name of the table where the new record will be added.
 * @param attributeCount Number of attributes in the record being added.
 * @param attributeDetails Array of attrInfo structures that detail the name, type, and value
 *                         of each attribute to be added to the table.
 * @return Status OK if the record is successfully inserted, or an error code if not.
 *         Possible error codes include ATTRTYPEMISMATCH for data type inconsistencies.
 */
const Status QU_Insert(const string &tableName,
					   const int attributeCount,
					   const attrInfo attributeDetails[])
{
	// Logging insert operation
	cout << "Executing QU_Insert" << endl;

	// Initialization of variables
	AttrDesc *tableAttributes;
	int tableAttrCount;
	Status opStatus;

	// Set up a new file scan for insert operation
	InsertFileScan insertScanner(tableName, opStatus);
	if (opStatus != OK)
	{
		return opStatus;
	}

	// Retrieve attribute information of the table
	opStatus = attrCat->getRelInfo(tableName, tableAttrCount, tableAttributes);
	if (opStatus != OK)
	{
		return opStatus;
	}

	// Calculate total length of all attributes
	int totalLength = 0;
	for (int i = 0; i < tableAttrCount; i++)
	{
		totalLength += tableAttributes[i].attrLen;
	}

	// Prepare buffer and record for insertion
	char buffer[totalLength];
	Record newRecord; // Record to be inserted
	newRecord.data = (void *)buffer;
	newRecord.length = totalLength;

	// Process each attribute to be inserted
	for (int i = 0; i < attributeCount; i++)
	{
		for (int j = 0; j < tableAttrCount; j++)
		{
			if (strcmp(tableAttributes[j].attrName, attributeDetails[i].attrName) == 0)
			{
				int valInt;
				float valFloat;
				char *actualValue;

				if (attributeDetails[i].attrValue == NULL)
				{
					return ATTRTYPEMISMATCH; // Data type mismatch
				}

				switch (attributeDetails[i].attrType)
				{
				case INTEGER:
					valInt = atoi((char *)attributeDetails[i].attrValue);
					actualValue = (char *)&valInt;
					break;
				case FLOAT:
					valFloat = atof((char *)attributeDetails[i].attrValue);
					actualValue = (char *)&valFloat;
					break;
				case STRING:
					actualValue = (char *)attributeDetails[i].attrValue;
					break;
				}

				// Store the value in the buffer
				memcpy(buffer + tableAttributes[j].attrOffset, actualValue, tableAttributes[j].attrLen);
			}
		}
	}

	// Finalize record insertion
	RID insertedRecordID;
	opStatus = insertScanner.insertRecord(newRecord, insertedRecordID);
	if (opStatus != OK)
	{
		return opStatus;
	}

	return OK; // Successfully inserted
}
