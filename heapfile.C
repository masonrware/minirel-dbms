#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* file;
    Status status;
    FileHdrPage* hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page* newPage;

    // Try to open the file. This should return an error if the file doesn't exist.
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // File doesn't exist. First create it and allocate an empty header page.
        status = db.createFile(fileName);
        status = db.openFile(fileName, file);
        if (status != OK)
        {
            cerr << "Error: Failed to create and open file " << fileName << endl;
            return status;
        }

        // Allocate an empty page for the header.
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK)
        {
            cerr << "Error: Failed to allocate header page for file " << fileName << endl;
            return status;
        }

        // Cast the empty page to FileHdrPage.
        hdrPage = reinterpret_cast<FileHdrPage*>(newPage);

        // Initialize the header page values.
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE - 1);
        hdrPage->fileName[MAXNAMESIZE - 1] = '\0'; // Ensure null termination
        hdrPage->firstPage = hdrPageNo; // Initialize to an invalid page number
        hdrPage->lastPage = hdrPageNo;  // Initialize to an invalid page number
        hdrPage->pageCnt = 0; // Initially, there are no pages
        hdrPage->recCnt = 0;  // Initially, there are no records
        // Set the fileName field of the header page.
        strncpy(hdrPage->fileName, fileName.c_str(), sizeof(hdrPage->fileName) - 1);
        hdrPage->fileName[sizeof(hdrPage->fileName) - 1] = '\0'; // Ensure null termination

        // Mark the header page as dirty and unpin it.
        // status = bufMgr->unPinPage(file, hdrPageNo, true);


        cout << "53 -- header info" << endl;
        cout << "fileName: " << hdrPage->fileName << endl;
        cout << "firstPage: " << hdrPage->firstPage << endl;
        cout << "lastPage: " << hdrPage->lastPage << endl;
        cout << "pageCnt: " << hdrPage->pageCnt << endl;
        cout << "recCnt: " << hdrPage->recCnt << endl;


        // Mark the header page as dirty and unpin it.
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK)
        {
            cerr << "Error: Failed to unpin header page for file " << fileName << endl;
            return status;
        }

        // Allocate an empty page for the first data page.
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK)
        {
            cerr << "Error: Failed to allocate first data page for file " << fileName << endl;
            return status;
        }
        
        // Initialize the first data page.
        newPage->init(newPageNo);

        // Store the page number of the first data page in the header page.
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

        // Mark the first data page as dirty and unpin it.
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK)
        {
            cerr << "Error: Failed to unpin first data page for file " << fileName << endl;
            return status;
        }

        return OK;
    }
    return FILEEXISTS;
}


// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
#include "heapfile.h"
#include "error.h"

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        Page * hdrPage;
        // Get the header page
        if((status = filePtr->getFirstPage(headerPageNo)) !=OK ){
            cerr << "Error: Failed get the page number of the header page of file " << fileName << endl;
            returnStatus = status;
            return;
        }

        // Read and pin the header page
        if ((status = bufMgr->readPage(filePtr, headerPageNo, hdrPage)) != OK) {
            cerr << "Error: Failed to read and pin the header page of file " << fileName << endl;
            returnStatus = status;
            return;
        }
        hdrDirtyFlag = false; // Header page initially not dirty
        headerPage = reinterpret_cast<FileHdrPage*>(hdrPage);

        cout << "137 -- header info" << endl;
        cout << "fileName: " << headerPage->fileName << endl;
        cout << "firstPage: " << headerPage->firstPage << endl;
        cout << "lastPage: " << headerPage->lastPage << endl;
        cout << "pageCnt: " << headerPage->pageCnt << endl;
        cout << "recCnt: " << headerPage->recCnt << endl;
        cout << "headerPageNo: " << headerPageNo << endl;

        // Get page number of first data page
        // if((status = hdrPage->getNextPage(curPageNo)) !=OK ){
        //     cerr << "Error: Failed get the page number of the first data page of file " << fileName << endl;
        //     returnStatus = status;
        //     return;
        // }

        // Read and pin the first data page
        if ((status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage)) != OK) {
            cerr << "Error: Failed to read and pin the first data page of file " << fileName << endl;
            returnStatus = status;
            return;
        }
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false; // Current page initially not dirty
        curRec = NULLRID; // No current record initially
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}


// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   
    Status status;

    // Check if the record is on the currently pinned page
    if (rid.pageNo != curPageNo) {
        // Unpin the current page
        if ((status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag)) != OK) {
            cerr << "Error: Failed to unpin current page while fetching record" << endl;
            return status;
        }
        // Read and pin the required page
        if ((status = bufMgr->readPage(filePtr, rid.pageNo, curPage)) != OK) {
            cerr << "Error: Failed to read and pin page " << rid.pageNo << " while fetching record" << endl;
            return status;
        }
        curPageNo = rid.pageNo; // Update current page number
        curDirtyFlag = false; // Current page initially not dirty
    }

    // Fetch the record from the current page
    status = curPage->getRecord(rid, rec);

    return status;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    // Do we start on first page of file?
        // check if curPage is null? or curRec?
            // if curPage is null do we set to first page?
            // or is it null when we reach the end of the file

    // curRec
	// matchRec - see if record matches scan filter (true / false)
        // convert RID to pointer to rec data first

	// Page->getNextPage - return pageNum of next page
        // then use bufMgr->readPage to read next page into curPage
        // read page sets pin count to 1 (?)
	
	// set outRid to curRec if it matches search
    // return OK if no errors, otherwise first error that occurs
	
	
	
	
	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
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
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*   newPage;
    int     newPageNo;
    Status  status, unpinstatus;
    RID     rid;

    // Check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // Will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // If the current page is NULL, make the last page the current page and read it into the buffer
    if (curPage == NULL)
    {
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK)
        {
            cerr << "Error: Failed to read current page " << headerPage->lastPage << " into buffer" << endl;
            return status;
        }
    }

    // if the current page is not the lastPage
    if(curPageNo != headerPage->lastPage) {
        // unpin curpage
        status = bufMgr->unPinPage(filePtr, curPageNo, true);

        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK)
        {
            cerr << "Error: Failed to read current page " << headerPage->lastPage << " into buffer" << endl;
            return status;
        }
    }

    
    // Try to insert the record into the current page
    status = curPage->insertRecord(rec, rid);
    if (status == OK)
    {
        // Update data fields
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        outRid = rid;

        return OK;
    }
    else if (status == NOSPACE)
    {
        // Current page is full, create a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK)
        {
            cerr << "Error: Failed to allocate new page for inserting record" << endl;
            return status;
        }

        // Initialize the new page
        newPage->init(newPageNo);
        if (status != OK)
        {
            cerr << "Error: Failed to initialize new page for inserting record" << endl;
            return status;
        }

        // Link up the new page appropriately
        curPage->setNextPage(newPageNo);
        curDirtyFlag = true;

        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        // TODO check status

        // Make the current page the newly allocated page
        curPage = newPage;
        curPageNo = newPageNo;

        // Try to insert the record into the new page
        status = curPage->insertRecord(rec, rid);
        if (status != OK)
        {
            cerr << "Error: Failed to insert record into new page" << endl;
            return status;
        }

        // Update data fields
        headerPage->recCnt++;
        headerPage->pageCnt++;
        headerPage->lastPage = newPageNo;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        outRid = rid;

        return OK;
    }
    else
    {
        // Other error occurred during insertion
        cerr << "Error: Failed to insert record into current page" << endl;
        return status;
    }
}



