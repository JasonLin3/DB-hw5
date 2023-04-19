#include "heapfile.h"
#include "error.h"

/**
 * Create a Heap File object with the given name
 * 
 * @param fileName          Contains the name for the new heapfile
 * @return const Status     Returns success or detailed error on fail
 */
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // Try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		status = db.createFile(fileName);
        status = db.openFile(fileName, file);
        // Allocate pages and initialize
		status = bufMgr->allocPage(file, hdrPageNo, newPage);
        hdrPage = (FileHdrPage*) newPage;
        strncpy(hdrPage->fileName, fileName.data(), fileName.size() + 1);
		status = bufMgr->allocPage(file, newPageNo, newPage);
        newPage->init(newPageNo);
        //Set header pag fields
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;
        //Clean up operation by unpinning used pages and closing file
		bufMgr->unPinPage(file, newPageNo, true);
		bufMgr->unPinPage(file, hdrPageNo, true);
        db.closeFile(file);
        return OK;
    }
    // Tried to open an existing file
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * Constructs a new heapfile object.
 * Opens the underlying file within the database for later usage.
 * Uses pass by reference for return status as constructor returns the new object.
 * 
 * @param fileName          Name for new heapFile
 * @param returnStatus      Pass by reference status to reflect success or failure
 */
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;
    int     hdrPageNo;
    int     hdrPage;

    // cout << "opening file " << fileName << endl;

    // open the file
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        //Access header page and read into buffer
		filePtr->getFirstPage(hdrPageNo);
        bufMgr->readPage(filePtr, hdrPageNo, pagePtr);
        //Update metadata in header page
        headerPage = (FileHdrPage*) pagePtr;
        headerPageNo = hdrPageNo;
        hdrDirtyFlag = false;

        //Access first page and update metadata
        bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
        curRec = NULLRID;	

		returnStatus = OK;
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
    // cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

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

/**
 * Retrieve an arbitrary record from a file.
 * if record is not on the currently pinned page, the current page
 * is unpinned and the required page is read into the buffer pool
 * and pinned.
 * 
 * @param rid               rid of record to be found
 * @param rec               Pass by reference variable to hold located record
 * @return const Status     Returns OK on succesfull record access
 */
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    
    // if record on curPage
   if(rid.pageNo == curPageNo) {
    //Read in record to rid
    curPage->getRecord(rid, rec);
   } else if (curPage == NULL){ // if curPage is null
    //Update page metadata and read in record to rid
    curPageNo = rid.pageNo;
    bufMgr->readPage(filePtr, curPageNo, curPage);
    curPage->getRecord(rid, rec);
    curDirtyFlag = false;
   } else { //Get the page containing the record
    //Update page metadata and read in record
    bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    bufMgr->readPage(filePtr, rid.pageNo, curPage);
    curPageNo = rid.pageNo;
    curDirtyFlag = false;
    curPage->getRecord(rid, rec);
   }
    //Set current record to rid
    curRec = rid;
    return OK;

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

/**
 * Acesses the next record that satisfies the filter,
 * starting with the most recently accessed record.
 * Continues between pages in necessary to find the next record.
 * 
 * @param outRid            Pass by reference variable to hold the RID of the next record
 * @return const Status     Returns OK for success or EOF if no next record exists
 */
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	pageStatus = OK;
    Status 	recStatus = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo = 0;
    Record      rec;

    // current page is invalid
    if(curPage == NULL) {
        //Access the header page and start search there
        curPageNo = headerPage-> firstPage;
        bufMgr->readPage(filePtr, curPageNo, curPage);
        curDirtyFlag = false;
        curPage->firstRecord(nextRid);
    } else {
        // get next record
        recStatus = curPage->nextRecord(curRec, nextRid);
    }
    

    while(pageStatus == OK && nextPageNo != -1){ // loop through pages
        while(recStatus == OK){ // loop through records
            curPage->getRecord(nextRid, rec);
            // found record that matches filter
            if(matchRec(rec)) {
                //Update current record and return variable
                curRec = nextRid;
                outRid = curRec;
                return OK;
            }
            //Look to next record and confirm it is valid
            tmpRid = nextRid;
            recStatus = curPage->nextRecord(tmpRid, nextRid);
        } 
        //Advance to the next page
        pageStatus = curPage->getNextPage(nextPageNo);
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curDirtyFlag = false;
        curPageNo = nextPageNo;
        bufMgr->readPage(filePtr, curPageNo, curPage);
        //Get first record on the new page to begin search
        recStatus = curPage->firstRecord(nextRid);
    }
    curRec = NULLRID; //Update curRec to reflect failure to find a valid RID
    return FILEEOF;
	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    //cout<< "getRecord. record (" << curRec.pageNo << "." << curRec.slotNo << ")" << endl;
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

/**
 * Inserts a record into the file where it can find room.
 * Will create new page if necessary to fit the new record.
 * 
 * @param rec               Record to be inserted
 * @param outRid            Pass by reference RID of record after insertion
 * @return const Status     Returns OK upon successful insertion
 */
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
    
    // make curPage last page if null
    if(curPage == NULL) {
        curPageNo = headerPage->lastPage;
        bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        curDirtyFlag = false;
    }
    
    // insert record and bookkeeping
    status = curPage->insertRecord(rec, rid);
  
    // if page doesn't have space make new page
    if(status == NOSPACE) {

        //Unpin page that didn't have room
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curDirtyFlag = false;
        // create new page
        bufMgr->allocPage(filePtr, newPageNo, newPage); // pin1
        newPage->init(newPageNo);
        // make last page current page
        curPageNo = headerPage->lastPage;
        bufMgr->readPage(filePtr, curPageNo, curPage);  // pin2
        // link last page to new last page
        curPage->setNextPage(newPageNo);
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);  // unpin 2
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt += 1;
        // set curPage to be new page
        curPageNo = newPageNo;
        bufMgr->unPinPage(filePtr, newPageNo, true); //Unpin new page
        bufMgr->readPage(filePtr, curPageNo, curPage); //Read in new page as curPage
        // try to insert record
        curPage->insertRecord(rec, rid);
    }
  
    // finish bookkeeping
    headerPage->recCnt += 1;
    curDirtyFlag = true;
    hdrDirtyFlag = true;
    outRid = rid;
    return OK;
}


