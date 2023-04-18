#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		status = db.createFile(fileName);
        status = db.openFile(fileName, file);
		status = bufMgr->allocPage(file, hdrPageNo, newPage);
        hdrPage = (FileHdrPage*) newPage;
        strncpy(hdrPage->fileName, fileName.data(), fileName.size() + 1);
		status = bufMgr->allocPage(file, newPageNo, newPage);
        newPage->init(newPageNo);
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;
		bufMgr->unPinPage(file, newPageNo, true);
		bufMgr->unPinPage(file, hdrPageNo, true);
        db.closeFile(file);
        return OK;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;
    int     hdrPageNo;
    int     hdrPage;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
		filePtr->getFirstPage(hdrPageNo);
        bufMgr->readPage(filePtr, hdrPageNo, pagePtr);
        headerPage = (FileHdrPage*) pagePtr;
        headerPageNo = hdrPageNo;
        hdrDirtyFlag = false;

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
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    
    // if record on curPage
   if(rid.pageNo == curPageNo) {
    curPage->getRecord(rid, rec);
   } else if (curPage == NULL){ // if curPage is null
    curPageNo = rid.pageNo;
    bufMgr->readPage(filePtr, curPageNo, curPage);
    curPage->getRecord(rid, rec);
    curDirtyFlag = false;
   } else { //Get the page containing the record
    bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    bufMgr->readPage(filePtr, rid.pageNo, curPage);
    curPageNo = rid.pageNo;
    curDirtyFlag = false;
    curPage->getRecord(rid, rec);
   }
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
        curPageNo = headerPage-> firstPage;
        bufMgr->readPage(filePtr, curPageNo, curPage);
        curDirtyFlag = false;
        curPage->firstRecord(nextRid);
    } else {
        // get next record
        recStatus = curPage->nextRecord(curRec, nextRid);
    }
    
    // // current record not valid, go to next page
    // if(curPage->getRecord(nextRid, rec)==INVALIDSLOTNO){
    //     cout << "GOING TO NEXT APGE" << endl;
    //     //Release current page
    //     pageStatus = curPage->getNextPage(nextPageNo);
    //     bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    //     //Update current page to next page
    //     curPageNo = nextPageNo;
    //     curDirtyFlag = false;
    //     bufMgr->readPage(filePtr, curPageNo, curPage);
    //     curPage->firstRecord(nextRid);
    // }

    while(pageStatus == OK && nextPageNo != -1){ // loop through pages
        while(recStatus == OK){ // loop through records
            curPage->getRecord(nextRid, rec);
            // found match
            if(matchRec(rec)) {
                curRec = nextRid;
                outRid = curRec;
                return OK;
            }
            tmpRid = nextRid;
            recStatus = curPage->nextRecord(tmpRid, nextRid);
        } 
        pageStatus = curPage->getNextPage(nextPageNo);
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curDirtyFlag = false;
        curPageNo = nextPageNo;
        bufMgr->readPage(filePtr, curPageNo, curPage);
        recStatus = curPage->firstRecord(nextRid);
    }
    curRec = NULLRID; //TODO
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

// Insert a record into the file
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


