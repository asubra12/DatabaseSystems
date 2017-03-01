import io, math, os, os.path, pickle, struct
from struct import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema
from Storage.Page        import PageHeader, Page
from Storage.SlottedPage import SlottedPageHeader, SlottedPage


class FileHeader:
  """
  A file header class, containing a page size and a schema for the data
  entries stored in the file.

  Our file header object also keeps its own binary representation per instance
  rather than at the class level, since each file may have a variable length schema.
  The binary representation is a struct, with three components in its format string:
  i.   header length
  ii.  page size
  iii. a JSON-serialized schema (from DBSchema.packSchema)

  # >>> schema = DBSchema('employee', [('id', 'int'), ('dob', 'char(10)'), ('salary', 'int')])
  # >>> fh = FileHeader(pageSize=io.DEFAULT_BUFFER_SIZE, pageClass=SlottedPage, schema=schema)
  # >>> b = fh.pack()
  # >>> fh2 = FileHeader.unpack(b)
  # >>> fh.pageSize == fh2.pageSize
  True

  # >>> fh.schema.schema() == fh2.schema.schema()
  True

  ## Test the file header's ability to be written to, and read from a Python file object.
  # >>> f1 = open('test.header', 'wb')
  # >>> fh.toFile(f1)
  # >>> f1.flush(); f1.close()
  #
  # >>> f2 = open('test.header', 'r+b')
  # >>> fh3 = FileHeader.fromFile(f2)
  # >>> fh.pageSize == fh3.pageSize \
  #     and fh.pageClass == fh3.pageClass \
  #     and fh.schema.schema() == fh3.schema.schema()
  True

  # >>> os.remove('test.header')
  """

  def __init__(self, **kwargs):
    other = kwargs.get("other", None) 
    if other:
      self.fromOther(other)

    else:
      pageSize    = kwargs.get("pageSize", None)
      pageClass   = kwargs.get("pageClass", None)
      schema      = kwargs.get("schema", None)

      if pageSize and pageClass and schema:
        pageClassLen   = len(pickle.dumps(pageClass))
        schemaDescLen  = len(schema.packSchema())
        self.binrepr   = Struct("HHHH"+str(pageClassLen)+"s"+str(schemaDescLen)+"s")
        self.size      = self.binrepr.size
        self.pageSize  = pageSize
        self.pageClass = pageClass
        self.schema    = schema

      else:
        raise ValueError("Invalid file header constructor arguments")

  def fromOther(self, other):
    self.binrepr   = other.binrepr
    self.size      = other.size
    self.pageSize  = other.pageSize
    self.pageClass = other.pageClass
    self.schema    = other.schema

  def pack(self):
    if self.binrepr and self.pageSize and self.schema:
      packedPageClass = pickle.dumps(self.pageClass)
      packedSchema    = self.schema.packSchema()
      return self.binrepr.pack(self.size, self.pageSize, \
              len(packedPageClass), len(packedSchema), \
              packedPageClass, packedSchema)

  @classmethod
  def binrepr(cls, buffer):
    lenStruct = Struct("HHHH")
    (headerLen, _, pageClassLen, schemaDescLen) = lenStruct.unpack_from(buffer)
    if headerLen > 0 and pageClassLen > 0 and schemaDescLen > 0:
      return Struct("HHHH"+str(pageClassLen)+"s"+str(schemaDescLen)+"s")
    else:
      raise ValueError("Invalid header length read from storage file header")

  @classmethod
  def unpack(cls, buffer):
    brepr  = cls.binrepr(buffer)
    values = brepr.unpack_from(buffer)
    if len(values) == 6:
      pageClass = pickle.loads(values[4])
      schema    = DBSchema.unpackSchema(values[5])
      return FileHeader(pageSize=values[1], pageClass=pageClass, schema=schema)

  def toFile(self, f):
    pos = f.tell()
    if pos == 0:
      f.write(self.pack())
    else:
      raise ValueError("Cannot write file header, file positioned beyond its start.")

  @classmethod
  def fromFile(cls, f):
    pos = f.tell()
    if pos == 0:
      lenStruct = Struct("H")
      headerLen = lenStruct.unpack_from(f.peek(lenStruct.size))[0]
      if headerLen > 0:
        buffer = f.read(headerLen)
        return FileHeader.unpack(buffer)
      else:
        raise ValueError("Invalid header length read from storage file header")
    else:
      raise ValueError("Cannot read file header, file positioned beyond its start.")



class StorageFile:
  """
  A storage file implementation, as a base class for all database files.

  All storage files have a file identifier, a file path, a file header and a handle
  to a file object as metadata.

  This implementation supports a readPage() and writePage() method, enabling I/O
  for specific pages to the backing file. Allocation of new pages is handled by the
  underlying file system (i.e. simply write the desired page, and the file system 
  will grow the backing file by the desired amount).

  Storage files may also serialize their metadata using the pack() and unpack(),
  allowing their metadata to be written to disk when persisting the database catalog.

  import shutil, Storage.BufferPool, Storage.FileManager, importlib
  from Storage.BufferPool import *
  from Storage.FileManager import *
  from importlib import *
  reload(Storage.BufferPool)
  reload(Storage.FileManager)
  from Storage.BufferPool import *
  from Storage.FileManager import *


  import Storage.BufferPool
  from Storage.BufferPool import *
  f.file.close()
  schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  bp = BufferPool()
  fm = FileManager(bufferPool=bp)
  bp.setFileManager(fm)
  #
  # # Create a relation for the given schema
  fm.createRelation(schema.name, schema)
  #
  # # Below 'f' is a StorageFile object returned by the FileManager
  (fId, f) = fm.relationFile(schema.name)
  #
  # # Check initial file status
  f.numPages() == 0
  # True
  #
  # # There should be a valid free page data structure in the file.
  f.freePages is not None
  # True
  #
  # # The first available page should be at page offset 0.
  f.availablePage().pageIndex
  # 0
  #
  # # Create a pair of pages.
  pId  = PageId(fId, 0)
  pId1 = PageId(fId, 1)
  p    = SlottedPage(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
  p1   = SlottedPage(pageId=pId1, buffer=bytes(f.pageSize()), schema=schema)
  #
  # # Populate pages
  for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
     _ = p.insertTuple(tup)

  #
  for tup in [schema.pack(schema.instantiate(i, i+20)) for i in range(10, 20)]:
     _ = p1.insertTuple(tup)

    #
  # # Write out pages and sync to disk.
  f.writePage(p)
  f.writePage(p1)
  f.flush()
  #
  # # Check the number of pages, and the file size.
  f.numPages() == 2
  # True
  #
  f.size() == (f.headerSize() + f.pageSize() * 2)
  # True
  #
  # # Read pages in reverse order testing offset and page index.
  pageBuffer = bytearray(f.pageSize())
  pIn1 = f.readPage(pId1, pageBuffer)
  pIn1.pageId == pId1
  # True
  #
  f.pageOffset(pIn1.pageId) == f.header.size + f.pageSize()
  # True
  #
  pIn = f.readPage(pId, pageBuffer)
  pIn.pageId == pId
  # True
  #
  f.pageOffset(pIn.pageId) == f.header.size
  # True
  #
  # # Test page header iterator
  [p[1].usedSpace() for p in f.headers()]  # Needs BufferPool stuff to be implemented
  # [80, 80]
  #
  # # Test page iterator
  [p[1].pageId.pageIndex for p in f.pages()]
  # [0, 1]
  #
  # # Test tuple iterator
  [schema.unpack(tup).id for tup in f.tuples()] == list(range(20))
  # True
  #
  # # Check buffer pool utilization
  # >>> (bp.numPages() - bp.numFreePages()) == 2
  True

  """

  # Change this to the Page class if you want contiguous page storage in the file.
  defaultPageClass = SlottedPage

  # StorageFile constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # bufferPool   : a buffer pool instance.
  # fileId       : a PageId instance identifying this page.
  # filePath     : a PageHeader instance.
  # mode         : the file open mode. Can be one of 'create', 'update', 'truncate'.
  # Also, any keyword arguments needed to construct a FileHeader.
  def __init__(self, **kwargs):
    self.bufferPool = kwargs.get("bufferPool", None)
    if self.bufferPool is None:
      raise ValueError("No buffer pool found when initializing a storage file")

    pageSize       = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    pageClass      = kwargs.get("pageClass", StorageFile.defaultPageClass)
    schema         = kwargs.get("schema", None)
    mode           = kwargs.get("mode", None)

    self.fileId    = kwargs.get("fileId", None)
    self.filePath  = kwargs.get("filePath", None)

    header = kwargs.get("header", None)
    self.freePages = [] # Use a tuple of (PageID, free Space) or something


    # pageSize and pageClass and schema
    if mode =='create':
      print("Going through create")
      header = self.initializeHeader(**kwargs)
      self.header = header

      if self.filePath is not None:
        f = open(self.filePath, 'bw+')
        header.toFile(f)
        self.file = f

      else:
        raise ValueError("Appropriate filePath is needed")

    elif mode == 'update':
      print("Going through update")
      with open(self.filePath, 'br+') as f:
        self.header = FileHeader.fromFile(f)
        while True:
          pageIt = 0
          tempPageBytes = f.read(self.header.pageSize)
          if tempPageBytes != b'':
            pId = PageId(self.fileId, pageIt)
            tempPage = self.pageClass().unpack(pId, tempPageBytes)
            self.freePages.append((tempPage.pageId, tempPage.header.freeSpace()))
            pageIt += 1
          else:
            break


      self.file = open(self.filePath, 'ba+')

    else:
      raise ValueError("No mode provided..")
    ######################################################################################
    # DESIGN QUESTION: how do you initialize these?
    # The file should be opened depending on the desired mode of operation.
    # The file header may come from the file contents (i.e., if the file already exists),
    # otherwise it should be created from scratch.
    # Need to create the file from scratch if mode is create, get the existing header if its update.

    # self.header    = None
    # self.file      = None

    ######################################################################################
    # DESIGN QUESTION: what data structure do you use to keep track of the free pages?



  def initializeHeader(self, **kwargs):
    pageSize = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    pageClass = kwargs.get("pageClass", StorageFile.defaultPageClass)
    schema = kwargs.get("schema", None)

    if pageSize and pageClass and schema:
      return FileHeader(pageSize=pageSize, pageClass=pageClass, schema=schema)
    else:
      raise ValueError("pageSize, pageClass, and schema required to be submitted")

  # File control
  def flush(self):
    self.file.flush()

  def close(self):
    if not self.file.closed:
      self.file.close()

  # Storage file helpers
  def pageId(self, pageIndex):
    return PageId(self.fileId, pageIndex)

  def schema(self):
    return self.header.schema

  def pageSize(self):
    return self.header.pageSize

  def pageClass(self):
    return self.header.pageClass

  def size(self):
    return os.path.getsize(self.filePath)

  def headerSize(self):
    return self.header.size

  def numPages(self):
    return len(self.freePages)

  # Returns the offset in the file corresponding to the given page id.
  # Notice this assumes the header is written before the first page,
  # and is not part of the first page itself.
  def pageOffset(self, pageId):
    return self.headerSize() + self.pageSize() * pageId.pageIndex

  # Returns whether the given page id is valid for this file.
  def validPageId(self, pageId):
    return pageId.fileId == self.fileId and pageId.pageIndex < self.numPages()


  # Page header operations

  # Reads a page header from disk.
  def readPageHeader(self, pageId):
    a = 0  # Dummy input argument
    page = self.readPage(pageId, a)
    return page.header

  # Writes a page header to disk.
  # The page must already exist, that is we cannot extend the file with only a page header.
  def writePageHeader(self, page):
    raise NotImplementedError


  # Page operations

  def readPage(self, pageId, page):
    print('Reading Page')

    self.file.seek(0)
    bufferStart = self.pageOffset(pageId)
    bufferEnd = bufferStart + self.header.pageSize

    return self.pageClass().unpack(pageId, self.file.read()[bufferStart:bufferEnd])


  def writePage(self, page):
    print('Writing Page')
    self.file.write(page.pack())
    self.freePages.append((page.pageId, page.header.freeSpace()))
    return

  def updatePage(self, pageId, page):  # Custom function used to update a page when it has been written to
    bufferStart = self.pageOffset(pageId)
    self.file.seek(bufferStart)
    self.file.write(page.pack())
    self.file.seek(self.size())
    return


  # Adds a new page to the file by writing past its end.
  def allocatePage(self):
    if self.file.read() != b'':
      print('Not at end of page!')
    else:
      pageId = PageId(self.fileId, self.numPages())
      p = self.defaultPageClass(pageId=pageId, buffer=bytes(self.pageSize()), schema=self.schema())
      self.writePage(p)
    return



  # Returns the page id of the first page with available space.
  def availablePage(self):
    if len(self.freePages) > 0:
      freePages = [a for (a,b) in self.freePages if b > self.schema().size]
      if len(freePages) == 0:
        return PageId(self.fileId, self.numPages())

      return freePages[0]
    else:
      return PageId(self.fileId, 0)

    # page ID should have fileID + pageIndex (equivalent to page number)


  # Tuple operations

  # Inserts the given tuple to the first available page.
  def insertTuple(self, tupleData):
    if len(tupleData) != self.schema().size:
      print('Tuple that is being inserted does not match schema')
      return
    if len(self.freePages) == 0:  # If there are no pages allcoated to begin with
      self.allocatePage()
    if self.availablePage().pageIndex == self.numPages():  # If there is no space in any page
      self.allocatePage()

    pageInsertId = self.availablePage()  # Id of available page
    tempPage = 0  # some weird argument that isn't used
    pageToInsert = self.readPage(pageInsertId, tempPage)
    tid = pageToInsert.insertTuple(tupleData)
    newFreeSpace = pageToInsert.header.freeSpace()
    self.updatePage(pageInsertId, pageToInsert)
    location = [i for i,v in enumerate(self.freePages) if v[0] == pageInsertId]

    self.freePages[location[0]] = (pageInsertId, newFreeSpace)

    return tid


  # Removes the tuple by its id, tracking if the page is now free
  def deleteTuple(self, tupleId):
    raise NotImplementedError

  # Updates the tuple by id
  def updateTuple(self, tupleId, tupleData):
    desiredPageId = tupleId.pageId
    self.file.seek(self.pageOffset(desiredPageId))
    pageBuffer = self.file.read(self.header.pageSize)
    newPage = self.pageClass().unpack(desiredPageId, pageBuffer)
    newPage.putTuple(tupleId, tupleData)

    self.updatePage(desiredPageId, newPage)

    return

  def numTuples(self):
    tot = 0
    for tup in self.freePages:
      ph = self.readPageHeader(tup[0])
      tot += ph.numTuples()
    return tot



  # Iterators
  # Page header iterator
  def headers(self):
    return self.FileHeaderIterator(self)
  
  # Page iterator, using the buffer pool
  def pages(self):
    return self.FilePageIterator(self)

  # Unbuffered page iterator.
  # Use with care, direct pages are not authoritative if the page is present in the buffer pool.
  def directPages(self):
    return self.FileDirectPageIterator(self)

  # Tuple iterator
  def tuples(self):
    return self.FileTupleIterator(self)


  # Iterator class implementations
  class FileHeaderIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        if self.storageFile.bufferPool.hasPage(pId):
          return (pId, self.storageFile.bufferPool.getPage(pId).header)
        else:
          return (pId, self.storageFile.readPageHeader(pId))
      else:
        raise StopIteration

  class FilePageIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        return (pId, self.storageFile.bufferPool.getPage(pId))
      else:
        raise StopIteration

  class FileDirectPageIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile
      self.buffer         = bytearray(storageFile.pageSize())

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        return (pId, self.storageFile.readPage(pId, self.buffer))
      else:
        raise StopIteration

  class FileTupleIterator:
    def __init__(self, storageFile):
      self.storageFile     = storageFile
      self.pageIterator    = storageFile.pages()
      self.nextPage()

    def __iter__(self):
      return self

    def __next__(self):
      if self.pageIterator is not None:
        while self.tupleIterator is not None:
          try:
            return next(self.tupleIterator)
          except StopIteration:
            self.nextPage()
      
      if self.pageIterator is None:
        raise StopIteration

    def nextPage(self):
      try:
        self.currentPage   = next(self.pageIterator)[1]
      except StopIteration:
        self.pageIterator  = None
        self.tupleIterator = None
      else:
        self.tupleIterator = iter(self.currentPage)        


# if __name__ == "__main__":
#     import doctest
#     doctest.testmod()
