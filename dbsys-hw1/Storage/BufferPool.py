import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  bp = BufferPool()
  fm = Storage.FileManager.FileManager(bufferPool=bp)
  bp.setFileManager(fm)

  # Check initial buffer pool size
  len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)

    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?
    self.freeList = [1] * self.numPages()
    self.pooledPages = []
    self.callOrder = {}
    self.callNum = 0


  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return self.numPages() - len(self.freeList)

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    return pageId in [a for (a,b) in self.pooledPages]
  
  def getPage(self, pageId):
    if self.hasPage(pageId):
      temp = [(a,b) for (a,b) in self.pooledPages if a == pageId]
      currTuple = temp[0]

      currPage = self.pool[currTuple[1]:currTuple[1]+self.pageSize]

      self.callOrder[pageId] = self.callNum
      self.callNum += 1

      returnClass = self.fileMgr.defaultFileClass.defaultPageClass

      pageObject = returnClass.unpack(currPage)
      return pageObject
    else:
      dummyArg = 0
      currPage = self.fileMgr.readPage(pageId, dummyArg)
      freeSpot = self.freeList.index(1)
      start = freeSpot * self.pageSize
      self.freeList[freeSpot] = 0
      self.pool[start:start+self.pageSize] = currPage.pack()
      self.pooledPages.append((pageId, start))

      self.callOrder[pageId] = self.callNum
      self.callNum += 1

      return currPage

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    raise NotImplementedError

  def flushPage(self, pageId):
    raise NotImplementedError

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    raise NotImplementedError

  # Flushes all dirty pages
  def clear(self):
    raise NotImplementedError

if __name__ == "__main__":
    import doctest
    doctest.testmod()
