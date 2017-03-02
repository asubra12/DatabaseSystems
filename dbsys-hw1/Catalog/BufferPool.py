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
  fm.createRelation(schema.name, schema)
  (fId, f) = fm.relationFile(schema.name)

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
    self.pool         = io.BytesIO(b'\x00' * self.poolSize).getbuffer()

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
      currId = temp[0]

      currPage = self.pool[currId[1]:currId[1]+self.pageSize]

      self.callOrder[pageId] = self.callNum
      self.callNum += 1

      returnClass = self.fileMgr.defaultFileClass.defaultPageClass

      pageObject = returnClass.unpack(pageId, currPage)
      return pageObject

    else:
      dummyArg = 0
      currPage = self.fileMgr.readPage(pageId, dummyArg)
      freeSpot = self.freeList.index(1)
      start = freeSpot * self.pageSize
      self.freeList[freeSpot] = 0
      self.pool[start:(start+self.pageSize)] = currPage.pack()
      self.pooledPages.append((pageId, start))

      self.callOrder[pageId] = self.callNum
      self.callNum += 1

      return currPage

  def writePage(self, pageId, page):
    if self.hasPage(pageId):
      bufferStart = [b for (a,b) in self.pooledPages if a == pageId]
      bufferStart = bufferStart[0]
      bufferEnd = bufferStart + self.pageSize
      self.pool[bufferStart:bufferEnd] = page.pack()
      return
    else:
      print('That page Id is not in the bufferPool!')
      return

  # def writeNewPage(self, pageId, page):
  #
  #   freeSpot = self.freeList.index(1)
  #   start = freeSpot*self.pageSize
  #   end = start + self.pageSize
  #
  #   self.freeList[freeSpot] = 0
  #   self.pool[start:end] = page.pack()
  #   self.pooledPages.append((pageId, start))
  #
  #   self.callOrder[pageId] = self.callNum
  #   self.callNum += 1
  #
  #   return


  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    if self.hasPage(pageId):
      pooledPageTuple = [(a,b) for (a,b) in self.pooledPages if a == pageId][0]
      index = self.pooledPages.index(pooledPageTuple)
      self.freeList[index] = 0
      self.pooledPages.remove(pooledPageTuple)
      return
    else:
      print('That pageId is not in the bufferPool!')



  def flushPage(self, pageId):
    temp = [(a, b) for (a, b) in self.pooledPages if a == pageId]
    if len(temp) <= 0:
      print('That pageId is not in the bufferPool!')
      return


    currId = temp[0]
    currPageBuffer = self.pool[currId[1]:currId[1] + self.pageSize]

    currPageClass = self.fileMgr.defaultFileClass.defaultPageClass
    currPageObj = currPageClass.unpack(currId, currPageBuffer)

    if currPageObj.header.isDirty():
      self.fileMgr.writePage(currPageObj)

    return

  # Evict using LRU policy.
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    pageIdEvict = min(self.callOrder, key=self.callOrder.get)
    self.discardPage(pageIdEvict)

  # Flushes all dirty pages
  def clear(self):
    for i in self.pooledPages:
      self.flushPage(i[0])

if __name__ == "__main__":
    import doctest
    doctest.testmod()
