import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
#
class SlottedPageHeader:
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  len =

  import io
  buffer = io.BytesIO(bytes(4096))
  ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  ph2    = SlottedPageHeader.unpack(buffer.getbuffer())

  ## Dirty bit tests
  ph.isDirty()
  False
  ph.setDirty(True)
  ph.isDirty()

  # True
  ph.setDirty(False)
  ph.isDirty()
  # False

  ## Tuple count tests
  ph.hasFreeTuple()
  # True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  ph.nextFreeTuple() == 0
  # True

  ph.numTuples()
  # 1

  tuplesToTest = 10
  [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

  ph.numTuples() == tuplesToTest+1
  # True

  ph.hasFreeTuple()
  # True

  # Check space utilization
  ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  # True

  ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  # True

  remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  # [11, 12, ...]  # Returns None one index early for some reason

  ph.hasFreeTuple()
  # False

  # No value is returned when trying to exceed the page capacity.
  ph.nextFreeTuple() == None
  # True

  ph.freeSpace() < ph.tupleSize
  # True
  """

  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)  # Passes in a BytesIO.getbuffer() object
    if buffer:
      self.flags = kwargs.get("flags", b'\x00')
      self.tupleSize = kwargs.get("tupleSize", None)
      self.pageCapacity = kwargs.get("pageCapacity", len(buffer))
      self.nextSlot = kwargs.get("nextSlot", 0)
      self.numSlots = kwargs.get("numSlots", math.floor((self.pageCapacity - 4) / (1 + self.tupleSize)))
      self.slotBuffer = kwargs.get("slotBuffer", BytesIO(bytes(b'\x00') * self.numSlots).getbuffer())

      # Filling in the buffer:
      # NumSlots(1) + NextSlot(1) + SlotBuffer(n) + TupleSlots(tupleSize*n)
      self.binrepr = struct.Struct(("H"*2) + ("B" * self.numSlots))
      self.size = self.binrepr.size
      buffer[:self.size] = self.pack()



    else:
      raise ValueError("No backing buffer supplied for SlottedPageHeader")

  def __eq__(self, other):
    return (self.tupleSize == other.tupleSize
            and self.pageCapacity == other.pageCapacity
            and self.numSlots == other.numSlots
            and self.nextSlot == other.nextSlot
            and self.slotBuffer == other.slotBuffer
            )

  def __hash__(self):
    return hash((self.tupleSize, self.pageCapacity, self.numSlots, self.nextSlot, self.slotBuffer))

  def headerSize(self):
    return self.size

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  def numTuples(self):
    return sum(self.slotBuffer)

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - self.size - self.usedSpace()

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    return self.tupleSize * sum(self.slotBuffer)

  # Slot operations.
  def offsetOfSlot(self, slot):
    return self.size + slot*self.tupleSize

  def hasSlot(self, slotIndex):
    if self.slotBuffer[slotIndex] == 1:
      return True
    else:
      return False

  def getSlot(self, slotIndex):
    startIndex = self.offsetOfSlot(slotIndex)
    endIndex = startIndex + self.tupleSize
    return self.buffer[startIndex:endIndex].tobytes()

  def setSlot(self, slotIndex, slot):
    # Slot is what we want to set the slotindex to lol
    self.slotBuffer[slotIndex] = slot

  def resetSlot(self, slotIndex):
    self.slotBuffer[slotIndex] = 0
    return

  def freeSlots(self):
    return self.numSlots - sum(self.slotBuffer)

  def usedSlots(self):
    return sum(self.slotBuffer)

  # Tuple allocation operations.

  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    if sum(self.slotBuffer) < self.numSlots:
        return True
    else:
        return False

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    if self.usedSlots == self.numSlots:
      return None
    else:
      returnSlot = self.nextSlot
      self.slotBuffer[self.nextSlot] = 1

      for i in range(self.numSlots):
        if self.slotBuffer[i] == 0:
          self.nextSlot = i
          return returnSlot

      self.nextSlot = None
      return returnSlot

  def nextTupleRange(self):
    nextSlot = self.nextFreeTuple()
    nextSlotStart = self.offsetOfSlot(nextSlot)
    nextSlotEnd = nextSlotStart + self.tupleSize
    return (nextSlot, nextSlotStart, nextSlotEnd)


  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    returnBinRepr = struct.pack('HH', self.numSlots, self.nextSlot)
    returnBinRepr += self.slotBuffer.tobytes()

    return returnBinRepr


  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def unpack(cls, buffer):
    numSlots = struct.unpack('H', buffer[:2])[0]
    nextSlot = struct.unpack('H', buffer[2:4])[0]
    headerLen = 4 + numSlots
    slotBuffer = BytesIO(buffer[4:headerLen].tobytes()).getbuffer()
    tupleSize = math.floor((len(buffer)-headerLen) / numSlots)

    return cls(buffer=buffer, tupleSize=tupleSize, numSlots=numSlots, nextSlot=nextSlot, slotBuffer=slotBuffer)





######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage:
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  from Catalog.Identifiers import FileId, PageId, TupleId
  from Catalog.Schema      import DBSchema

  # Test harness setup.
  schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  pId    = PageId(FileId(1), 100)
  p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  e1 = schema.instantiate(1,25)
  tId = p.insertTuple(schema.pack(e1))

  len(p.pack())
  # 4096
  p2 = SlottedPage.unpack(pId, p.pack())
  p.pageId == p2.pageId
  # True
  p.header == p2.header
  # True

  tId.tupleIndex
  0

  # Retrieve the previous tuple
  e2 = schema.unpack(p.getTuple(tId))
  e2
  employee(id=1, age=25)

  # Update the tuple.
  e1 = schema.instantiate(1,28)
  p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  e3 = schema.unpack(p.getTuple(tId))
  e3
  employee(id=1, age=28)

  # Compare tuples
  e1 == e3
  True

  e2 == e3
  False

  # Check number of tuples in page
  p.header.numTuples() == 1
  True

  # Add some more tuples
  for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  p.header.numTuples()
  11

  # Test iterator
  [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  tId = TupleId(p.pageId, 0)
  sizeBeforeClear = p.header.usedSpace()
  p.clearTuple(tId)

  schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  sizeBeforeRemove = p.header.usedSpace()
  p.deleteTuple(tId)

  [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Check that the page's slots have tracked the deletion.
  p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.
  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    if buffer:
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)
      self.buffer = BytesIO(buffer).getbuffer()

      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")

      # raise NotImplementedError

    else:
      raise ValueError("No backing buffer provided to page constructor.")


  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.buffer, tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    self.filledSlots = []
    self.it = 0

    for i in range(self.header.numSlots):
      if self.header.slotBuffer[i] == 1:
        self.filledSlots.append(i)

    if len(self.filledSlots) > 0:
      self.iterSlot = self.filledSlots[self.it]
    else:
      self.iterSlot = None

    return self

  def __next__(self):
    if self.it < len(self.filledSlots):
      self.iterSlot = self.filledSlots[self.it]
      t = self.getTuple(TupleId(self.pageId, self.iterSlot))

      self.it += 1
      return t

    else:
      raise StopIteration

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    slotIndex = tupleId.tupleIndex
    if self.header.hasSlot(slotIndex):
      slotStart = self.header.offsetOfSlot(slotIndex)
      slotEnd = slotStart + self.header.tupleSize
      return self.buffer[slotStart:slotEnd]
    else:
      return None

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    slotIndex = tupleId.tupleIndex
    slotStart = self.header.offsetOfSlot(slotIndex)
    slotEnd = slotStart + self.header.tupleSize

    self.buffer[slotStart:slotEnd] = tupleData
    return


  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    if self.header.hasFreeTuple():
      (slot, tupleStart, tupleEnd) = self.header.nextTupleRange()  # Slot will be 'None' When there is no more space
      self.buffer[tupleStart:tupleEnd] = tupleData
      self.header.setSlot(slot, 1)
      tId = TupleId(self.pageId, slot)
      return tId
    else:
      return None


  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    slot = tupleId.tupleIndex
    slotStart = self.header.offsetOfSlot(slot)
    slotEnd = slotStart + self.header.tupleSize
    self.buffer[slotStart:slotEnd] = b'\x00'*self.header.tupleSize

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    slot = tupleId.tupleIndex
    self.header.setSlot(slot, 0)
    return


  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    packedHeader = self.header.pack()
    self.buffer[:self.header.size] = packedHeader
    return self.buffer.tobytes()

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    numSlots = struct.unpack('H', buffer[:2])[0]
    nextSlot = struct.unpack('H', buffer[2:4])[0]
    headerLen = 4 + numSlots
    slotBuffer = BytesIO(buffer[4:headerLen]).getbuffer()
    tupleSize = math.floor((len(buffer)-headerLen) / numSlots)
    buffer = BytesIO(buffer).getbuffer()

    header = SlottedPage.headerClass(buffer=buffer, tupleSize=tupleSize, numSlots=numSlots,
                                     nextSlot=nextSlot, slotBuffer=slotBuffer)
    return cls(buffer=buffer, pageId=pageId, header=header)






'''

  @classmethod
  def unpack(cls, buffer):
    numSlots = struct.unpack('H', buffer[:2])[0]
    nextSlot = struct.unpack('H', buffer[2:4])[0]
    headerLen = 4 + numSlots
    slotBuffer = BytesIO(buffer[4:headerLen].tobytes()).getbuffer()
    tupleSize = math.floor((len(buffer)-headerLen) / numSlots)

    return cls(buffer=buffer, tupleSize=tupleSize, numSlots=numSlots, nextSlot=nextSlot, slotBuffer=slotBuffer)


  def pack(self):
    packed_header = self.header.pack()
    self.buffer[:self.header.size] = packed_header
    return self.buffer.tobytes()

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    new_buffer = BytesIO(buffer).getbuffer()
    header = Page.headerClass.unpack(new_buffer[:Page.headerClass.size])
    return cls(buffer=new_buffer.tobytes(), pageId=pageId, header=header)

'''
if __name__ == "__main__":
    import doctest
    doctest.testmod()
