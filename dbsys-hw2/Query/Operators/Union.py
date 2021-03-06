from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Union(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)
    self.lhsPlan  = lhsPlan # equivalent of subplan in select.py
    self.rhsPlan  = rhsPlan
    self.validateSchema()

  # Unions must have equivalent schemas on their left and right inputs.
  def validateSchema(self):
    if self.lhsPlan.schema().match(self.rhsPlan.schema()):
      schemaName       = self.operatorType() + str(self.id())
      schemaFields     = self.lhsPlan.schema().schema()
      self.unionSchema = DBSchema(schemaName, schemaFields)
    else:
      raise ValueError("Union operator type error, mismatched input schemas")

  def schema(self):
    return self.unionSchema

  def inputSchemas(self):
    return [self.lhsPlan.schema(), self.rhsPlan.schema()]

  def operatorType(self):
    return "UnionAll"

  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for union operator.
  # The iterator must be set up to deal with input iterators and handle both pipelined and
  # non-pipelined cases
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.lhsPlan)
    self.right = False
    self.inputFinished = False

    if not self.pipelined:
      self.outputIterator = self.processAllPages()

    return self

  # Method used for iteration, doing work in the process. Handle pipelined and non-pipelined cases
  def __next__(self):
    if self.pipelined:
      while not (self.inputFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.inputIterator)
          self.processInputPage(pageId, page)
        except StopIteration:
          if not self.right:
            self.inputIterator = iter(self.rhsPlan)
            self.right = True
          else:
            self.inputFinished = True
      return self.outputPage()
    else:
      return next(self.outputIterator)


  # Page processing and control methods

  # Page-at-a-time operator processing
  # For union all, this copies over the input tuple to the output
  def processInputPage(self, pageId, page):
    for inputTuple in page:
      self.emitOutputTuple(inputTuple)

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.inputIterator is None:
      self.inputIterator = iter(self.lhsPlan)
    try:
      for (pageId, page) in self.inputIterator:
        self.processInputPage(pageId, page)

    # To support pipelined operation, processInputPage may raise a
    # StopIteration exception during its work. We catch this and ignore in batch mode.
    except StopIteration:
      pass

    # Moving to right hand side
    self.inputIterator = iter(self.rhsPlan)
    try:
      for (pageId, page) in self.inputIterator:
        self.processInputPage(pageId, page)

    # To support pipelined operation, processInputPage may raise a
    # StopIteration exception during its work. We catch this and ignore in batch mode.
    except StopIteration:
      pass

    # No need to track anything but the last output page when in batch mode.
    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())