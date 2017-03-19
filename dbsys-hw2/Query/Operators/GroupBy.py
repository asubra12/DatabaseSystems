from Catalog.Schema import DBSchema
from Query.Operator import Operator

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None) #groupSchema=keySchema=DBSchema('employeeKey',  [('id', 'int')])
    self.aggSchema   = kwargs.get("aggSchema", None) #aggSchema=aggMinMaxSchema=DBSchema('minmax', [('minAge', 'int'), ('maxAge','int')])
    self.groupExpr   = kwargs.get("groupExpr", None) #groupExpr=(lambda e: e.id % 2)
    self.aggExprs    = kwargs.get("aggExprs", None) #aggExprs=[(sys.maxsize, lambda acc, e: min(acc, e.age), lambda x: x), (0, lambda acc, e: max(acc, e.age), lambda x: x)]
    self.groupHashFn = kwargs.get("groupHashFn", None) #groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2)

    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    self.initializeOutput()
    #self.inputIterator = iter(self.subPlan) # is this needed?
    #self.inputIterator = False # is this needed?

    #self.outputIterator = self.processAllPages()

    return self.processAllPages()

  def __next__(self):
    return next(self.outputIterator) # is this needed?

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    bucket = self.partitionPlan(self.subPlan, self.subSchema)

    for hashKey in bucket:
      output = {}
      file = self.storage.fileMgr.relationFile(bucket[hashKey])[1]
      for (pageId, page) in file.pages():
        for myTuple in page:
          myTuple = self.subSchema.unpack(myTuple)

          tupleKey = self.groupExpr(myTuple)

          if not isinstance(tupleKey, tuple):
            if not isinstance(tupleKey, list):
              tupleKey = [tupleKey]

            tupleKey = tuple(tupleKey)

          if tupleKey not in output:
            output[tupleKey] = []
            for (a, b, c) in self.aggExprs:
              output[tupleKey].append(a)

          curr = output[tupleKey]
          new = []

          for currVal, (a, b, c) in zip(curr, self.aggExprs):
            new.append(b(currVal, myTuple))

          # updating with newest values
          output[tupleKey] = new

      for key in output:
        outputVal = output[key]
        new = []

        for currVal, (a, b, c) in zip(outputVal, self.aggExprs):
          new.append(c(currVal))

        outputTuple = self.schema().instantiate(*(list(key) + new))
        self.emitOutputTuple(self.schema().pack(outputTuple))

    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    return self.storage.pages(self.relationId())



  def partitionPlan(self, plan, planSchema):
    partitionFiles = {}

    for (pageId, page) in plan:
      for myTuple in page:
        # need to hash
        temp = self.groupExpr(self.subSchema.unpack(myTuple))

        if not isinstance(temp, tuple):

          if not isinstance(temp, list):
            temp = [temp]

          temp = tuple(temp)

        bucket = self.groupHashFn(temp)

        if bucket not in partitionFiles:
          relId = self.relationId() + '_' + str(bucket)
          if self.storage.hasRelation(relId):
            self.storage.removeRelation(relId)
          self.storage.createRelation(relId, planSchema)

          file = self.storage.fileMgr.relationFile(relId)[1]

          file.insertTuple(myTuple)
          partitionFiles[bucket] = relId

        else:
          relId = partitionFiles[bucket]
          file = self.storage.fileMgr.relationFile(relId)[1]
          file.insertTuple(myTuple)

    return partitionFiles

  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"