import itertools

from Catalog.Schema import DBSchema
from Query.Operator import Operator


class Join(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined join operator not supported")

    self.lhsPlan    = lhsPlan
    self.rhsPlan    = rhsPlan
    self.joinExpr   = kwargs.get("expr", None)
    self.joinMethod = kwargs.get("method", None)
    self.lhsSchema  = kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
    self.rhsSchema  = kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())

    self.lhsKeySchema   = kwargs.get("lhsKeySchema", None)
    self.rhsKeySchema   = kwargs.get("rhsKeySchema", None)
    self.lhsHashFn      = kwargs.get("lhsHashFn", None)
    self.rhsHashFn      = kwargs.get("rhsHashFn", None)
    self.blockIds       = []

    self.validateJoin()
    self.initializeSchema()
    self.initializeMethod(**kwargs)

  # Checks the join parameters.
  def validateJoin(self):
    # Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
    if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:

      raise ValueError("Invalid join method in join operator")

    # Check all fields are valid.
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      methodParams = [self.joinExpr]

    elif self.joinMethod == "indexed":
      methodParams = [self.lhsKeySchema]

    elif self.joinMethod == "hash":
      methodParams = [self.lhsHashFn, self.lhsKeySchema, \
                      self.rhsHashFn, self.rhsKeySchema]

    requireAllValid = [self.lhsPlan, self.rhsPlan, \
                       self.joinMethod, \
                       self.lhsSchema, self.rhsSchema ] \
                       + methodParams

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete join specification, missing join operator parameter")

    # For now, we assume that the LHS and RHS schema have
    # disjoint attribute names, enforcing this here.
    for lhsAttr in self.lhsSchema.fields:
      if lhsAttr in self.rhsSchema.fields:
        raise ValueError("Invalid join inputs, overlapping schema detected")


  # Initializes the output schema for this join.
  # This is a concatenation of all fields in the lhs and rhs schema.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.lhsSchema.schema() + self.rhsSchema.schema()
    self.joinSchema = DBSchema(schema, fields)

  # Initializes any additional operator parameters based on the join method.
  def initializeMethod(self, **kwargs):
    if self.joinMethod == "indexed":
      self.indexId = kwargs.get("indexId", None)
      if self.indexId is None or self.lhsKeySchema is None:
        raise ValueError("Invalid index for use in join operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.joinSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.lhsSchema, self.rhsSchema]

  # Returns a string describing the operator type
  def operatorType(self):
    readableJoinTypes = { 'nested-loops'       : 'NL'
                        , 'block-nested-loops' : 'BNL'
                        , 'indexed'            : 'Index'
                        , 'hash'               : 'Hash' }
    return readableJoinTypes[self.joinMethod] + "Join"

  # Returns child operators if present
  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for join operator.
  def __iter__(self):
    self.initializeOutput()
    return iter(self.processAllPages())

  def __next__(self):
    raise NotImplementedError

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.joinMethod == "nested-loops":
      return self.nestedLoops()

    elif self.joinMethod == "block-nested-loops":
      return self.blockNestedLoops()

    elif self.joinMethod == "indexed":
      return self.indexedNestedLoops()

    elif self.joinMethod == "hash":
      return self.hashJoin()

    else:
      raise ValueError("Invalid join method in join operator")


  ##################################
  #
  # Nested loops implementation
  #
  def nestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, rhsPage) in iter(self.rhsPlan):
          for rTuple in rhsPage:
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate the join predicate, and output if we have a match.
            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


  ##################################
  #
  # Block nested loops implementation
  #
  # This attempts to use all the free pages in the buffer pool
  # for its block of the outer relation.

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    raise NotImplementedError

  def blockJoin(self):
    for lhsPageId in self.blockIds:
      lhsPage = self.storage.bufferPool.getPage(lhsPageId, pinned=True)
      for lTuple in lhsPage:
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, _) in self.rhsPlan:
          rhsPage = self.storage.bufferPool.getPage(rPageId, pinned=True)
          # self.storage.bufferPool.pinPage(rPageId)

          for rTuple in rhsPage:
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

          self.storage.bufferPool.unpinPage(rPageId)

          if self.outputPages:
            self.outputPages = [self.outputPages[-1]]


  def blockNestedLoops(self):
    bp = self.storage.bufferPool
    freePages = bp.numFreePages() + 1  # Save one free page for the page of rhs that we read in
    # Do we need to worry about the number of output pages that we create?
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      bp.getPage(lPageId, pinned=True)
      self.blockIds.append(lPageId)

      if len(self.blockIds) == freePages:  # We've used all the pages available in the bufferPool
        self.blockJoin()
        for pId in self.blockIds:
          bp.unpinPage(pId)
        self.blockIds = []

    self.blockJoin()  # If we are able to add all the pages we want to our block
    for pId in self.blockIds:
      bp.unpinPage(pId)
    self.blockIds = []

    return self.storage.pages(self.relationId())

  ##################################
  #
  # Indexed nested loops implementation
  #
  # TODO: test
  def indexedNestedLoops(self):
    raise NotImplementedError

  ##################################
  #
  # Hash join implementation.
  #
  def hashJoin(self):
    bp = self.storage.bufferPool

    lPartitions = self.partitionPlan('L', self.lhsPlan, self.lhsHashFn, self.lhsSchema, self.lhsKeySchema)
    rPartitions = self.partitionPlan('R', self.rhsPlan, self.rhsHashFn, self.rhsSchema, self.rhsKeySchema)

    for lKey in lPartitions:  # Go through all the keys of our outer partition
      lRelId = lPartitions[lKey]
      lFile = self.storage.fileMgr.relationFile(lRelId)[1]

      if lKey in rPartitions:  # Check if the key is in our inner partition. No need for 'eval' now
        rRelId = rPartitions[lKey]
        rFile = self.storage.fileMgr.relationFile(rRelId)[1]  # Get the file of all tuples w the same hash
        lPages = lFile.pages(pinned=True)

        for (lPageId, lPage) in lPages:
          for lTuple in lPage:
            joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

            rPages = rFile.pages(pinned=True)
            for (rPageId, rPage) in rPages:
              for rTuple in rPage:
                lKeyCheck = self.lhsSchema.project(self.lhsSchema.unpack(lTuple), self.lhsKeySchema)
                rKeyCheck = self.rhsSchema.project(self.rhsSchema.unpack(rTuple), self.rhsKeySchema)

                if lKeyCheck == rKeyCheck:
                  joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
                  outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                  self.emitOutputTuple(self.joinSchema.pack(outputTuple))

              bp.unpinPage(rPageId)

          bp.unpinPage(lPageId)

    for key in lPartitions:
      self.storage.removeRelation(lPartitions[key])
    for key in rPartitions:
      self.storage.removeRelation(rPartitions[key])

    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    return self.storage.pages(self.relationId())

    # Partition both rhs and lhs into partition files
    # Read one partition file at a time
    # Block join each partition file

  def partitionPlan(self, planSide, plan, hashFn, planSchema, keySchema):
    partitionFiles = {}

    for (pageId, page) in plan:
      for tuple in page:
        joinExprEnv = self.loadSchema(planSchema, tuple)
        bucket = eval(hashFn, globals(), joinExprEnv)

        if bucket not in partitionFiles:
          relId = self.relationId() + '_' + planSide + '_' + str(bucket)
          if self.storage.hasRelation(relId):
            self.storage.removeRelation(relId)
          self.storage.createRelation(relId, planSchema)

          file = self.storage.fileMgr.relationFile(relId)[1]

          file.insertTuple(tuple)
          partitionFiles[bucket] = relId
        else:
          relId = partitionFiles[bucket]
          file = self.storage.fileMgr.relationFile(relId)[1]
          file.insertTuple(tuple)

    return partitionFiles
  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      exprs = "(expr='" + str(self.joinExpr) + "')"

    elif self.joinMethod == "indexed":
      exprs =  "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
        ))) + ")"

    elif self.joinMethod == "hash":
      exprs = "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
            "rhsKeySchema=" + self.rhsKeySchema.toString() ,
            "lhsHashFn='" + self.lhsHashFn + "'" ,
            "rhsHashFn='" + self.rhsHashFn + "'" ]
        ))) + ")"

    return super().explain() + exprs
