import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Query.Operators.TableScan import TableScan
from Utils.ExpressionInfo import ExpressionInfo

class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  We provide doctests for example usage only.
  Implementations and cost heuristics may vary.

  import Database
  db = Database.Database()
  try:
    db.createRelation('department', [('did', 'int'), ('eid', 'int')])
    db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  except ValueError:
    pass

  # Join Order Optimization
query4 = db.query().fromTable('employee').join( \
db.query().fromTable('department'), \
method='block-nested-loops', expr='id == eid').finalize()

  >>> db.optimizer.pickJoinOrder(query4)

  # Pushdown Optimization

import Database
db = Database.Database()

query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
db.query().fromTable('department'), \
method='block-nested-loops', expr='id == eid')\
.where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
.select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

newQuery = db.optimizer.pushdownOperators(query5)

  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}
    self.bestPlan = None

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    raise NotImplementedError

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    raise NotImplementedError

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    root = plan.root
    newPlan = self.singlePushDown(root)

    return Plan(root=newPlan)

  def singlePushDown(self, operator):

    if operator.operatorType() == 'Select':
      selectOperator = operator
      selectOperator.subplan = self.singlePushDown(selectOperator.subPlan)

      subPlan = selectOperator.subPlan
      subplanType = subPlan.operatorType()

      if subplanType.endswith('Join'):
        lhsPlan = subPlan.lhsPlan
        rhsPlan = subPlan.rhsPlan

        lhsFields = lhsPlan.schema().fields
        rhsFields = rhsPlan.schema().fields

        sendToLeft = ''
        sendToRight = ''
        kept = ''

        selectExprs = ExpressionInfo(selectOperator.selectExpr).decomposeCNF()

        for selectExpr in selectExprs:
          attributes = ExpressionInfo(selectExpr).getAttributes()
          for attr in attributes:
            if attr in lhsFields:
              sendToLeft += selectExpr
              sendToLeft += ' and '
            elif attr in rhsFields:
              sendToRight += selectExpr
              sendToRight += ' and '
            else:
              kept += selectExpr
              kept += ' and '

        if len(sendToLeft) > 0:
          sendToLeft = sendToLeft[:-5]
          selectOperator.subPlan.lhsPlan = self.singlePushDown(Select(selectOperator.subPlan.lhsPlan, sendToLeft))
        if len(sendToRight) > 0:
          sendToRight = sendToRight[:-5]
          selectOperator.subPlan.rhsPlan = self.singlePushDown(Select(selectOperator.subPlan.rhsPlan, sendToRight))
        if len(kept) > 0:
          kept = kept[:-5]
          return Select(selectOperator.subplan, kept)

      elif subplanType == 'UnionAll':
        subPlan.lhsPlan = self.singlePushDown(Select(subPlan.lhsPlan, selectOperator.selectExpr))
        subPlan.rhsPlan = self.singlePushDown(Select(subPlan.rhsPlan, selectOperator.selectExpr))

      else:  # We only push down selects through joins and unions
        return selectOperator

      return selectOperator.subPlan  # This is the very last return statement



    elif operator.operatorType() == 'Project':
      projectOperator = operator
      projectOperator.subPlan = self.singlePushDown(projectOperator.subPlan)

      subPlan = projectOperator.subPlan
      subplanType = subPlan.operatorType()

      if subplanType == 'Select':
        selectCriteria = ExpressionInfo(subPlan.selectExpr).getAttributes()

        for selection in selectCriteria:
          if selection not in operator.projectExprs:
            return operator

        operator.subPlan = operator.subPlan.subPlan
        operator.subPlan.subPlan = self.singlePushDown(operator)

      elif subplanType.endswith('Join'):
        lhsPlan = subPlan.lhsPlan
        rhsPlan = subPlan.rhsPlan

        lhsFields = lhsPlan.schema().fields
        rhsFields = rhsPlan.schema().fields

        sendToLeft = {}
        sendToRight = {}
        kept = {}

        projectExprs = projectOperator.projectExprs

        for key in projectExprs:
          if key in lhsFields:
            sendToLeft[key] = projectExprs[key]
          elif key in rhsFields:
            sendToRight[key] = projectExprs[key]
          else:
            kept[key] = projectExprs[key]

        if sendToLeft:
          projectOperator.subPlan.lhsPlan = self.singlePushDown(Project(projectOperator.subPlan.lhsPlan, sendToLeft))
        if sendToRight:
          projectOperator.subPlan.rhsPlan = self.singlePushDown(Project(projectOperator.subPlan.rhsPlan, sendToRight))
        if kept:
          return projectOperator  # There are project Exprs that are not join predicates

      elif subplanType == 'UnionAll':
        subPlan.lhsPlan = self.singlePushDown(Project(subPlan.lhsPlan, projectOperator.projectExprs))
        subPlan.rhsPlan = self.singlePushDown(Project(subPlan.rhsPlan, projectOperator.projectExprs))

      else:
        return operator

      return projectOperator.subPlan


    elif operator.operatorType() == 'UnionAll' or operator.operatorType().endswith('Join'):
      operator.lhsPlan = self.singlePushDown(operator.lhsPlan)
      operator.rhsPlan = self.singlePushDown(operator.rhsPlan)
      return operator

    elif operator.operatorType() == 'GroupBy':
      operator.subPlan = self.singlePushDown(operator.subPlan)
      return operator

    else:
      return operator

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    joins, tableIDs, optimalSubPlans, fields = self.optimizerSetup(plan)
    # Joins is a list of joins
    # TableIDs is a list of the operator on top of a tableScan or the scan itself (Select, Projcect)
    # optimalSubPlan is a dictionary where the key is the top operator ID (from TableID) and val is the operator
    # fields is a dictionary where key is top operator ID, val is the dictionary of fields

    if len(joins) == 0:
      return plan

    numTables = 2

    while numTables <= len(tableIDs):
      joinOrderings = itertools.combinations(tableIDs, numTables)

      # Check each ordering, check each join method
      # Start with two tables total
      # pick one as the LHS, one as the RHS


      for joinOrdering in joinOrderings:  # This iterates through subsets of size numTables
        bestCost = 1e99
        bestPlan = None

        for rhsID in joinOrdering:  # Eventually we'll even iterate through swapping 2-joins
          lhsIDs = list(joinOrdering)
          lhsIDs.remove(rhsID)  # Make this one the right side join
          lhsKey = frozenset(lhsIDs)  # Key for optimalSubPlan dict
          rhsKey = frozenset([rhsID])  # Key for optimalSubPlan dict

          cachedLHS = optimalSubPlans[lhsKey] if lhsKey in optimalSubPlans else None  # Get the optimal subPlan
          cachedRHS = optimalSubPlans[rhsKey]  # Get the optimal subPlan

          # Do we even care about doing this join?
          allAttributes = []

          for lhsID in lhsIDs:
            allAttributes.extend(fields[frozenset([lhsID])])  # These are all the attributes in the join
          allAttributes.extend(fields[rhsKey])


          for join in joins:
            if join.joinMethod == 'Hash':
              lhsCheck = join.lhsKeySchema.fields  # This is a list
              rhsCheck = join.rhsKeySchema.fields  # This is a list
              joinAttr = lhsCheck + rhsCheck

              for attr in joinAttr:
                if attr not in allAttributes:

              # make sure all attributes required for the join are in the lhs+rhs arguments
                # Need to worry about a join being just on the lhs arguments?
                # Is there only one join that will be possible ie. can we break out of loop?

            # Do a similar check for BNL and NL joins
            # Store the join expression

          # Iterate through potential join iterations
            # There's probably some way to convert a hash to BNL/NL and vice versa
            # Make the plan, prepare it, sample, get the cost, update the bestCost/bestPlan

      # Add a frozenset/Plan to the optimalSubPlans dict
    # Increment numTables
    # Save the best Plan




  def optimizerSetup(self, plan):
    joins = []
    tableIDs = []
    optimalSubPlans = {}
    capturedTableScans = []
    fields = {}

    for (num, operator) in plan.flatten():

      if isinstance(operator, Select):
        if isinstance(operator.subPlan, TableScan):
          tableIDs.append(operator.id())
          optimalSubPlans[frozenset([operator.id()])] = operator
          fields[frozenset([operator.id()])] = operator.schema().fields
          capturedTableScans.append(operator.subPlan.id())

      elif isinstance(operator, Project):
        if isinstance(operator.subPlan, TableScan):
          tableIDs.append(operator.id())
          optimalSubPlans[frozenset([operator.id()])] = operator
          fields[frozenset([operator.id()])] = operator.schema().fields
          capturedTableScans.append(operator.subPlan.id())


      elif isinstance(operator, TableScan):
        if operator.id() not in capturedTableScans:
          tableIDs.append(operator.id())
          optimalSubPlans[frozenset([operator.id()])] = operator
          fields[frozenset([operator.id()])] = operator.schema().fields

      elif isinstance(operator, Join):
        joins.append(operator)

    return joins, tableIDs, optimalSubPlans, fields


          # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan
#
if __name__ == "__main__":
  import doctest
  doctest.testmod()
