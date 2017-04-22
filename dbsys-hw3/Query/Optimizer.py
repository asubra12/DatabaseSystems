import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
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

      return projectOperator.subPlan


    elif operator.operatorType() == 'UnionAll' or operator.operatorType().endswith('Join'):
      operator.lhsPlan = self.singlePushDown(operator.lhsPlan)
      operator.rhsPlan = self.singlePushDown(operator.rhsPlan)
      return operator

    elif operator.operatorType() == 'GroupBy':
      operator.subPlan = self.singlePushDown(operator)
      return operator

    else:
      return operator

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    raise NotImplementedError

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
