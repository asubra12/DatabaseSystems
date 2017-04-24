import Database
from Catalog.Schema import DBSchema

import sys
import unittest
import warnings

def setup():
    db = Database.Database()
    return db

def query1(db):
    query = db.query().fromTable('lineitem').where( \
        "(L_SHIPDATE >= 19940101) and (L_SHIPDATE < 19950101) and (0.06-0.01 <= L_DISCOUNT <= 0.06 + 0.01) and \
        (L_QUANTITY < 24").groupBy( \
        groupSchema=DBSchema('groupKey', [('ONE', 'int')]), \
        groupExpr=(lambda e: 1), \
        aggSchema=DBSchema('groupBy', [('revenue', 'float')]), \
        aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * e.L_DISCOUNT), lambda x: x)], \
        groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select({'revenue': ('revenue', 'float')}).finalize()
    return query

def query2(db):
    query = db.query().fromTable('part')\
        .join(
            db.query().fromTable('lineitem').where("(L_SHIPDATE >= 19950901) and (L_SHIPDATE < 19951001)"),
            method='hash',
            lhsHashFn='hash(P_PARTKEY) % 7', lhsKeySchema=DBSchema('partkey2',[('P_PARTKEY', 'int')]),
            rhsHashFn='hash(L_PARTKEY) % 7', rhsKeySchema=DBSchema('partkey1', [('L_PARTKEY', 'int')])) \
        .groupBy( \
            groupSchema=DBSchema('groupKey', [('ONE', 'int')]),
            aggSchema=DBSchema('groupBy', [('promo_revenue', 'float')]),
            groupExpr=(lambda e: 1),
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
            groupHashFn=(lambda gbVal: hash(gbVal) % 1))\
        .select(
            {'promo_revenue' : ('promo_revenue', 'float')}).finalize()

    return query

def query3(db):
    query = db.query().fromTable('customer')\
        .join(
            db.query().fromTable('orders'),
            method='hash',
            lhsHashFn='hash(C_CUSTKEY) % 5', lhsKeySchema=DBSchema('customerKey1', [('C_CUSTKEY', 'int')]),
            rhsHashFn='hash(O_CUSTKEY) % 5', rhsKeySchema=DBSchema('customerKey2', [('O_CUSTKEY', 'int')]))\
        .join(
            db.query().fromTable('lineitem'),
            method='hash',
            lhsHashFn='hash(O_ORDERKEY) % 5', lhsKeySchema=DBSchema('orderKey1', [('O_ORDERKEY', 'int')]),
            rhsHashFn='hash(L_ORDERKEY) % 5', rhsKeySchema=DBSchema('orderkey2', [('L_ORDERKEY', 'int')]))\
        .where(
            "C_MKTSEGMENT == 'BUILDING' and O_ORDERDATE < 19950315 and L_SHIPDATE > 19950315")\
        .groupBy(
            groupSchema=DBSchema('groupKey', [('L_ORDERKEY', 'int'), ('O_ORDERDATE', 'int'), ('O_SHIPPRIORITY', 'int')]),
            aggSchema=DBSchema('groupAgg', [('revenue', 'float')]),
            groupExpr=(lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)),
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
            groupHashFn=(lambda gbVal: hash(gbVal) % 10))\
        .select(
            {'l_orderkey': ('L_ORDERKEY', 'int'),
            'revenue': ('revenue', 'float'),
            'o_orderdate': ('O_ORDERDATE', 'int'),
            'o_shippriority': ('O_SHIPPRIORITY', 'int')})\
        .finalize()
    return query

db=setup()
testQuery1=query1(db)
testQuery2=query2(db)
testQuery3=query3(db)