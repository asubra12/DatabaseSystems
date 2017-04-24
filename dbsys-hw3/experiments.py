import Database
from Catalog.Schema import DBSchema

import sys
import unittest
import warnings

def setup():
    db = Database.Database(dataDir='~cs416/datasets/tpch-sf0.01')
    return db

def query1(db):
    query = db.query().fromTable('lineitem')\
        .where(
            "(L_SHIPDATE >= 19940101) and (L_SHIPDATE < 19950101) and (0.06-0.01 <= L_DISCOUNT <= 0.06 + 0.01) and (L_QUANTITY < 24")\
        .groupBy(
            groupSchema=DBSchema('groupKey', [('ONE', 'int')]),
            groupExpr=(lambda e: 1),
            aggSchema=DBSchema('groupBy', [('revenue', 'float')]),
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * e.L_DISCOUNT), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 1))\
        .select(
            {'revenue': ('revenue', 'float')})\
        .finalize()
    return query

def query2(db):
    query = db.query().fromTable('part')\
        .join(
            db.query().fromTable('lineitem').where("(L_SHIPDATE >= 19950901) and (L_SHIPDATE < 19951001)"),
            method='hash',
            lhsHashFn='hash(P_PARTKEY) % 7', lhsKeySchema=DBSchema('partkey2',[('P_PARTKEY', 'int')]),
            rhsHashFn='hash(L_PARTKEY) % 7', rhsKeySchema=DBSchema('partkey1', [('L_PARTKEY', 'int')])) \
        .groupBy(
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

def query4(db):
    query = db.query().fromTable('nation')\
        .join(
            db.query().fromTable('customer'),
            method='hash',
            lhsHashFn='hash(N_NATIONKEY) % 11', lhsKeySchema=DBSchema('nationKey1', [('N_NATIONKEY', 'int')]),
            rhsHashFn='hash(C_NATIONKEY) % 11', rhsKeySchema=DBSchema('nationKey2', [('N_NATIONKEY', 'int')]))\
        .join(
            db.query().fromTable('orders'),
            method='hash',
            lhsHashFn='hash(C_CUSTKEY) % 11', lhsKeySchema=DBSchema('custKey1', [('C_CUSTKEY', 'int')]),
            rhsHashFn='hash(O_CUSTKEY) % 11', rhsKeySchema=DBSchema('custKey2', [('O_CUSTKEY', 'int')])) \
        .join(
            db.query().fromTable('lineitem'),
            method='hash',
            lhsHashFn='hash(O_ORDERKEY) % 11', lhsKeySchema=DBSchema('orderKey1', [('O_ORDERKEY', 'int')]),
            rhsHashFn='hash(L_ORDERKEY) % 11', rhsKeySchema=DBSchema('orderKey2', [('L_ORDERKEY', 'int')])) \
        .where(
            "O_ORDERDATE >= 19931001 and O_ORDERDATE < 19940101 and L_RETURNFLAG = 'R'")\
        .groupBy(
            groupSchema=DBSchema('groupKey', [('C_CUSTKEY','int'),('C_NAME','char(25)'),('C_ACCTBAL','float'),('C_PHONE','char(15)'),('N_NAME','char(25)'),('C_ADDRESS','char(40)'),('C_COMMENT','char(117)')]),
            aggSchema=DBSchema('groupAgg', [('revenue', 'float')]),
            groupExpr=(lambda e: (e.C_CUSTKEY, e.C_NAME, e.C_ACCTBAL, e.C_PHONE, e.N_NAME, e.N_NAME, e.C_ADDRESS, e.C_COMMENT)),
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
            groupHashFn=(lambda gbVal: hash(gbVal) % 11))\
        .select(
            {'c_custkey': ('C_CUSTKEY', 'int'),
             'c_name': ('C_NAME', 'char(25)'),
             'revenue': ('revenue', 'float'),
             'c_acctbal': ('C_ACCTBAL', 'float'),
             'n_name': ('N_NAME', 'char(25)'),
             'c_address': ('C_ADDRESS', 'char(40)'),
             'c_phone': ('C_PHONE', 'char(15)'),
             'c_comment': ('C_COMMENT', 'char(117)')})\
        .finalize()
    return query

def query5(db):
    query = db.query().fromTable('region')\
        .join(
            db.query().fromTable('nation'),
            method='hash',
            lhsHashFn='hash(R_REGIONKEY) % 11', lhsKeySchema=DBSchema('regionKey1', [('R_REGIONKEY', 'int')]),
            rhsHashFn='hash(N_REGIONKEY) % 11', rhsKeySchema=DBSchema('regionKey2', [('N_REGIONKEY', 'int')]))\
        .join(
            db.query().fromTable('supplier'),
            method='hash',
            lhsHashFn='hash(N_NATIONKEY) % 11', lhsKeySchema=DBSchema('nationKey1', [('N_NATIONKEY', 'int')]),
            rhsHashFn='hash(S_NATIONKEY) % 11', rhsKeySchema=DBSchema('nationKey2', [('S_NATIONKEY', 'int')])) \
        .join(
            db.query().fromTable('lineitem'),
            method='hash',
            lhsHashFn='hash(S_SUPPKEY) % 11', lhsKeySchema=DBSchema('suppKey1', [('S_SUPPKEY', 'int')]),
            rhsHashFn='hash(L_SUPPKEY) % 11', rhsKeySchema=DBSchema('suppKey2', [('L_SUPPKEY', 'int')])) \
        .join(
            db.query().fromTable('orders'),
            method='hash',
            lhsHashFn='hash(L_ORDERKEY) % 11', lhsKeySchema=DBSchema('orderKey1', [('L_ORDERKEY', 'int')]),
            rhsHashFn='hash(O_ORDERKEY) % 11', rhsKeySchema=DBSchema('orderKey2', [('O_ORDERKEY', 'int')])) \
        .join(
            db.query().fromTable('customer'),
            method='hash',
            lhsHashFn='hash(O_CUSTKEY) % 11', lhsKeySchema=DBSchema('custKey1', [('O_CUSTKEY', 'int')]),
            rhsHashFn='hash(C_CUSTKEY) % 11', rhsKeySchema=DBSchema('custKey2', [('C_CUSTKEY', 'int')])) \
        .join(
            db.query().fromTable('supplier'),
            method='hash',
            lhsHashFn='hash(C_NATIONKEY) % 11', lhsKeySchema=DBSchema('nationKey3', [('C_NATIONKEY', 'int')]),
            rhsHashFn='hash(S_NATIONKEY) % 11', rhsKeySchema=DBSchema('nationKey4', [('S_NATIONKEY', 'int')])) \
        .where(
            "R_NAME == 'ASIA' and O_ORDERDATE >= 19940101 and O_ORDERDATE < 19950101")\
        .groupBy(
            groupSchema=DBSchema('groupKey', [('N_NAME','char(25)')]),
            aggSchema=DBSchema('groupAgg', [('revenue', 'float')]),
            groupExpr=(lambda e: e.N_NAME),
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)],
            groupHashFn=(lambda gbVal: hash(gbVal) % 11))\
        .select(
            {'n_name': ('n_name', 'char(25)'),
             'revenue': ('revenue', 'float')})\
        .finalize()
    return query


db=setup()
testQuery1=query1(db)
testQuery2=query2(db)
testQuery3=query3(db)
testQuery4=query4(db)
