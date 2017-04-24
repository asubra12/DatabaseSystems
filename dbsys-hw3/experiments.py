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
        .select( \
            {'promo_revenue' : ('promo_revenue', 'float')}).finalize()

    return query