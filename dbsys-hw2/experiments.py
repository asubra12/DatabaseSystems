import sqlite3
import time

import Database
from Catalog.Schema import DBSchema

import sys
import unittest

import warnings

def sqlite_tests(db):

    query1 = ''' select p.p_name, s.s_name
    from part p, supplier s, partsupp ps
    where p.p_partkey = ps.ps_partkey
      and ps.ps_suppkey = s.s_suppkey
      and ps.ps_availqty = 1
    union all
    select p.p_name, s.s_name
    from part p, supplier s, partsupp ps
    where p.p_partkey = ps.ps_partkey
          and ps.ps_suppkey = s.s_suppkey
          and ps.ps_supplycost < 5;
    '''

    query2 = '''select part.p_name, count(*) as count
    from part, lineitem
    where part.p_partkey = lineitem.l_partkey and lineitem.l_returnflag = 'R'
    group by part.p_name;
    '''

    query3a = '''with temp as (
       select n.n_name as nation, p.p_name as part, sum(l.l_quantity) as num
       from customer c, nation n, orders o, lineitem l, part p
       where c.c_nationkey = n.n_nationkey
         and c.c_custkey = o.o_custkey
         and o.o_orderkey = l.l_orderkey
         and l.l_partkey = p.p_partkey
       group by n.n_name, p.p_name
    )
    select nation, max(num)
    from temp
    group by nation;
    '''

    query3b = '''with temp as (
       select n.n_name as nation, p.p_name as part, sum(l.l_quantity) as num
       from customer c, nation n, orders o, lineitem l, part p
       where c.c_nationkey = n.n_nationkey
         and o.o_orderkey = l.l_orderkey
         and c.c_custkey = o.o_custkey
         and l.l_partkey = p.p_partkey
       group by n.n_name, p.p_name
    )
    select nation, max(num)
    from temp
    group by nation;
    '''

    queries = [query1, query2, query3a, query3b]

    results = {}

    results[db] = []
    conn = sqlite3.connect(db)
    c = conn.cursor()

    for query in queries:
        start = time.time()
        c.execute(query)
        end = time.time()
        duration = end - start
        results[db].append(duration)

    return results

def setup():
    db = Database.Database()
    return db

def query1BNL_test(db):
    query1 = db.query().fromTable('part') \
        .join(db.query() \
              .fromTable('partsupp').where('PS_AVAILQTY == 1'),
              rhsSchema=db.relationSchema('partsupp'),
              method='block-nested-loops',
              expr='P_PARTKEY == PS_PARTKEY') \
        .join(db.query().fromTable('supplier'),
              rhsSchema=db.relationSchema('supplier'),
              method='block-nested-loops',
              expr='PS_SUPPKEY == S_SUPPKEY') \
        .union(
        db.query().fromTable('part') \
            .join(db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5'),
                  rhsSchema=db.relationSchema('partsupp'),
                  method='block-nested-loops',
                  expr='P_PARTKEY == PS_PARTKEY') \
            .join(db.query().fromTable('supplier'),
                  rhsSchema=db.relationSchema('supplier'),
                  method='block-nested-loops',
                  expr='PS_SUPPKEY == S_SUPPKEY')) \
        .select({'P_NAME': ('P_NAME', 'char(55)'), 'S_NAME': ('S_NAME', 'char(25)')}) \
        .finalize()

    start = time.time()
    processedQuery=db.getResults(query1)
    end = time.time()
    duration = start-end
    print('Results: ', processedQuery)
    print('Time', duration)
    return


#
# def query3a_test(db, joinMethod='hash'):
#     query = db.query().fromTable('nation').join( \
#         db.query.fromTable('customer'),
#         rhsSchema=db.relationSchema('customer'),
#         method=joinMethod,
#         lhsKeySchema=DBSchema('N_NATIONKEY', [('N_NATIONKEY', 'int')]),
#         lhsHashFn='hash(N_NATIONKEY) % 111',
#
#

    # )


    '''
    query3a = with temp as (
       select n.n_name as nation, p.p_name as part, sum(l.l_quantity) as num
       from customer c, nation n, orders o, lineitem l, part p
       where c.c_nationkey = n.n_nationkey
         and c.c_custkey = o.o_custkey
         and o.o_orderkey = l.l_orderkey
         and l.l_partkey = p.p_partkey
       group by n.n_name, p.p_name
    )
    select nation, max(num)
    from temp
    group by nation;
    '''

    '''
    schema = self.db.relationSchema('employee')
    e2schema   = schema.rename('employee2', {'id':'id2', 'age':'age2', 'dept_id': 'dept_id2'})
    keySchema  = DBSchema('employeeKey',  [('id', 'int')])
    keySchema2 = DBSchema('employeeKey2', [('id2', 'int')])
    hashJoin = self.db.query().fromTable('employee').join( \
                 self.db.query().fromTable('employee'), \
                 rhsSchema=e2schema, \
                 method='hash', \
                 lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
                 rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2, \
               ).finalize()
    results = self.getResults(hashJoin)
    self.assertEqual(len(results), self.numEmployees)
    '''
# databases = ['HW2_0.001.db', 'HW2_0.01.db']
# print(sqlite_tests(databases[0]))
# print(sqlite_tests(databases[1]))
