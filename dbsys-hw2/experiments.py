import sqlite3
import time

import Database
from Catalog.Schema import DBSchema

import sys
import unittest

import warnings

def sqlite_tests(db, printOutput):

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
       where p.p_partkey = l.l_partkey
         and l.l_orderkey = o.o_orderkey
         and o.o_custkey = c.c_custkey
         and c.c_nationkey = n.n_nationkey
       group by n.n_name, p.p_name
    )
    select nation, max(num)
    from temp
    group by nation;
    '''

    queries = [query1, query2, query3a, query3b]
    names = ['query1', 'query2', 'query3a', 'query3b']

    results = {}

    results[db] = []
    conn = sqlite3.connect(db)
    c = conn.cursor()

    for i in range(len(queries)):
        start = time.time()
        processedQuery = c.execute(queries[i])
        end = time.time()
        duration = end - start
        print('Database: ', db)
        print('Query: ', names[i])
        if printOutput:
            for row in processedQuery:
                print(row)
        print('Time: ', duration)

    return

def setup():
    db = Database.Database()
    return db

def query1BNL_test(db, printOutput):
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
    processedQuery=[query1.schema().unpack(tup) for page in db.processQuery(query1) for tup in page[1]]
    end = time.time()
    duration = end-start
    print('Query: Query 1, Block Nested Loops')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

def query1Hash_test(db, printOutput):
    query1 = db.query().fromTable('part') \
        .join(db.query().fromTable('partsupp').where('PS_AVAILQTY == 1'),
              rhsSchema=db.relationSchema('partsupp'),
              method='hash',
              lhsHashFn='hash(P_PARTKEY) % 111', lhsKeySchema=DBSchema('P_PARTKEY', [('P_PARTKEY', 'int')]),
              rhsHashFn='hash(PS_PARTKEY) % 111', rhsKeySchema=DBSchema('PS_PARTKEY', [('PS_PARTKEY', 'int')])) \
        .join(db.query().fromTable('supplier'),
              rhsSchema=db.relationSchema('supplier'),
              method='hash',
              lhsHashFn='hash(PS_SUPPKEY) % 111', lhsKeySchema=DBSchema('PS_SUPPKEY', [('PS_SUPPKEY', 'int')]),
              rhsHashFn='hash(S_SUPPKEY) % 111', rhsKeySchema=DBSchema('S_SUPPKEY', [('S_SUPPKEY', 'int')])) \
        .union(
        db.query().fromTable('part')
            .join(db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5'),
                  rhsSchema=db.relationSchema('partsupp'),
                  method='hash',
                  lhsHashFn='hash(P_PARTKEY) % 111', lhsKeySchema=DBSchema('P_PARTKEY', [('P_PARTKEY', 'int')]),
                  rhsHashFn='hash(PS_PARTKEY) % 111', rhsKeySchema=DBSchema('PS_PARTKEY', [('PS_PARTKEY', 'int')])) \
            .join(db.query().fromTable('supplier'),
                  rhsSchema=db.relationSchema('supplier'),
                  method='hash',
                  lhsHashFn='hash(PS_SUPPKEY) % 111', lhsKeySchema=DBSchema('PS_SUPPKEY', [('PS_SUPPKEY', 'int')]),
                  rhsHashFn='hash(S_SUPPKEY) % 111', rhsKeySchema=DBSchema('S_SUPPKEY', [('S_SUPPKEY', 'int')]))) \
        .select({'P_NAME': ('P_NAME', 'char(55)'), 'S_NAME': ('S_NAME', 'char(25)')}) \
        .finalize()

    start = time.time()
    processedQuery = [query1.schema().unpack(tup) for page in db.processQuery(query1) for tup in page[1]]
    end = time.time()
    duration = end - start
    print('Query: Query 1, Hash')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

def query2BNL_test(db, printOutput):
    query2 = db.query().fromTable('part') \
        .join(db.query().fromTable('lineitem').where('L_RETURNFLAG == \'R\''),
              rhsSchema=db.relationSchema('lineitem'),
              method='block-nested-loops',
              expr='P_PARTKEY == L_PARTKEY') \
        .groupBy(groupSchema=DBSchema('P_NAME', [('P_NAME', 'char(55)')]),
                    aggSchema=DBSchema('COUNT', [('COUNT', 'int')]),
                    groupExpr=(lambda e: e.P_NAME),
                    aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)],
                    groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .finalize()

    start = time.time()
    processedQuery = [query2.schema().unpack(tup) for page in db.processQuery(query2) for tup in page[1]]
    end = time.time()
    duration = end - start
    print('Query: Query 2, BNL')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

def query2Hash_test(db, printOutput):
    query2 = db.query().fromTable('part') \
        .join(db.query().fromTable('lineitem').where('L_RETURNFLAG == \'R\''),
              rhsSchema=db.relationSchema('lineitem'),
              method='hash',
              lhsHashFn='hash(P_PARTKEY) % 111', lhsKeySchema=DBSchema('P_PARTKEY', [('P_PARTKEY', 'int')]),
              rhsHashFn='hash(L_PARTKEY) % 111', rhsKeySchema=DBSchema('L_PARTKEY', [('L_PARTKEY', 'int')])) \
            .groupBy(groupSchema=DBSchema('P_NAME', [('P_NAME', 'char(55)')]),
                    aggSchema=DBSchema('COUNT', [('COUNT', 'int')]),
                    groupExpr=(lambda e: e.P_NAME),
                    aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)],
                    groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .finalize()

    start = time.time()
    processedQuery = [query2.schema().unpack(tup) for page in db.processQuery(query2) for tup in page[1]]
    end = time.time()
    duration = end - start
    print('Query: Query 2, Hash')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

def query3aBNL_test(db, printOutput):
    query3 = db.query().fromTable('nation') \
        .join(db.query().fromTable('customer'),
              rhsSchema=db.relationSchema('customer'),
              method='block-nested-loops',
              expr='N_NATIONKEY == C_NATIONKEY') \
        .join(db.query().fromTable('orders'),
              rhsSchema=db.relationSchema('orders'),
              method='block-nested-loops',
              expr='C_CUSTKEY == O_CUSTKEY') \
        .join(db.query().fromTable('lineitem'),
              rhsSchema=db.relationSchema('lineitem'),
              method='block-nested-loops',
              expr='O_ORDERKEY == L_ORDERKEY') \
        .join(db.query().fromTable('part'),
              rhsSchema=db.relationSchema('part'),
              method='block-nested-loops',
              expr='L_PARTKEY == P_PARTKEY') \
        .groupBy(groupSchema=DBSchema('FIRST', [('N_NAME', 'char(25)'), ('P_NAME', 'char(55)')]),
                 aggSchema=DBSchema('SUM', [('SUM', 'double')]),
                 groupExpr=(lambda e: (e.N_NAME, e.P_NAME)),
                 aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .groupBy(groupSchema=DBSchema('SECOND', [('N_NAME', 'char(25)')]),
                 aggSchema=DBSchema('MAX', [('MAX', 'double')]),
                 groupExpr=(lambda e: e.N_NAME),
                 aggExprs=[(0, lambda acc, e: max(acc, e.SUM), lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .select({'NATION': ('N_NAME', 'char(25)'), 'MAX': ('MAX', 'double')}) \
        .finalize()

    start = time.time()
    processedQuery = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
    end = time.time()
    duration = end - start
    print('Query: Query 3, BNL')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

def query3aHash_test(db, printOutput):
    query3 = db.query().fromTable('nation') \
        .join(db.query().fromTable('customer'),
              rhsSchema=db.relationSchema('customer'),
              method='hash',
              lhsHashFn='hash(N_NATIONKEY) % 111', lhsKeySchema=DBSchema('N_NATIONKEY', [('N_NATIONKEY', 'int')]),
              rhsHashFn='hash(C_NATIONKEY) % 111', rhsKeySchema=DBSchema('C_NATIONKEY', [('C_NATIONKEY', 'int')])) \
        .join(db.query().fromTable('orders'),
              rhsSchema=db.relationSchema('orders'),
              method='hash',
              lhsHashFn='hash(C_CUSTKEY) % 111', lhsKeySchema=DBSchema('C_CUSTKEY', [('C_CUSTKEY', 'int')]),
              rhsHashFn='hash(O_CUSTKEY) % 111', rhsKeySchema=DBSchema('O_CUSTKEY', [('O_CUSTKEY', 'int')])) \
        .join(db.query().fromTable('lineitem'),
              rhsSchema=db.relationSchema('lineitem'),
              method='hash',
              lhsHashFn='hash(O_ORDERKEY) % 111', lhsKeySchema=DBSchema('O_ORDERKEY', [('O_ORDERKEY', 'int')]),
              rhsHashFn='hash(L_ORDERKEY) % 111', rhsKeySchema=DBSchema('L_ORDERKEY', [('L_ORDERKEY', 'int')])) \
        .join(db.query().fromTable('part'),
              rhsSchema=db.relationSchema('part'),
              method='hash',
              lhsHashFn='hash(L_PARTKEY) % 111', lhsKeySchema=DBSchema('L_PARTKEY', [('L_PARTKEY', 'int')]),
              rhsHashFn='hash(P_PARTKEY) % 111', rhsKeySchema=DBSchema('P_PARTKEY', [('P_PARTKEY', 'int')])) \
        .groupBy(groupSchema=DBSchema('FIRST', [('N_NAME', 'char(25)'), ('P_NAME', 'char(55)')]),
                 aggSchema=DBSchema('SUM', [('SUM', 'double')]),
                 groupExpr=(lambda e: (e.N_NAME, e.P_NAME)),
                 aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .groupBy(groupSchema=DBSchema('SECOND', [('N_NAME', 'char(25)')]),
                 aggSchema=DBSchema('MAX', [('MAX', 'double')]),
                 groupExpr=(lambda e: e.N_NAME),
                 aggExprs=[(0, lambda acc, e: max(acc, e.SUM), lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .select({'NATION': ('N_NAME', 'char(25)'), 'MAX': ('MAX', 'double')}) \
        .finalize()

    start = time.time()
    processedQuery = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
    end = time.time()
    duration = end - start
    print('Query: Query 3, Hash')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

def query3bBNL_test(db, printOutput):
    query3 = db.query().fromTable('part') \
        .join(db.query().fromTable('lineitem'),
              rhsSchema=db.relationSchema('lineitem'),
              method='block-nested-loops',
              expr='L_PARTKEY == P_PARTKEY') \
        .join(db.query().fromTable('orders'),
              rhsSchema=db.relationSchema('orders'),
              method='block-nested-loops',
              expr='L_ORDERKEY == O_ORDERKEY') \
        .join(db.query().fromTable('customer'),
              rhsSchema=db.relationSchema('customer'),
              method='block-nested-loops',
              expr='O_CUSTKEY == C_CUSTKEY') \
        .join(db.query().fromTable('nation'),
              rhsSchema=db.relationSchema('nation'),
              method='block-nested-loops',
              expr='C_NATIONKEY == N_NATIONKEY') \
        .groupBy(groupSchema=DBSchema('FIRST', [('N_NAME', 'char(25)'), ('P_NAME', 'char(55)')]),
                 aggSchema=DBSchema('SUM', [('SUM', 'double')]),
                 groupExpr=(lambda e: (e.N_NAME, e.P_NAME)),
                 aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .groupBy(groupSchema=DBSchema('SECOND', [('N_NAME', 'char(25)')]),
                 aggSchema=DBSchema('MAX', [('MAX', 'double')]),
                 groupExpr=(lambda e: e.N_NAME),
                 aggExprs=[(0, lambda acc, e: max(acc, e.SUM), lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .select({'NATION': ('N_NAME', 'char(25)'), 'MAX': ('MAX', 'double')}) \
        .finalize()

    start = time.time()
    processedQuery = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
    end = time.time()
    duration = end - start
    print('Query: Query 3b (Order Reversed), BNL')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

def query3bHash_test(db, printOutput):
    query3 = db.query().fromTable('part') \
        .join(db.query().fromTable('lineitem'),
              rhsSchema=db.relationSchema('lineitem'),
              method='hash',
              lhsHashFn='hash(P_PARTKEY) % 111', lhsKeySchema=DBSchema('P_PARTKEY', [('P_PARTKEY', 'int')]),
              rhsHashFn='hash(L_PARTKEY) % 111', rhsKeySchema=DBSchema('L_PARTKEY', [('L_PARTKEY', 'int')])) \
        .join(db.query().fromTable('orders'),
              rhsSchema=db.relationSchema('orders'),
              method='hash',
              lhsHashFn='hash(L_ORDERKEY) % 111', lhsKeySchema=DBSchema('L_ORDERKEY', [('L_ORDERKEY', 'int')]),
              rhsHashFn='hash(O_ORDERKEY) % 111', rhsKeySchema=DBSchema('O_ORDERKEY', [('O_ORDERKEY', 'int')])) \
        .join(db.query().fromTable('customer'),
              rhsSchema=db.relationSchema('customer'),
              method='hash',
              lhsHashFn='hash(O_CUSTKEY) % 111', lhsKeySchema=DBSchema('O_CUSTKEY', [('O_CUSTKEY', 'int')]),
              rhsHashFn='hash(C_CUSTKEY) % 111', rhsKeySchema=DBSchema('C_CUSTKEY', [('C_CUSTKEY', 'int')])) \
        .join(db.query().fromTable('nation'),
              rhsSchema=db.relationSchema('nation'),
              method='hash',
              lhsHashFn = 'hash(C_NATIONKEY) % 111', lhsKeySchema = DBSchema('C_NATIONKEY', [('C_NATIONKEY', 'int')]),
              rhsHashFn = 'hash(N_NATIONKEY) % 111', rhsKeySchema = DBSchema('N_NATIONKEY', [('N_NATIONKEY', 'int')])) \
        .groupBy(groupSchema=DBSchema('FIRST', [('N_NAME', 'char(25)'), ('P_NAME', 'char(55)')]),
                 aggSchema=DBSchema('SUM', [('SUM', 'double')]),
                 groupExpr=(lambda e: (e.N_NAME, e.P_NAME)),
                 aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .groupBy(groupSchema=DBSchema('SECOND', [('N_NAME', 'char(25)')]),
                 aggSchema=DBSchema('MAX', [('MAX', 'double')]),
                 groupExpr=(lambda e: e.N_NAME),
                 aggExprs=[(0, lambda acc, e: max(acc, e.SUM), lambda x: x)],
                 groupHashFn=(lambda gbVal: hash(gbVal[0]) % 111)) \
        .select({'NATION': ('N_NAME', 'char(25)'), 'MAX': ('MAX', 'double')}) \
        .finalize()

    start = time.time()
    processedQuery = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]
    end = time.time()
    duration = end - start
    print('Query: Query 3b (Order Reversed), Hash')
    if printOutput:
        print('Results: ', processedQuery)
    print('Time', duration)
    return

# databases = ['HW2_0.001.db', 'HW2_0.01.db']
# print(sqlite_tests(databases[0]))
# print(sqlite_tests(databases[1]))

