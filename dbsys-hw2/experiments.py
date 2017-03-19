import sqlite3
import time

import Database
from Catalog.Schema import DBSchema

import sys
import unittest

import warnings

def sqlite_tests():
    databases = ['HW2_0.001.db', 'HW2_0.01.db']

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

    for db in databases:
        results[db] = []
        conn = sqlite3.connect(db)
        c = conn.cursor()

        for query in queries:
            start = time.time()
            c.execute(query)
            end = time.time()
            duration = end - start
            results[db].append(query, duration)

    return results