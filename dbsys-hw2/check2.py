# Query 1 - Block Nested Loop Join
query1 = db.query().fromTable('part')\
    .join(db.query()\
          .fromTable('partsupp').where('PS_AVAILQTY == 1'),
          rhsSchema=db.relationSchema('partsupp'),
          method='block-nested-loops',
          expr='P_PARTKEY == PS_PARTKEY')\
    .join(db.query().fromTable('supplier'),
          rhsSchema=db.relationSchema('supplier'),
          method='block-nested-loops',
          expr='PS_SUPPKEY == S_SUPPKEY')\
    .union(
    db.query().fromTable('part')\
        .join(db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5'),
              rhsSchema=db.relationSchema('partsupp'),
              method='block-nested-loops',
              expr='P_PARTKEY == PS_PARTKEY')\
        .join(db.query().fromTable('supplier'),
              rhsSchema=db.relationSchema('supplier'),
              method='block-nested-loops',
              expr='PS_SUPPKEY == S_SUPPKEY'))\
    .select({'P_NAME': ('P_NAME', 'char(55)'), 'S_NAME': ('S_NAME', 'char(25)')})\
    .finalize()

# Query 1 - Hash Join
query1 = db.query().fromTable('part')\
    .join(db.query()\
          .fromTable('partsupp').where('PS_AVAILQTY == 1'),
          rhsSchema=db.relationSchema('partsupp'),
          method='hash',
          lhsHashFn = 'hash(P_PARTKEY) % 111', lhsKeySchema = DBSchema('P_PARTKEY', [('P_PARTKEY', 'int')]),
          rhsHashFn = 'hash(PS_PARTKEY) % 111', rhsKeySchema=DBSchema('PS_PARTKEY', [('PS_PARTKEY', 'int')]))\
    .join(db.query().fromTable('supplier'),
          rhsSchema=db.relationSchema('supplier'),
          method='hash',
          lhsHashFn='hash(PS_SUPPKEY) % 111', lhsKeySchema=DBSchema('PS_SUPPKEY', [('PS_SUPPKEY', 'int')]),
          rhsHashFn='hash(S_SUPPKEY) % 111', rhsKeySchema=DBSchema('S_SUPPKEY', [('S_SUPPKEY', 'int')]))\
    .union(db.query().fromTable('part')\
        .join(db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5'),
              rhsSchema=db.relationSchema('partsupp'),
              method='hash',
              lhsHashFn='hash(P_PARTKEY) % 111', lhsKeySchema=DBSchema('P_PARTKEY', [('P_PARTKEY', 'int')]),
              rhsHashFn='hash(PS_PARTKEY) % 111', rhsKeySchema=DBSchema('PS_PARTKEY', [('PS_PARTKEY', 'int')]))\
        .join(db.query().fromTable('supplier'),
              rhsSchema=db.relationSchema('supplier'),
              method='hash',
              lhsHashFn='hash(PS_SUPPKEY) % 111', lhsKeySchema=DBSchema('PS_SUPPKEY', [('PS_SUPPKEY', 'int')]),
              rhsHashFn='hash(S_SUPPKEY) % 111', rhsKeySchema=DBSchema('S_SUPPKEY', [('S_SUPPKEY', 'int')]))\
    .select({'P_NAME': ('P_NAME', 'char(55)'), 'S_NAME': ('S_NAME', 'char(25)')})\
    .finalize()
