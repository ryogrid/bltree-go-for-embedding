[![test](https://github.com/ryogrid/bltree-go-for-embedding/actions/workflows/ci.yaml/badge.svg?event=push)](https://github.com/ryogrid/bltree-go-for-embedding/actions/workflows/ci.yaml)

# blink-tree-go for embedding
- This is a fork of [hmaru66/blink-tree-go](https://github.com/hmarui66/blink-tree-go)
- This fork is customized for embedding in other projects

# Customized Point
- blink-tree-go has own buffer manager
- But if you want to use blink-tree-go on your DBMS project, you should want to use your buffer manager for memory management
- For realization of memory management with your buffer manager, bltree-go-for-embedding integrates buffer manager of blink-tree-go and one of yours
  - Buffer manager (BufMgr) of bltree-go-for-embedding treat your buffer manager as some storage or something which offers persistence
- You need only to implement ParentBufMgr interface and ParentPage interface
- Then you only pass the object of ParentBufMgr interface implemented class to factory function of BufMgr and create BLTree object with it

# Note
- You need allocate fixed amount of pages to BufMgr of bltree-go-for-embedding, unfortunately your buffer manager can't page out all of pages which are used by bltree-go-for-embedding
  - The amount can be specified at call of factory function of BufMgr
- Page size can be specified in same manner with bits of page size
  - ex: 12 bits means 2^12 bytes (4096 bytes) 

# Usage example
- In [ryogrid/SamehadaDB](https://github.com/ryogrid/SamehadaDB)
  - Please see [these](https://github.com/ryogrid/SamehadaDB/tree/master/lib/container/btree) codes
