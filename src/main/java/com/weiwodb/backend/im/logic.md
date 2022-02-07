- 根据rootUid和dm 我们可以通过Node.loadNode方法获取Node节点


###searchLeaf方法
- 参数 NodeUid, key
- 过程 
 - 根据uid获取Node节点，判断是否是叶子节点，如果是，返回uid，如果不是，
   那么就调用searchNext方法,searchNext方法内部使用死循环，调用Node.searchNext()方法，直到返回下一个uid
 - 递归调用searchLeaf方法
###searchRange(leftKey,rightKey)
- 首先调用上面的searchLeaf方法获取与leftKey对应的叶子节点的uid，然后在叶子节点那一行中进行搜索
- 通过调用where(true)方法，如果一个叶子中搜索不完，那就在一行中不断搜索，相当于是在B+树的叶子节点的链表上操作。
- 最后循环结果，返回uids

那么现在，search有关方法梳理结束 主要是返回uid表示子节点 下一层，而返回兄弟节点表示还在同一层

###Node.insertAndSplit()
首先调用Node.insert方法，想节点中插入数据，如果此时false，表示数据范围不对，因此本方法返回兄弟节点
如果插入成功：
然后判断是否需要分割，如果不需要那么返回res 此时res中无
如果需要分割，那么调用split方法，返回newNode,newKey(newKey为新节点的值最小的key)
然后本方法返回newNode newKey

由于Node类中维护DataItem的引用，那么在操作之后调用before，如果操作过程中出现异常，调用unbefore方法(写回数据，oldRaw->newRaw)
没有出现异常则写入更新日志
```java
public void logDataItem(long xid, DataItem di) {
byte[] log = Recover.updateLog(xid, di);// 1 [LogType] 8 [XID] 8 [UID] [OldRaw] [NewRaw]
logger.log(log);
}
```

回来BPlusTree类中规定insertAndSplit方法，其实就是添加了一个while(true)  如果返回兄弟节点，那就操作兄弟节点
如果没有返回兄弟节点，将newSon和newKey返回

在BPlusTree类中，insertAndSplit方法又被insert方法调用。
```java
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {//判断是否是叶子节点，
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            //如果不是就searchNext()
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }
```

insert方法
判断

   
