package com.weiwodb.backend.im;

import com.weiwodb.backend.common.SubArray;
import com.weiwodb.backend.dm.dataItem.DataItem;
import com.weiwodb.backend.dm.provide.DataManager;
import com.weiwodb.backend.tm.TransactionManagerImpl;
import com.weiwodb.common.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class BPlusTree {
    DataManager dm; //IM基于DM
    long bootUid;
    DataItem bootDataItem;//dm.readBootUid
    //由于B+树在插入删除时，会动态调整
    // 根节点不是固定节点 专门用于存储根节点的UID  IM在操作DM时，使用的事务都是SUPER_XID
    Lock bootLock;

    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();//空根节点
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);//rootUid表示根节点的UID
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.longToBytes(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);//bootDataItem通过读取bootUid获取
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;//
        t.bootLock = new ReentrantLock();
        return t;
    }
    //ok ok
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();//获取数据部分
            //DataItem->SubArray  解析long类型数据
            return Parser.BytesToLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));//解析Long类型 8个字节
        } finally {
            bootLock.unlock();
        }
    }
    //更新 ok
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            //dm的insert寻找一个具有足够空间大小的一个地方，返回uid，也就是页号+偏移量
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);//SUPER_XID事务永远处于提交状态，可见性强
            //为了保证DataItem更新操作的原子性
            bootDataItem.before();//before()
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.longToBytes(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);//after(xid)
        } finally {
            bootLock.unlock();
        }
    }
    //ok
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            //如果不是子节点，就访问下一层，直到是子节点为止。
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }
    //ok
    //Node 类中的searchNext实现：访问目标子节点或者访问同一层的兄弟节点
    //BPlusTree类中的searchNext实现：访问下一层的目标子节点
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }
    //ok
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }
    //
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        //先从跟节点查找到leftKey对应的叶子节点
        long leafUid = searchLeaf(rootUid, leftKey);
        //确定左边界后创建uids集合，开始记录uid
        List<Long> uids = new ArrayList<>();

        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            //这是为一个唯一一处调用Node.leafSearchRange方法的地方
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            //传入参数为rootUid
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    //
    class InsertRes {
        long newNode, newKey;
    }

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

    //调用node.insertAndSplit
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            //插入分离
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
