package com.weiwodb.backend.im;

import com.weiwodb.backend.common.SubArray;
import com.weiwodb.backend.dm.dataItem.DataItem;
import com.weiwodb.backend.tm.TransactionManagerImpl;
import com.weiwodb.common.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 *   byte       short    long
 * [LeafFlag][KeyNumber][SiblingUid]//兄弟节点存储在DM中的UId
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN] keyN始终为MAX_VALUE
 *
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;//0
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;//1
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;//3
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;//11

    static final int BALANCE_NUMBER = 32;// 平衡树的阶数  sonx keyx 字节长度为2*8
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);
    //  Node 类持有  用于方便快速修改数据和释放数据。
    BPlusTree tree;//其B+树结构的引用
    DataItem dataItem;//DataItem引用
    SubArray raw;//SubArray的引用
    long uid;//uid
    //ok
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }
    //ok
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }
    //ok  key int
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(
                Parser.shortToBytes((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }
    // short -> int
    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.BytesToShort(
                Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }
    //longg
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.longToBytes(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }
    //long
    static long getRawSibling(SubArray raw) {
        return Parser.BytesToLong(
                Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }
    //ok
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.longToBytes(uid), 0, raw.raw, offset, 8);
    }
    //ok
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.BytesToLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }
    //ok
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.longToBytes(key), 0, raw.raw, offset, 8);
    }
    //ok
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.BytesToLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }
    //从第k个节点开始拷贝 from to
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }
    //移动kth对应的节点，向右一个节点单位
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }
    /*
    生成一个根节点
     初始化两个子节点left和right,初始化left对应的键值key
     */
    static byte[] newRootRaw(long left, long right, long key)  {
        //新创建一个SubArray start =0 ,end =Node_SIZE
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);//2个键
        setRawSibling(raw, 0);//根节点无兄弟节点id
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;//字节数组
    }
    /*
    生成一个空的根节点数据
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);//是否是叶子节点
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }
    //通过uid获取节点，BPlusTree tree uid
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }
    //release
    public void release() {
        dataItem.release();//node.release->dataItem.release(
        //获取加载的时候调用了read方法，获取后需要使用Node.release
    }
    //
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }
    //ok
    class SearchNextRes {
        long uid;
        long siblingUid;
    }
    //ok Node类中的方法，用于辅助B+树做插入操作
    //寻找对应key的uid，如果找不到，返回兄弟节点的uid
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();//读取操作加 读锁
        try {
            //结果的抽象
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);//获取key的数量
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);//i是数目个数的下标
                if(key < ik) {//如果key<ik
                    res.uid = getRawKthSon(raw, i);//根据下标获取值uid
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }
    //ok
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }
    //
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        //首先 获取第一个大于等于leftKey的那个key，
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    //uids.add 哪些key对应的孩子节点的uid
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            //兄弟节点
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;//[leftKey,rightKey]这样key范围内对应的孩子节点的uid
            res.siblingUid = siblingUid;//如果没有截止，那么就将兄弟节点的uid也返回遍历下一次查找
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }
    class InsertAndSplitRes {
        //
        long siblingUid, newSon, newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();
        //没成功，说明key太大超过该节点中的key最大值，插入位置错了，因此返回兄弟节点，newSon,newKey为0
        //成功，不需要分割  返回res为空对象
        //成功，需要分割，那就分割，返回兄弟节点为空,newSon为新节点xid，newKey为该节点的第一个key
        //long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);

        dataItem.before();
        try {
            //调用insert方法
            success = insert(uid, key);
            //如果插入失败
            if(!success) {
                //返回兄弟节点  newSon and newKey 无
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    //分割
                    SplitRes r = split();

                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    //数据太大超过页，或者系统正忙
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }
    //ok 在当前节点中插入key,uid，相当于B+数中的插入操作
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        //如果插入失败，表示不应该插入到当前节点中
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        if(getRawIfLeaf(raw)) {//叶子节点
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);


            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }
    //ok
    private boolean needSplit() {
        //key个数为64的时候开始需要分离
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {//分割后返回newSon ,newKey
        long newSon, newKey;
    }
    //ok
    private SplitRes split() throws Exception {
        //一个节点分成两个 raw->raw+nodeRaw
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));//是否是叶子节点
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);//32
        setRawSibling(nodeRaw, getRawSibling(raw));//设置兄弟节点为原来的兄弟节点  ?
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        //Im在操作dm时，使用的事务都是SUPER_XID
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    //
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
