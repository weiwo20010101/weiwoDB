package com.weiwodb.backend.vm;

import java.util.Map;
import java.util.Set;

/**
 * @Author weiwo
 * @Created on 2022/1/27
 */
public class Transaction {
    public static final Long SUPER_XID=0L;
    public long xid;
    public int level;
    public Set<Long> snapshot;
    public Exception err;
    public boolean autoAborted;                   //当前正在活跃的事务
    public static  Transaction newTransaction(long xid,int level,Map<Long,Transaction>active){
        Transaction transaction=new Transaction();
        transaction.xid=xid;
        transaction.level=level;
        if(level==0){//如果该事务的隔离级别是提交度，那么不需要这个东西
        }else {//如果隔离级别为可重复读，需要这个
           transaction.snapshot.addAll(active.keySet());
        }
        //由于是先创建事务，后将事务放入activeTransactions中，所有哈希表中没有本事务
        return  transaction;
    }
    public  boolean isInSnapShot(long xid){
        if(xid==SUPER_XID) return  false;//超级事务特殊处理
        return snapshot.contains(xid);
    }
}
