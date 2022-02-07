package com.weiwodb.backend.vm;

import com.weiwodb.backend.common.AbstractCache;
import com.weiwodb.backend.dm.provide.DataManager;
import com.weiwodb.backend.tm.TransactionManager;
import com.weiwodb.common.utils.Error;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author weiwo
 * @Created on 2022/1/27
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{
    TransactionManager tm;
    DataManager dm;
    Map<Long,Transaction> activeTransactions;
    Lock lock;
    LockTable lockTable;
    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);//  ???
        this.tm=tm;
        this.dm=dm;
        activeTransactions.put(Transaction.SUPER_XID,Transaction.newTransaction(Transaction.SUPER_XID,0,null));
        this.lock=new ReentrantLock();
    }


    @Override
    protected Entry getForCache(long key) throws Exception {
     Entry entry=Entry.loadEntry(this,key);
     //传入vm的作用？ vm依赖于dm，保存dm的引用  dm.read(key)  用于释放
        //key --> uid
        //dataItem
     return  entry;
    }
    @Override
    protected void releaseForCache(Entry entry) {
     entry.removeDataItem();//entry与dataItem一一对应 释放entry之后释放dataItem，方便写回
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        //lock
        Transaction transaction=getTransactionAndThrow(xid);
        Entry entry=null;
        try {
            entry=super.get(uid);//super 方法中已经加lock
            if(Visibility.isVisible(tm,transaction,entry)){
                return  entry.data();
            }else return null;
        }finally {
            assert entry != null;
            entry.releaseEntry();
        }

    }

    @Override
    public long  insert(long xid, byte[] data) throws Exception {
        //先将原始数据保证，无脑调用
       Transaction transaction=getTransactionAndThrow(xid);
       byte[]entry=Entry.warpEntry(xid,data);
       return  dm.insert(xid,entry);//entry作为dataItem的data先被包装
    }
    private Transaction getTransactionAndThrow(long xid) throws Exception {
        lock.lock();
        Transaction transaction=activeTransactions.get(xid);
        lock.unlock();
        if(transaction.err!=null){
            throw  transaction.err;
        }
        return transaction;
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
    return false;
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long number = tm.begin();
            Transaction transaction =
                    Transaction.newTransaction(number, level, activeTransactions);
            activeTransactions.put(number,transaction);
            return  number;
        }finally {
            lock.unlock();
        }
    }

    /**
     * 1.从活跃的事务中获取相关事务对象
     * 2.如果事务有一异常，就抛出
     * 3.捕获空指针异常 系统异常结束
     * 4.从lockTable中删除xid
     * 5.调用事务管理器提交xid
     * @param xid
     * @throws Exception
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransactions.get(xid);
        lock.unlock();
        try {
            if (transaction.err != null) {
                throw transaction.err;
            }
        } catch (NullPointerException e) {
            e.printStackTrace();

        }
        activeTransactions.remove(xid);
        lockTable.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {

    }
    private void abort(long xid,boolean autoAborted){//是否手动将事务从当前活跃事务中去除
       lock.lock();
        Transaction transaction = activeTransactions.remove(xid);
        if(!autoAborted){
            activeTransactions.remove(xid);
        }
        if(transaction.autoAborted) return;//判断事务是否是自动abort
        lockTable.remove(xid);
        tm.abort(xid);
    }

    public void release(Entry entry){
      super.release(entry.getUid());
    }
}
