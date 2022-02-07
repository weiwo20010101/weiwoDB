package com.weiwodb.backend.dm.dataItem;

import com.weiwodb.backend.common.SubArray;
import com.weiwodb.backend.dm.page.Page;
import com.weiwodb.backend.dm.provide.DataManagerImpl;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author weiwo
 * @Created on 2022/1/27
 */
/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;//raw
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;//dm的作用 1.缓存释放，dm继承缓存框架 2.将操作记入日志
    private long uid;
    private Page pg;

    //该dataItem所属的页信息由构造方法传入
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    public boolean isValid() {//SubArray[start
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }
    /**
     * 该方法返回的数组是数据共享的，而不是拷贝实现的
     * @return
     */
    @Override
    public SubArray data() {
        //返回数据从 start+3,end
        return  new SubArray(raw.raw,raw.start+OF_DATA,raw.end);
    }
   /*
在修改之前需要调用 before() 方法，想要撤销修改时，调用 unBefore() 方法，
在修改完成后，调用 after() 方法。整个流程，
主要是为了保存前相数据，并及时落日志。DM 会保证对 DataItem 的修改是原子性的。
    */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

   @Override
   public  void after(long xid){
        dm.logDataItem(xid,this);
        wLock.unlock();
   }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page getPage() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return  raw;
    }


}
