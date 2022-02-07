package com.weiwodb.backend.dm.page;


import com.weiwodb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public class PageImpl  implements  Page {
    private  int PageNumber;
    private Lock lock;
    private  volatile boolean dirty;
    private  byte[]data;


    public PageImpl(int pageNumber, byte[] data, PageCache cache) {
        PageNumber = pageNumber;
        this.data = data;
        this.cache = cache;
        lock=new ReentrantLock();
    }

    //保存页面缓存的引用页面缓存
    PageCache cache;
    @Override
    public void lock() {
     lock.lock();
    }

    @Override
    public void unlock() {
     lock.unlock();
    }

    @Override
    public void release() {
     cache.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty=dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return PageNumber;
    }

    @Override
    public byte[] getData() {
        return  data;
    }

}
