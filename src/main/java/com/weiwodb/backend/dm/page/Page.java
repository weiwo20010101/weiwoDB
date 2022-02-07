package com.weiwodb.backend.dm.page;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public interface Page {
    /**
     * 对于数据页我们需要的接口
     * 1. 加锁
     * 2. 释放锁
     * 3. 释放页
     * 4. 设置脏页
     * 5. 判断脏页
     * 6. 获取页号
     * 7. 获取页数据
     */
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
