package com.weiwodb.backend.vm;

import com.weiwodb.backend.dm.provide.DataManager;
import com.weiwodb.backend.tm.TransactionManager;

/**
 * @Author weiwo
 * @Created on 2022/1/27
 */
//vm层提供了
public interface VersionManager {
    byte[]read(long xid,long uid) throws Exception;
   long insert(long ixd,byte[]data) throws Exception;
    boolean delete(long xid,long uid)throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);
    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        //调用实现类的构造方法
        return new VersionManagerImpl(tm, dm);
    }
}
