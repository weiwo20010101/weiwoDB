package com.weiwodb.backend.dm.recover;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
public class InsertLogInfo {
    long xid;
    int pageNumber;
    short offset;//传入PageX.recoverInsert方法中，提供页内偏移量信息
    byte[]raw;//raw[0]=1 表示valid  [dataItem]

    public InsertLogInfo(long xid, int pageNumber, short offset, byte[] raw) {
        this.xid = xid;
        this.pageNumber = pageNumber;
        this.offset = offset;
        this.raw = raw;
    }
}
