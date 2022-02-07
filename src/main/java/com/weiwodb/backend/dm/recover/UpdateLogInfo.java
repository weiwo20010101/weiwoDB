package com.weiwodb.backend.dm.recover;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
public class UpdateLogInfo {
    long xid;
    int pageNumber;
    short offset;
    byte[]oldRaw;
    byte[]newRaw;

    public UpdateLogInfo(long xid, int pageNumber, short offset, byte[] oldRaw, byte[] newRaw) {
        this.xid = xid;
        this.pageNumber = pageNumber;
        this.offset = offset;
        this.oldRaw = oldRaw;
        this.newRaw = newRaw;
    }
}
