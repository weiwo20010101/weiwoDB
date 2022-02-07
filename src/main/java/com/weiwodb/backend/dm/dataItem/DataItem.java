package com.weiwodb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.weiwodb.backend.common.SubArray;
import com.weiwodb.backend.dm.page.Page;
import com.weiwodb.backend.dm.provide.DataManagerImpl;
import com.weiwodb.common.utils.Parser;
import com.weiwodb.common.utils.Types;


import java.util.Arrays;


/**
 * @Author weiwo
 * @Created on 2022/1/27
 */
public interface DataItem {
    SubArray data();
    void before();//set Dirty ,lock,system.arrayCopy
    void unBefore();//unlock arrayCopy
    void after(long xid);//dm.log 写入日志
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page getPage();//dataItem所属的页
    long getUid();//
    byte[] getOldRaw();
    SubArray getRaw();


    public static byte[] rawWrapToDataItem(byte[]raw){
        byte[] valid = new byte[1];
        byte[] size=Parser.shortToBytes((short) raw.length);
        return  Bytes.concat(valid,size,raw);
    }
    public static  DataItem parseDataItem(Page page,short offset,DataManagerImpl dm){
        byte[]raw=page.getData();
        byte[]size=Arrays.copyOfRange(raw,offset+DataItemImpl.OF_SIZE,offset+DataItemImpl.OF_DATA);
        short length=(short)(Parser.BytesToShort(size)+DataItemImpl.OF_DATA);
        byte[]old=new byte[length];
        long uid= Types.addressToUid(page.getPageNumber(),offset);
        return new DataItemImpl(new SubArray(raw,offset,offset+length),old,page,uid,dm);
    }
    public static  void setDataPartInValid(byte[]raw){
        raw[DataItemImpl.OF_VALID]=(byte)1;
    }

}
