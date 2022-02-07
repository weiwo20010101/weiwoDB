package com.weiwodb.backend.vm;

import com.google.common.primitives.Bytes;
import com.weiwodb.backend.common.SubArray;
import com.weiwodb.backend.dm.dataItem.DataItem;
import com.weiwodb.common.utils.Parser;

import java.util.Arrays;

/**
 * @Author weiwo
 * @Created on 2022/1/27
 * [XMIN] [XMAX] [data]
 * //共享数组subArray raw不变，但是start变化
 */
public class Entry {
    private static final  int OF_MIN=0;
    private static final  int OF_MAX=OF_MIN+8;
    private static final  int OF_DATA=OF_MAX+8;
    private long uid;//dm 缓存的key  pageNumber+offset
    private DataItem dataItem;//dm层提供的数据抽象
    private VersionManager vm;//vm管理器 vm依赖于dm，因此vm中持有dm的引用
    public static Entry newEntry(VersionManager vm,DataItem dataItem,long uid){
        Entry entry=new Entry();
        entry.dataItem=dataItem;
        entry.uid=uid;
        entry.vm=vm;
        return  entry;
    }
    public byte[] data(){
     dataItem.rLock();
     try {
         SubArray array = dataItem.data();
         byte[] data = new byte[array.end - array.start - OF_DATA];
         System.arraycopy(array.raw, array.start + OF_DATA, data, 0, data.length);
         return  data;
     }finally {
         dataItem.rUnLock();
     }
    }
    //通过传入VM和uid获取dataItem，再获取Entry
    public static Entry loadEntry(VersionManager vm,long uid) throws Exception {
        DataItem dataItem=null;
        DataItem da = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(vm,da,uid);
    }
    public  static byte[] warpEntry(long xid,byte[]data){
        //创建时的xid就是XMIN
        byte[] XMIN = Parser.longToBytes(xid);
        byte[] XMAX =new byte[8];
        return Bytes.concat(XMIN,XMAX,data);
    }

    public long getXMAX(){
        SubArray subArray = dataItem.data();
        return  Parser.BytesToLong(Arrays.copyOfRange(subArray.raw,subArray.start+OF_MAX,subArray.start+OF_DATA));

    }
    public long getXMIN(){
        SubArray subArray=dataItem.data();
        return  Parser.BytesToLong(Arrays.copyOfRange(subArray.raw,subArray.start+OF_MIN,subArray.start+OF_MAX));

    }
    public void setXMAX(long xid){
     dataItem.before();
        SubArray data = dataItem.data();
        System.arraycopy(Parser.longToBytes(xid),0,data.raw,data.start+OF_MAX,8);
     dataItem.after(xid);
    }

    public long getUid() {
        return  uid;
    }
    public void releaseEntry(){
        ((VersionManagerImpl) vm).release(this);
    }
    public void removeDataItem(){
        dataItem.release();
    }
}
