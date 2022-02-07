package com.weiwodb.backend.dm.page;

import com.weiwodb.backend.dm.pageCache.PageCache;
import com.weiwodb.common.utils.Parser;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 * [dataOffset]2 [data]
 * 所有操作基本上就是对FSO的操作
 */
public class PageX {
    //根据文件结构定义一些常量
    public static final short OF_FREE=0;//short fso
    public static final short OF_DATA=2;
    public  static final short MAX_FREE_SPACE= (short) (PageCache.PAGE_SIZE-OF_DATA);

    public static byte[] initRaw(){
        byte[] res=new byte[PageCache.PAGE_SIZE];
        setFSO(res,OF_DATA);//初始化设置偏移量2
        return res;
    }
    public static void setFSO(byte[] bytes,short offset){
        System.arraycopy(Parser.shortToBytes(offset),0,bytes,OF_FREE,OF_FREE);
    }
    public static short getFSO(Page page){
        return  getFSO(page.getData());
    }
    public static short  getFSO(byte[] data){
        return  Parser.BytesToShort(Arrays.copyOfRange(data,0,2));
    }
    public  static  int getFreeSpace(Page page){
        return PageCache.PAGE_SIZE-(int)getFSO(page);
    }
    public static  short insert(Page page,byte[]raw){
        page.setDirty(true);
        short offset=getFSO(page);
        System.arraycopy(raw,0,page.getData(),offset,raw.length);
        setFSO(page.getData(),(short)(offset+raw.length));
        return  offset;
    }
    /**
     * 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。
     */
    //向页中插入数据 设置脏页，获取旧的偏移量 设置为新的偏移量
     public  static  void  recoverInsert(Page page,byte[]data,short offset){
        page.setDirty(true);
        System.arraycopy(data,0,page.getData(),offset,data.length);
        short fso = getFSO(page);//旧的FSO 从文件中获取
        if(fso<offset+data.length){
            setFSO(page.getData(),(short)(offset+ data.length));
        }
    }
    /**
     * 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。
     */
    public static void recoverUpdate(Page page,byte[]data,short offset){
        page.setDirty(true);
        System.arraycopy(data,0,page.getData(),offset,data.length);
    }

}
