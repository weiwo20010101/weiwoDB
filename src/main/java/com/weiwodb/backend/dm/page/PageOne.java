package com.weiwodb.backend.dm.page;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */

import com.weiwodb.backend.dm.pageCache.PageCache;
import com.weiwodb.common.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    private static final int OFFSET = 100;
    private static final int LENGTH = 8;

    /**
     * 给PageOne初始化数据
     * @return
     */
    //        int pageNumber = pc.newPage(PageOne.InitRaw());
    public static byte[] initRaw(){
        //初始化字节数组raw
        byte[] res = new byte[PageCache.PAGE_SIZE];
        setWhenOpen(res);//给pageOne的字节数组打上随机字符
        return res;
    }
    public static void setWhenOpen(Page page){
        page.setDirty(true);//
        setWhenOpen(page.getData());
    }

    public static void setWhenOpen(byte[] data){
        byte[] bytes = RandomUtil.randomBytes(LENGTH);
        System.arraycopy(bytes,0,data,OFFSET,LENGTH);
    }
    public static boolean check(Page page){
        return  check(page.getData());
    }
    private static boolean check(byte[] data){
        return compare(Arrays.copyOfRange(data,OFFSET,OFFSET+LENGTH),
               Arrays.copyOfRange(data,OFFSET+LENGTH,OFFSET+LENGTH*2));
    }
    public static void setWhenClose(Page page){
        setWhenClose(page.getData());
    }
    private static void setWhenClose(byte[]data){
        System.arraycopy(data,OFFSET,data,OFFSET+LENGTH,LENGTH);
    }
    public  static boolean compare(byte[]b1,byte[]b2){
        if(b1.length!=b2.length) return  false;
        for(int i=0;i<b1.length;i++){
            if(b1[i]!=b2[i]) return false;
        }
        return true;
    }



}
