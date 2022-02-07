package com.weiwodb.backend.dm.pageCache;
import com.weiwodb.backend.dm.page.Page;
import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public interface PageCache {
    public static final int PAGE_SIZE = 1<<13;//8 k  8192
    int newPage(byte[] initData);
    Page getPage(int pageNumber) throws Exception;
    void close();
    void release(Page page);

    void truncateByPageNumber(int maxPageNumber);
    //该方法在recover时调用 先通过遍历logger获取日志对象，进而得到页号的最大值 通过这个方法压缩文件体积
    //file.set(getPageOffSet(maxPageNumber);
    //pageNumbers.set(maxPageNumber)
    int getPageNumber();
    //该方法用于获取最大页号
    //用于开启数据管理器时遍历所有页，填充页面索引时所用。通过该方法得到最大页号 for(int i=2;i<=pageNumber;i++)
    //注意页号从1开始，而第一页用于正常退出，异常退出校验
    //刷新页面
    //获取页号，再通过页号获取偏移位置 然后获取page.getData() 写入文件，同时刷入磁盘
    //用于修改第一页之后
    void flushPage(Page pg);
    /**
     * 页面缓存需要的功能
     * 1.根据字节数组（数据）创建页面
     * 2.释放页面
     * 3.根据页号获取页面
     * 4.刷新页面，将缓存刷入磁盘
     * 5.获取页面总数
     * 6.关闭页面
     * ...............
      */
    public  static PageCacheImpl create(String path,long memory){
        //创建
        File f=new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(!f.canRead()){
            Panic.panic(Error.FileCannotREADException);
        }
        if(!f.canWrite()){
            Panic.panic(Error.FileCannotWRITEException);
        }
        FileChannel fc=null;
        RandomAccessFile raf=null;
        try {
             raf=new RandomAccessFile(f,"rw");
             fc=raf.getChannel();
        }catch (IOException e){
            Panic.panic(e);
        }
        return  new PageCacheImpl(raf,fc,(int)memory/PAGE_SIZE);
    }
     public static PageCacheImpl open(String path,long memory){
        File f=new File(path+PageCacheImpl.DB_SUFFIX);// .db后缀
         if(!f.exists()){
             Panic.panic(Error.FileNotExistsException);
         }
         if(!f.canRead()){
             Panic.panic(Error.FileCannotREADException);
         }
         if(!f.canWrite()){
             Panic.panic(Error.FileCannotWRITEException);
         }
         FileChannel fc=null;
         RandomAccessFile raf=null;
         try {
             raf=new RandomAccessFile(f,"rw");
             fc=raf.getChannel();
         }catch (IOException e){
             Panic.panic(e);
         }
         //new 一个
         return new PageCacheImpl(raf,fc,(int)memory/PAGE_SIZE);
     }
    }


