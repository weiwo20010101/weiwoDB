package com.weiwodb.backend.dm.pageCache;
import com.weiwodb.backend.common.AbstractCache;
import com.weiwodb.backend.dm.page.Page;
import com.weiwodb.backend.dm.page.PageImpl;
import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    //操作的
    public static final String DB_SUFFIX = ".db";
    private static final int LEN_MIN_LIMIT=10;//最小的页数 小于该数会报错
    RandomAccessFile file;  //可以用于修改文件属性
    FileChannel fc;//用于读取和写入
    private Lock lock;//用于一些写入和读取操作的原子性
    private AtomicInteger pageNumbers;
    //继承抽象类，要求最大资源是int整数类型，因此取(int)(memory/PAGE_SIZE)
    public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource) {
        super(maxResource);
        if(maxResource<LEN_MIN_LIMIT){
            Panic.panic(Error.MemTooSmallException);
        }
        file=this.file;
        this.fc=fc;
        this.lock=new ReentrantLock();
        long length=0;
        try {
            length=file.length();//获取文件长度
        }catch (IOException e){
            Panic.panic(e);
        }
        this.pageNumbers=new AtomicInteger((int)length/PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNumber = pageNumbers.incrementAndGet();
         Page page = new PageImpl(pageNumber, initData, null);
        flush(page);
        return pageNumber;
    }

    @Override
    public Page getPage(int pageNumber) throws Exception {
        return  get(pageNumber);//调用缓存计数框架的方法
    }

    @Override
    public void close() {
        super.close();//清除缓存计数框架中的数据页缓存
        try {
            fc.close();//关闭FileChannel
            file.close();//关闭文件连接
        } catch (IOException e) {
            Panic.panic(e);
        }

    }
    /*
    根据pageNumber从  数据库文件(xx.db) 中读取页数据，并且包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNumber =(int)key;//获取偏移量
        long offset=pageOffset(pageNumber);//获取页号
        ByteBuffer buffer=ByteBuffer.allocate(PAGE_SIZE);//read操作分配一页
        lock.lock();
        try {
            fc.position(offset);
            fc.read(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        byte[] bytes = buffer.array();
        return new PageImpl(pageNumber,bytes,this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }

    private void flush(Page page) {
        int pageNumber = page.getPageNumber();
        long offset = pageOffset(pageNumber);
        lock.lock();
        try {
            fc.position(offset);
            ByteBuffer buffer= ByteBuffer.wrap(page.getData());
            fc.write(buffer);
            fc.force(false);//立即刷新缓存
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
    }
   /*
    对比一个页的释放 与  页缓存的关闭
    页的释放很简单，调用父类方法，释放即可
    页缓存的关闭，也是调用父类的close方法，释放所有缓存的页，然后关闭对数据库文件(.db)的连接
    */
    @Override
    public void release(Page page) {
        release(page.getPageNumber());//调用框架的释放方法
    }
    //怎么方法怎么实现呢？根据最后一页的页号截取内容，因为这里是直接修改文件的
    //因此直接调用文件对象，修改文件长度即可.


    @Override
    public void truncateByPageNumber(int maxPageNumber) {
        //修改
        //1.文件长度（物理）
        //2.页数量（抽象属性）
        long size = pageOffset(maxPageNumber);
        try {
            file.setLength(size);//
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNumber);//
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }
    private static  long pageOffset(int pageNumber){
        //页号从1开始
        return  (pageNumber-1)*PAGE_SIZE;
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }
}
