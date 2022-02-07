package com.weiwodb.backend.dm.provide;

import com.weiwodb.backend.common.AbstractCache;
import com.weiwodb.backend.dm.dataItem.DataItem;
import com.weiwodb.backend.dm.dataItem.DataItemImpl;
import com.weiwodb.backend.dm.logger.Logger;
import com.weiwodb.backend.dm.page.Page;
import com.weiwodb.backend.dm.page.PageOne;
import com.weiwodb.backend.dm.page.PageX;
import com.weiwodb.backend.dm.pageCache.PageCache;
import com.weiwodb.backend.dm.pageIndex.PageIndex;
import com.weiwodb.backend.dm.pageIndex.PageInfo;
import com.weiwodb.backend.dm.recover.Recover;
import com.weiwodb.backend.tm.TransactionManager;
import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;
import com.weiwodb.common.utils.Types;

/**
 * @Author weiwo
 * @Created on 2022/1/27
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(TransactionManager tm, PageCache pageCache, Logger logger) {
        super(0);
        this.tm = tm;
        this.pageCache = pageCache;
        this.logger = logger;
        this.pageIndex=new PageIndex();
    }
    public void initPageOne(){//创建dm时，创建第一页
         int pageNumber=pageCache.newPage(PageOne.initRaw());
         assert  pageNumber==1;
         this.pageOne=null;
         try {
             pageOne = pageCache.getPage(pageNumber);
         }catch (Exception e){
             Panic.panic(e);
         }
         pageCache.flushPage(pageOne);
    }
    boolean loadCheckPageOne(){
        try {
            pageOne=pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return  PageOne.check(pageOne);//在open的时候校验
    }
    public void fillPageIndex(){
        int pageNumber=pageCache.getPageNumber();//
        for(int i=2;i<=pageNumber;i++){
            Page page=null;
            try {
                page=pageCache.getPage(i);
            }catch (Exception e){
                Panic.panic(e);
            }
            pageIndex.add(page.getPageNumber(), PageX.getFreeSpace(page));
            page.release();
        }

    }
    @Override
    public DataItem read(long uid) throws Exception {
      DataItemImpl dataItem=(DataItemImpl)super.get(uid);
      if(!dataItem.isValid()){
          dataItem.release();
          return  null;
      }
      return dataItem;
    }
    public void logDataItem(long xid,DataItem dataItem){
        byte[] log = Recover.updateLog(xid, dataItem);//更新日志
        logger.log(log);//真正写入日志 logger.log(log)
    }
    public  void releaseDataItem(DataItem dataItem){
        super.release(dataItem.getUid());
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] item = DataItem.rawWrapToDataItem(data);
        if(item.length>PageX.MAX_FREE_SPACE){
            Panic.panic(Error.DataTooLargeException);
        }
        PageInfo info=null;
        for(int i=0;i<5;i++){
            info = pageIndex.find(item.length);
            if(info!=null){
                break;
            }else {
                int pageNumber = pageCache.newPage(PageX.initRaw());
                pageIndex.add(pageNumber,PageX.MAX_FREE_SPACE);
            }
        }
        if(info==null){
            Panic.panic(Error.DatabaseBusyException);
        }
        //
        Page page=null;
        try {
            page=pageCache.getPage(info.pageNumber);
            //先写日志 **
            byte[] log = Recover.insertLog(xid, page, item);
            logger.log(log);
            short offset = PageX.insert(page, item);
            return  Types.addressToUid(info.pageNumber,offset);
        }finally {
            page.release();//获取缓存之后，一定要释放掉，以防缓存撑爆
           if(info!=null){//由于pageIndex.find()方法会去除掉得到的PageInfo，因此最后需要重新添加
               pageIndex.add(info.pageNumber,PageX.getFreeSpace(page));
           }else  pageIndex.add(info.pageNumber,0);
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setWhenClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        //key -> uid
        short offset= (short)(uid&((1L<<16)-1));
        uid>>>=32;
        int pageNumber=(int)(uid &((1L<<32)-1));
        Page page=pageCache.getPage(pageNumber);
        return  DataItem.parseDataItem(page,offset,this);

    }

    @Override
    protected void releaseForCache(DataItem obj) {
       obj.getPage().release();
        //读取和写入都是以页为单位的，因此写回dataItem就是就会dataItem所在的页
    }
}
