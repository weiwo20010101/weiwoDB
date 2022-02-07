package com.weiwodb.backend.dm.provide;

import com.weiwodb.backend.dm.dataItem.DataItem;
import com.weiwodb.backend.dm.logger.Logger;
import com.weiwodb.backend.dm.page.PageOne;
import com.weiwodb.backend.dm.pageCache.PageCache;
import com.weiwodb.backend.dm.recover.Recover;
import com.weiwodb.backend.tm.TransactionManager;

/**
 * @Author weiwo
 * @Created on 2022/1/27
 */
public interface DataManager {
   DataItem read(long uid) throws Exception; //直接从框架中获取

    //转成dataItem格式 从页面索引中查找 得到PageInfo 获取page 先写日志 转成日志格式
   long insert(long xid,byte[]raw) throws Exception;//抛出异常 暂不处理

   void  close();

   public static DataManager create(String path,long memory,TransactionManager tm){

       Logger logger=Logger.create(path);
       PageCache pageCache=PageCache.create(path,memory);//maxResource是页数，从memory到页数
       DataManagerImpl dataManager = new DataManagerImpl(tm, pageCache, logger);
       dataManager.initPageOne();//创建时maxResource=0
       return  dataManager;
               //在PageCache的构造方法中，会对memory/PageSize得到进行检查，如果小于10，就会系统异常结束抛出异常
               //        super(maxResource);
               //        if(maxResource<LEN_MIN_LIMIT){
               //            Panic.panic(Error.MemTooSmallException);
               //        }
               //同时在PageCache构造方法中根据传入的raf,fc更新页数

   }
    public static  DataManager open(String path,long memory,TransactionManager tm){
        PageCache pageCache=PageCache.open(path,memory);
       Logger l = Logger.open(path);
       DataManagerImpl dataManager = new DataManagerImpl(tm, pageCache, l);
       if(!dataManager.loadCheckPageOne()){
           Recover.recover(tm,l,pageCache);
       }
       dataManager.fillPageIndex();//打开时生成页索引
       PageOne.setWhenOpen(dataManager.pageOne);//
       //对页进行修改之后需要立即刷入磁盘
       dataManager.pageCache.flushPage(dataManager.pageOne);
       return  dataManager;
   }



}
