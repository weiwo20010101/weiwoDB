package com.weiwodb.backend.dm.recover;

import com.google.common.primitives.Bytes;
import com.weiwodb.backend.common.SubArray;
import com.weiwodb.backend.dm.dataItem.DataItem;
import com.weiwodb.backend.dm.logger.Logger;
import com.weiwodb.backend.dm.page.Page;
import com.weiwodb.backend.dm.page.PageX;
import com.weiwodb.backend.dm.pageCache.PageCache;
import com.weiwodb.backend.tm.TransactionManager;
import com.weiwodb.backend.tm.TransactionManagerImpl;
import com.weiwodb.common.utils.Panic;
import com.weiwodb.common.utils.Parser;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
@Slf4j
public class Recover {
    private static final  byte TYPE_INSERT=0;
    private static final  byte TYPE_UPDATE=1;

    private static final  int REDO=0;
    private static final  int UNDO=1;

    //updateLog 1 [LogType] 8 [XID] 8 [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    //insertLog 1 [LogType] 8[XID] 4[Pgno] 2[Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;
    private static final String STATUS_RECOVERING="Recovering.....";



    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache){
        say(STATUS_RECOVERING);
        logger.rewind();
        int maxPageNumber=0;
        //
        while (true){
            byte[] dataOfLog = logger.next();
            if(dataOfLog==null) break;
            if(isInsertLog(dataOfLog)){
                maxPageNumber=Math.max(parseToInsertLog(dataOfLog).pageNumber,maxPageNumber);
            }else
                maxPageNumber=Math.max(parseToUpdateLog(dataOfLog).pageNumber,maxPageNumber);
        }
        say("no logger , no page, then set pageNumber = 1");
        if(maxPageNumber==0) maxPageNumber=1;
        //恢复的时候根据所有日志记录更新文件大小 去除maxTail
        pageCache.truncateByPageNumber(maxPageNumber);
        /*
        //修改
        //1.文件长度
        //2.页数量
        long size = pageOffset(maxPageNumber);
        try {
            file.setLength(size);//
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNumber);//
         */
        say("after truncating,,,the db/page file be set to"
                +maxPageNumber+" pages\nwe reset fileLength and PageNumbers");
        say("starting redo.........");
        redoTransactions(tm,logger,pageCache);
        say("redo end..............");
        say("starting undo.........");
        undoTransactions(tm,logger,pageCache);
        say("undo end..............");
        say("recovery over.........");
    }
    private static  void say(String str){
        log.debug("{}",str);
        System.out.println(str);
    }
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        //redo insertLog updateLog
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseToInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    //如果日志不是活跃的，那就说明已经完成了
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseToUpdateLog(log);
                long xid = xi.xid;
                //如果日志不是活跃的，那就说明已经完成了
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }
    private static void undoTransactions(TransactionManager tm,Logger logger,PageCache pc){
     //通过logger遍历所有日志，tm判断事务xid状态，pc获取页面缓存，进行一定操作
        //map<Long xid ,ArrayList<Log>>map
        Map<Long, List<byte[]>> map=new HashMap<>();
        logger.rewind();
        while (true){
            byte[] dataOfLog = logger.next();//获取一条日志(size,checksum,data)的data部分
            if(dataOfLog==null) break;
            if(isInsertLog(dataOfLog)){//日志类型直接判断
                InsertLogInfo info = parseToInsertLog(dataOfLog);
                long xid = info.xid;
                if(tm.isActive(xid)){
                    //从log-->
                    if(!map.containsKey(xid)){
                        map.put(xid,new ArrayList<>());
                    }
                    map.get(xid).add(dataOfLog);
                }
                //如果已经提交，那就不操作
            }else {
                UpdateLogInfo info = parseToUpdateLog(dataOfLog);
                long xid=info.xid;
                if(tm.isActive(xid)){
                    if(!map.containsKey(xid)){
                        map.put(xid,new ArrayList<>());
                    }
                    map.get(xid).add(dataOfLog);
                }
            }
        }
        //倒序undo
      for(List<byte[]> v:map.values()){
          for(int i=v.size()-1;i>=0;i--){
              if(isInsertLog(v.get(i))){
                  doInsertLog(pc,v.get(i),UNDO);
              }else doUpdateLog(pc,v.get(i),REDO);
          }
      }
    }




   //理解该方法
   private static void doUpdateLog(PageCache pageCache,byte[]log,int flag){
        //更新日志
       int pageNumber;
       short offset;
       byte[]raw;
       UpdateLogInfo info=parseToUpdateLog(log);
       pageNumber=info.pageNumber;//根据页号获取页，根据页获取
       offset=info.offset;
       if(flag==REDO){//重做日志  需要设置新的值
           raw=info.newRaw;
       }else {
           raw=info.oldRaw;//undo 日志，需要设置为旧值
       }
       Page page=null;
       try {
           page=pageCache.getPage(pageNumber);
       } catch (Exception e) {
           Panic.panic(e);
       }
       try {
           PageX.recoverUpdate(page,raw,offset);//提供页，用于覆盖的数据，偏移量
       }finally {
     page.release();//释放缓存
       }
   }
   private static void doInsertLog(PageCache pageCache,byte[]log,int flag){
        //插入日志，如果是redo就是插入，如果是undo就是删除
       InsertLogInfo info = parseToInsertLog(log);
       Page page=null;
       try {
           page= pageCache.getPage(info.pageNumber);
       }catch (Exception e){
           Panic.panic(e);
       }
       try {
           if(flag==UNDO){
            //在MYDB中使用逻辑删除
               DataItem.setDataPartInValid(info.raw);//?仍有疑问
           }
           PageX.recoverInsert(page,info.raw,info.offset);
       }finally {
           page.release();//释放缓存 因为当时由于获取缓存，key对应的计数+1，现在用完要release
       }
   }



     private static boolean isInsertLog(byte[] dataOfLog){
        return dataOfLog[0]==TYPE_INSERT;//一条日志的第一个字节
     }
     private static boolean isUpdateLog(byte[]data){
        return  data[1]==TYPE_UPDATE;
     }
    //给定事务id，页对象，数据
   public static  byte[] insertLog(long xid, Page page,byte[]raw){
        byte[]bytes=new byte[]{TYPE_INSERT};
        byte[]xids= Parser.longToBytes(xid);
        byte[]pageNumber=Parser.intToBytes(page.getPageNumber());
        //偏移量怎么获取 由于我们知道普通页PageX的数据字节数组的头两个字节是指定空闲位置的偏移量，因此调用PageX.getFSO方法即可

        return Bytes.concat(bytes,xids,pageNumber,Parser.shortToBytes(PageX.getFSO(page)),raw);
    }
    public static  byte[] updateLog(long xid, DataItem dataItem){
        SubArray array=dataItem.getRaw();
        return  updateLog(xid,dataItem.getUid(),dataItem.getOldRaw(),
          Arrays.copyOfRange(array.raw,array.start,array.end));
    }
    private static  byte[] updateLog(long xid,long uid,byte[]oldRaw,byte[]newRaw){
        return  Bytes.concat(new byte[]{TYPE_UPDATE},Parser.longToBytes(xid),
                Parser.longToBytes(uid),oldRaw,newRaw);
    }
    private  static  InsertLogInfo parseToInsertLog(byte[]log){
        long xid=Parser.BytesToLong(Arrays.copyOfRange(log,OF_XID,OF_INSERT_PGNO));
        int pageNumber=Parser.BytesToInt(Arrays.copyOfRange(log,OF_INSERT_PGNO,OF_INSERT_OFFSET));
        short offset=Parser.BytesToShort(Arrays.copyOfRange(log,OF_INSERT_OFFSET,OF_INSERT_RAW));
        byte[]raw=Arrays.copyOfRange(log,OF_INSERT_RAW,log.length);
        return  new InsertLogInfo(xid,pageNumber,offset,raw);
    }
    private  static  UpdateLogInfo parseToUpdateLog(byte[]log){
        long xid=Parser.BytesToLong(Arrays.copyOfRange(log,OF_XID,OF_UPDATE_UID));
        long uid=Parser.BytesToLong(Arrays.copyOfRange(log,OF_UPDATE_UID,OF_UPDATE_RAW));
        //得到uid
        //字节数组中存放的是uid,转化成对象是变成pageNumber,offset
        short offset=(short)(uid & ((1L <<16) -1));//注意是1L,L不可以少
        uid>>>=32;
        int pageNumber=(int)(uid & ((1L<<32)-1));//注意是1L,L不可以少
        int length=(log.length-OF_UPDATE_RAW)/2;//oldRaw和newRaw各一半
        byte[]old=Arrays.copyOfRange(log,OF_UPDATE_RAW,OF_UPDATE_RAW+length);
        byte[]newRaw=Arrays.copyOfRange(log,OF_UPDATE_RAW+length,OF_UPDATE_RAW+length*2);//两个length
        return new UpdateLogInfo(xid,pageNumber,offset,old,newRaw);

    }
}
