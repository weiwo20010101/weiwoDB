package com.weiwodb.backend.dm.logger;


import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;
import com.weiwodb.common.utils.Parser;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 */
public interface Logger {
     void log(byte[] data);//将数据包装成一条日志，写入日志，刷新磁盘，同时更新checksum数据
     void truncate(long x) throws Exception;//fc.truncate(position)
     byte[] next();//返回日志数据
     void rewind();//position=4
     void close();//fc file

     /**
      * 创建一个日志文件  xxx.log 设置checkSum=0
      * @param path
      * @return
      */

   public static Logger create(String path){
        File f=new File(path+LoggerImpl.LOG_SUFFIX);
        try {
             if(!f.createNewFile()){
                  Panic.panic(Error.FileExistsException);
             }
        } catch (IOException e) {
             Panic.panic(e);
        }
        if(!f.canRead()){
             Panic.panic(Error.FileCannotREADException);
        }
        if(!f.canWrite()){
             Panic.panic(Error.FileCannotWRITEException);
        }
        RandomAccessFile file=null;
        FileChannel fc=null;
        try {
             file=new RandomAccessFile(f,"rw");
             fc=file.getChannel();
        }catch (IOException e){
             Panic.panic(e);
        }
        ByteBuffer buf=ByteBuffer.wrap(Parser.intToBytes(0));
        try {
             fc.position(0);
             fc.write(buf);//四个字节的checkSum
             fc.force(false);  //创建日志文件，立即刷入磁盘 <>><><>
        }catch (IOException e){
             Panic.panic(e);
        }
        return  new LoggerImpl(file,fc,0);
   }
   public static Logger open(String path){
        File f=new File(path+LoggerImpl.LOG_SUFFIX);
        if(!f.exists()){
             Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead()){
             Panic.panic(Error.FileCannotREADException);
        }
        if(!f.canWrite()){
             Panic.panic(Error.FileCannotWRITEException);
        }
        RandomAccessFile file=null;
        FileChannel fc=null;
        try {
             file=new RandomAccessFile(f,"rw");
             fc=file.getChannel();
        }catch (IOException e){
             Panic.panic(e);
        }

        LoggerImpl logger = new LoggerImpl(file, fc);
        //在打开文件中，需要进行一些操作 检查文件长度(因为起码首部有4个字节的checkSum)
        return  logger;

   }
}
