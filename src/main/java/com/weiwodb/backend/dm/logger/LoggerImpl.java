package com.weiwodb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;
import com.weiwodb.common.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;

/**
 * @Author weiwo
 * @Created on 2022/1/26
 * [size][checksum][data]
 */
public class LoggerImpl implements Logger{
    private static final int SEED=13331;
    private static final int OF_SIZE=0;
    private static final int OF_CHECKSUM=OF_SIZE+4;
    private static final int OF_DATA=OF_CHECKSUM+4;
    static final String LOG_SUFFIX=".log";
    private RandomAccessFile file;

    private FileChannel fc;

    private Lock lock;

    private int xCheckSum;//整个文件的checkSum

    private long fileLength;//文件长度

    private long position;//位置，在InternNext方法中大量使用

   public LoggerImpl(RandomAccessFile raf,FileChannel fc){
        this.file=raf;
        this.fc=fc;
    }
    public LoggerImpl(RandomAccessFile raf,FileChannel fc,int xCheckSum){
        this.file=raf;
        this.fc=fc;
        this.xCheckSum=xCheckSum;
    }

    /**
     * 初始化   创建文件时xcheckSum传入0
     * 初始化   打开文件时，需要检查文件长度，校验xcheckSum并且检查badTail
     */
    public void init(){
        long size=0;//获取文件长度
        try {
            size=file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        if(size<OF_CHECKSUM){
            Panic.panic(Error.BadLogFileException);
        }
        //获取checkSum
        ByteBuffer buffer=ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        int checkSum=Parser.BytesToInt(buffer.array());
        this.xCheckSum=checkSum;
        this.fileLength=size;
        //获取文件长度和校验和之后，进行校验
        checkAndRemoveTail();
    }

    private  void checkAndRemoveTail() {
        rewind();
        int check=0;
        while (true){
            byte[]log=nextLog();
            if(log==null) break;
            check=calCheckSum(check,log);
        }
        if(check!=this.xCheckSum){
            Panic.panic(Error.BadLogFileException);
        }
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        }catch (IOException e){
            Panic.panic(e);
        }
        rewind();

    }
    private  int calCheckSum(int xcheck,byte[]log){
        for(byte b:log){
            xcheck=xcheck*SEED+b;
        }
        return xcheck;
    }
    private byte[] dataChangeToLog(byte[] data) {
        //计算int checksum 转化成4个字节的字节数组
        byte[] checksum = Parser.intToBytes(calCheckSum(0,data));
        //得到int size 转化成4个字节的字节数组
        byte[] size = Parser.intToBytes(data.length);
        //主要目的是使用谷歌的方法 连接字节数组
        //很优雅
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void log(byte[] data) {
        //根据数据获取一条日志
        byte[] log = dataChangeToLog(data);
        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.unlock();
        try {
            fc.position(fc.size());
            fc.write(buffer);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
      updateCheckSum(log);
    }

    private void updateCheckSum(byte[] log) {
        lock.lock();
         xCheckSum = calCheckSum(this.xCheckSum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.intToBytes(xCheckSum)));
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }

    }

    private  byte[] nextLog(){
        if(position+OF_DATA>=fileLength){
            return  null;
        }
        ByteBuffer buffer=ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        int size=Parser.BytesToInt(buffer.array());
        //说明
        if(position+size+OF_SIZE>fileLength){
            return null;
        }
        ByteBuffer buf=ByteBuffer.allocate(OF_DATA+size);//一整条log
        try {
            fc.position(position);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        byte[] log=buf.array();
        int checkSum1=calCheckSum(0, Arrays.copyOfRange(log,OF_DATA,log.length));
        int checkSum2=Parser.BytesToInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA));
        if(checkSum1!=checkSum2){
            return null;
        }
        position+=log.length;
        return  log;
    }

    @Override
    public void truncate(long x) throws Exception {
    lock.lock();
    try {
        fc.truncate(position);
    }finally {
        lock.unlock();
    }
    }

    @Override
    public byte[] next() {
        lock.lock();
     try {
         byte[] log = nextLog();
         if (log == null) return null;
         return Arrays.copyOfRange(log, OF_DATA, log.length);
     }finally {
         lock.unlock();
     }
    }

    @Override
    public void rewind() {
    position=4;
    }

    @Override
    public void close() {
    try {
        fc.close();
        file.close();
    }catch (IOException e){
        Panic.panic(e);
    }
    }
}
