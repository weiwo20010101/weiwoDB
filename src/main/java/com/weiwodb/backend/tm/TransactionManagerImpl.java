package com.weiwodb.backend.tm;

import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;
import com.weiwodb.common.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public class TransactionManagerImpl implements TransactionManager{
    public  static final  String XID_SUFFIX=".xid";

    static  final  int  LEN_XID_HEADER_LENGTH= 8;

    private  static  final  int LEN_FIELD_SIZE= 1;

    private static  final  byte ACTIVE =1;
    private  static  final  byte COMMITTED =2;
    private  static  final  byte ABORTED =3;

    public  static final  long SUPER_XID = 0;

    private  RandomAccessFile file;
    private  FileChannel fc;
    private  long xidCounter;//xid计数器
    private Lock counterLock;//加锁

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
         this.file=raf;
        this.fc=fc;
        counterLock=new ReentrantLock();
        //检查文件头
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     */
    private void checkXIDCounter() {
        long fileLen=0;
        try {
            fileLen=file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(fileLen<LEN_XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }
        //读取long数据
        ByteBuffer buffer=ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        this.xidCounter= Parser.BytesToLong(buffer.array());//从XID文件中读取到的计数器
        long end=getXidPosition(this.xidCounter+1);//取XidCounter+1
        if(end!=fileLen){//将从文件中读取得到的xidCounter和文件实际大小进行比较
            Panic.panic(Error.BadXIDFileException);
        }
    }
    private  long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH+(xid-1)*LEN_FIELD_SIZE;
    }

    /**
     * 要求立即写入磁盘，调用FileChannel.force()方法
     * 包含添加和修改两种功能
     * @param xid
     * @param status
     */
    private  void updateXID(long xid,byte status){
        long offset=getXidPosition(xid);
        byte[]temp=new byte[LEN_FIELD_SIZE];
        temp[0]=status;
        ByteBuffer buffer=ByteBuffer.wrap(temp);
        try {
            fc.position(offset);
            fc.write(buffer);//fc -> buffer-> byte[]->new byte[]->position
        }catch (IOException e){
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
           Panic.panic(e);
        }
    }
    private void incrXidCounter(){
       xidCounter++;
        ByteBuffer buffer = ByteBuffer.wrap(Parser.longToBytes(xidCounter));
        try {
            fc.position(0);
            fc.write(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        try {
            fc.force(false);//理解写入磁盘
        }catch (Exception e){
            Panic.panic(e);
        }
    }

    /**
     * @return
     */
    public long begin(){
        counterLock.lock();
        try {
            long xid=xidCounter+1;
            updateXID(xid,ACTIVE);
            incrXidCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }
    public void commit(long xid){
        updateXID(xid,COMMITTED);
    }

    public void abort(long xid){
        updateXID(xid,ABORTED);
    }
    private  boolean checkXID(long xid,byte status){
        long offset=getXidPosition(xid);
        ByteBuffer buffer=ByteBuffer.wrap(new byte[LEN_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buffer);
        }catch (Exception e){
            Panic.panic(e);
        }
        //由于字节和字节比较，不需要转化
        return buffer.array()[0]==status;
    }
    public boolean isActive(long xid){
        //特殊处理超级事务
        if(xid== SUPER_XID) return  false;
        return  checkXID(xid,ACTIVE);
    }
    public   boolean isCommitted(long xid){
        if(xid== SUPER_XID) return true;
        return  checkXID(xid,COMMITTED);
    }
    public boolean isAborted(long xid){
        if(xid== SUPER_XID) return  false;
        return  checkXID(xid,ABORTED);
    }
    public void close(){
        try {
            fc.close();//
            file.close();//
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
