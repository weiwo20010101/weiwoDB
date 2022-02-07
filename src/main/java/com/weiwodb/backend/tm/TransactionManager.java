package com.weiwodb.backend.tm;

import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public interface TransactionManager {
    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();
    public static TransactionManagerImpl create(String path){
        File file=new File(path+TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!file.canRead()){
            Panic.panic(Error.FileCannotREADException);
        }
        if(!file.canWrite()){
            Panic.panic(Error.FileCannotWRITEException);
        }
        FileChannel fc=null;
        RandomAccessFile raf=null;
        try {
            raf=new RandomAccessFile(file,"rw");
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        ByteBuffer buffer= ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buffer);
        } catch (IOException e) {
           Panic.panic(e);
        }
        //fc
        return new TransactionManagerImpl(raf,fc);
    }
    public static TransactionManagerImpl open(String path) {
        File file=new File(path+TransactionManagerImpl.XID_SUFFIX);
        if(!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canRead()){
            Panic.panic(Error.FileCannotREADException);
        }
        if(!file.canWrite()){
            Panic.panic(Error.FileCannotWRITEException);
        }
        FileChannel fc=null;
        RandomAccessFile raf=null;
        try {
            raf=new RandomAccessFile(file,"rw");
            fc=raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        //
        return new TransactionManagerImpl(raf,fc);
    }

}
