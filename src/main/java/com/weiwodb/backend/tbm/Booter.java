package com.weiwodb.backend.tbm;

import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * @Author weiwo
 * @Created on 2022/1/28
 */

public class Booter {
    public static final String BOOTER_SUFFIX=".bt";
    public  static final String BOOTER_TEM_SUFFIX=".bt_tmp";
    String path;
    File file;
    public Booter(String path,File file){
        this.path=path;
        this.file=file;
    }
    public static  Booter create(String path){
        removeTempFile(path);
        File file=new File(path+BOOTER_SUFFIX);
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
        return new Booter(path,file);
    }
    private static void removeTempFile(String path){
        new File(path+BOOTER_TEM_SUFFIX).delete();
    }
    public static Booter open(String path){
        removeTempFile(path);
        File file=new File(path+BOOTER_SUFFIX);
        if(!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        return new Booter(path,file);
    }
    public void update(byte[]data){
        File file=new File(path+BOOTER_TEM_SUFFIX);
        if(!file.canRead()){
            Panic.panic(Error.FileCannotREADException);
        }
        if(!file.canWrite()){
            Panic.panic(Error.FileCannotWRITEException);
        }
        try {
            FileOutputStream stream=new FileOutputStream(file);
            stream.write(data);
            stream.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Files.move(file.toPath(),new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        File f=new File(path+BOOTER_SUFFIX);
        if(!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canRead()){
            Panic.panic(Error.FileCannotREADException);
        }
        if(!file.canWrite()){
            Panic.panic(Error.FileCannotWRITEException);
        }
    }
    public  byte[] load(){
        byte[]buf=null;
        try {
            buf=Files.readAllBytes(file.toPath());
        } catch (IOException e) {
          Panic.panic(e);
        }
        return  buf;
    }

}
