package com.weiwodb.common.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public class Parser {
    public  static  byte[] shortToBytes(short value){
        return ByteBuffer.allocate(Short.SIZE/Byte.SIZE).putShort(value).array();
    }
    public static  short BytesToShort(byte[]buf){
        ByteBuffer res = ByteBuffer.wrap(buf, 0, 2);
        return res.getShort();
    }
    public static byte[]  intToBytes(int value){
        return  ByteBuffer.allocate(Integer.SIZE/Byte.SIZE).putInt(value).array();
    }
    public static  int  BytesToInt(byte[]buf){
        ByteBuffer res = ByteBuffer.wrap(buf, 0, 4);
        return res.getInt();
    }
    public static byte[]  longToBytes(long value){
        return  ByteBuffer.allocate(Long.SIZE/Byte.SIZE).putLong(value).array();
    }
    public static  long  BytesToLong(byte[]buf){
        ByteBuffer res = ByteBuffer.wrap(buf, 0, 8);
        return res.getLong();
    }
    public static  ParseStringRes parseString(byte[]raw){
        int length=BytesToInt(Arrays.copyOf(raw,4));
        String str=new String(Arrays.copyOfRange(raw,4,length+4));
        return new ParseStringRes(str,length+4);
    }
    public static  byte[] StringToBytes(String str){//对于字符串的存储是这样的
        //在字符串开头存储四个字节，int类型的数据，表示字符串长度
        byte[] res=intToBytes(str.length());
        return Bytes.concat(res,str.getBytes());
        //Bytes.concat 来自于 依赖 package com.guava.common
    }
    public static  long StringToUid(String key){
        long seed=13331;
        long res=0;
        for(byte b:key.getBytes()){
            res=res*seed+(long)b;
        }
        return  res;
    }
}
