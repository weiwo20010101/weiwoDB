package com.weiwodb.transport;

import com.google.common.primitives.Bytes;
import com.weiwodb.common.utils.Error;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @Author weiwo
 * @Created on 2022/1/30
 */
public class Encoder {
    public byte[] encode(Package pkg){
        if(pkg.getE()!=null){
            Exception e=pkg.getE();
            String message=e.getMessage()==null?"500 Internal Server Error":e.getMessage();
            return Bytes.concat(new byte[]{1},message.getBytes(StandardCharsets.UTF_8));
            //字符串转为字节数组使用 getbytes()方法
        }else return Bytes.concat(new byte[]{0},pkg.getData());
    }

    public Package decode(byte[]data) throws Exception{
        //检查
        if(data.length<1){
            throw Error.InvalidPkgDataException;
        }
        if(data[0]==0){
            return  new Package(Arrays.copyOf(data, data.length-1),null);
        }else if(data[0]==1){
            return new Package(null,new RuntimeException(new String(Arrays.copyOf(data,data.length-1))));
                    //字节数组转为字符串使用new String 构造方法即可
        }else{
            throw Error.InvalidPkgDataException;
        }
    }
}
