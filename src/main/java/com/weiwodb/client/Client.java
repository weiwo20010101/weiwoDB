package com.weiwodb.client;


import com.weiwodb.transport.Package;
import com.weiwodb.transport.Packager;

public class Client {
    //client包装rt
    private RoundTripper rt;
    //
    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }
    //参数为字节数组也就是标准输入转化的字节数组
    public byte[] execute(byte[] stat) throws Exception {
        //将输入保证称为Package
        Package pkg=new Package(stat,null);
        //客户端先发送数据包，然后阻塞接收数据包
        Package resPkg = rt.roundTrip(pkg);
        //得到响应结果
        if(resPkg.getE() != null) {//抛出
            throw resPkg.getE();
        }
        return resPkg.getData();
    }
   //
    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
