package com.weiwodb.client;


import com.weiwodb.transport.Package;
import com.weiwodb.transport.Packager;

//起名方法，将一个过程变成一个对象，只需要加er即可
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }
    //roundTrip
    public Package roundTrip(Package pkg) throws Exception {
        //发送数据包
        packager.send(pkg);
        //接收数据包
        return packager.receive();
    }
      //来回这个过程的close，其实就是所包装的packager类的close
    //其实就是transporter的close
    //    public void close() throws IOException {
      //        //输出流结束
      //        writer.close();
      //        //然后输入流结束
      //        reader.close();
      //        //然后结束socket
      //        socket.close();
      //    }
    public void close() throws Exception {
        packager.close();
    }
}
