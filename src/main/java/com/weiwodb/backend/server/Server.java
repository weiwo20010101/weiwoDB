package com.weiwodb.backend.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weiwodb.backend.tbm.TableManager;
import com.weiwodb.transport.Encoder;
import com.weiwodb.transport.Packager;
import com.weiwodb.transport.Transporter;
import com.weiwodb.backend.server.*;
import com.weiwodb.transport.Package;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author weiwo
 * @Created on 2022/1/30
 */

public class Server {
    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        //where true -> serverSocket
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                10, 20, 1L,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(100),
                new ThreadFactoryBuilder().setNameFormat("XX-task-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                Socket socket = ss.accept();//ServerSocket.accept()
                //创建一个线程池  tpe.execute(worker)
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }

    class HandleSocket implements Runnable {
        private Socket socket;
        private TableManager tbm;//表管理器

        public HandleSocket(Socket socket, TableManager tbm) {
            this.socket = socket;
            this.tbm = tbm;
        }

        @Override
        public void run() {
            InetSocketAddress address=(InetSocketAddress)socket.getRemoteSocketAddress();
            System.out.println("Establish connection: " +
                    address.getAddress().getHostAddress()+":"+address.getPort()+address.getHostName());
            Packager packager = null;
            try {
                Transporter t = new Transporter(socket);
                Encoder e = new Encoder();
                packager = new Packager(t, e);//创建packager
            } catch(IOException e) {
                e.printStackTrace();
                try {
                    socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                return;
            }
            Executor exe = new Executor(tbm);
            while(true) {
                Package pkg = null;
                try {
                    //packager接收包 Package
                    pkg = packager.receive();
                } catch(Exception e) {
                    break;
                }
                //从Package总获取sql语句的字节数组表示
                byte[] sql = pkg.getData();
                byte[] result = null;
                Exception e = null;
                try {
                    //executer类执行sql语句 得到返回结果
                    result = exe.execute(sql);
                } catch (Exception e1) {
                    e = e1;
                    e.printStackTrace();
                }
                //发送结果 获取异常
                pkg = new Package(result, e);
                try {
                    packager.send(pkg);
                } catch (Exception e1) {//如果在send过程中捕获到异常 break
                    // 比如网络问题 该连接中断 如果当前处于一个事务，那么就会调用abort方法自动舍弃当前事务
                    e1.printStackTrace();
                    break;
                }
            }

            exe.close();  //调用tbm.abort方法自动舍弃当前事务
            try {
                //
                packager.close();//关闭socket连接
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
