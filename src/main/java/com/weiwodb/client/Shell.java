package com.weiwodb.client;

import java.util.Scanner;

//ok 调用client对象的.execute方法执行标准输入
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            while(true) {
                System.out.print(":> ");
                String statStr = sc.nextLine();
                if("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    //字符串转换为字节数组，客户端对象调用execute方法，返回字节数组
                    byte[] res = client.execute(statStr.getBytes());//可能抛出异常
                    //返回字符串
                    System.out.println(new String(res));
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

            }
        } finally {
            //scanner结束写入结束
            sc.close();
            //客户端结束读取执行结束
            client.close();
        }
    }
}
