package com.weiwodb.common.utils;

/**
 * @Author weiwo
 * @Created on 2022/1/25
 */
public class Panic {
   public static void panic(Exception e){
       e.printStackTrace();
       System.exit(1);//程序异常退出
   }
}
