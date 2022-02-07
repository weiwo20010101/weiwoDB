package com.weiwodb.backend;

import com.weiwodb.backend.dm.provide.DataManager;
import com.weiwodb.backend.server.Server;
import com.weiwodb.backend.tbm.TableManager;
import com.weiwodb.backend.tm.TransactionManager;
import com.weiwodb.backend.vm.VersionManager;
import com.weiwodb.backend.vm.VersionManagerImpl;
import com.weiwodb.common.utils.Error;
import com.weiwodb.common.utils.Panic;
import org.apache.commons.cli.*;

public class Launcher {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;//64MB
    public static final long KB = 1 << 10;
	public static final long MB = 1 << 20;
	public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);//参数
        HelpFormatter f=new HelpFormatter();
        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();//服务器开始接收套接字连接
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() <= 2) {//格式错误 启动服务端失败
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = 0;
      try {
         memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
      }catch (NumberFormatException e){
          Panic.panic(Error.InvalidMemException);
      }
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
