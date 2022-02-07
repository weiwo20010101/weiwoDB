package com.weiwodb.backend.tbm;


import com.weiwodb.backend.dm.provide.DataManager;
import com.weiwodb.backend.vm.VersionManager;
import com.weiwodb.common.utils.Parser;
import com.weiwodb.common.utils.statement.*;

public interface TableManager {
    //TRANSACTION
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    //CREATE
    byte[] create(long xid, Create create) throws Exception;

    //CRUD
    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.longToBytes(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
