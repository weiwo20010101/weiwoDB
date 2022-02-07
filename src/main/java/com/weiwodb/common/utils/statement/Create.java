package com.weiwodb.common.utils.statement;

public class Create {
    //create table tableName
    //fieldName fieldType 一一对应
    //...
    //index(field...)
    public String tableName;
    public String[] fieldName;
    public String[] fieldType;
    public String[] index;
}
