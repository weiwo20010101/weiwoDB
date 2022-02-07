package com.weiwodb.backend.tbm;
//由于表管理器已经是被最外层的Server调用，这些方法直接返回执行的结果，
// 例如错误信息或者结果信息的字节数组
public class BeginRes {
    public long xid;
    public byte[] result;
}
