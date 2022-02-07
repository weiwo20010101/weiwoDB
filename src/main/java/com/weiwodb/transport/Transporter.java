package com.weiwodb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * @Author weiwo
 * @Created on 2022/1/30
 */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private PrintWriter printWriter;
    public Transporter(Socket socket) throws IOException {
        this.socket=socket;
        reader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer=new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        printWriter = new PrintWriter(socket.getOutputStream());
    }
    public void send(byte[]data) throws IOException{
        String raw= Hex.encodeHexString(data,true)+"\n";
        writer.write(raw);
        writer.flush();
    }
    public byte[] receive() throws Exception {
        String s = reader.readLine();
        if(s==null) close();
        assert s != null;
        return  Hex.decodeHex(s);
    }
    public void close() throws Exception{
        reader.close();
        writer.close();
        socket.close();
    }

}
