package com.hadoop.visits;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装用户 visits 模型的数据
 */
public class VisitsBean implements Writable{
    private String session; // 会话
    private String remote_addr; // ip
    private String inTime; // 访问时间
    private String outTime; // 离开时间
    private String inPage; // 进入页面
    private String outPage; // 离开页面
    private String referal; // 访问路径
    private int pageVisits; // 用户访问次数

    public void set(String session, String remote_addr, String inTime, String outTime, String inPage, String outPage, String referal, int pageVisits) {
        this.session = session;
        this.remote_addr = remote_addr;
        this.inTime = inTime;
        this.outTime = outTime;
        this.inPage = inPage;
        this.outPage = outPage;
        this.referal = referal;
        this.pageVisits = pageVisits;
    }



    public void setSession(String session) {
        this.session = session;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public void setInTime(String inTime) {
        this.inTime = inTime;
    }

    public void setOutTime(String outTime) {
        this.outTime = outTime;
    }

    public void setInPage(String inPage) {
        this.inPage = inPage;
    }

    public void setOutPage(String outPage) {
        this.outPage = outPage;
    }

    public void setReferal(String referal) {
        this.referal = referal;
    }

    public void setPageVisits(int pageVisits) {
        this.pageVisits = pageVisits;
    }

    public String getSession() {
        return session;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public String getInTime() {
        return inTime;
    }

    public String getOutTime() {
        return outTime;
    }

    public String getInPage() {
        return inPage;
    }

    public String getOutPage() {
        return outPage;
    }

    public String getReferal() {
        return referal;
    }

    public int getPageVisits() {
        return pageVisits;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(session);
        out.writeUTF(remote_addr);
        out.writeUTF(inTime);
        out.writeUTF(outTime);
        out.writeUTF(inPage);
        out.writeUTF(outPage);
        out.writeUTF(referal);
        out.writeInt(pageVisits);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.session = in.readUTF();
        this.remote_addr = in.readUTF();
        this.inTime = in.readUTF();
        this.outTime = in.readUTF();
        this.inPage = in.readUTF();
        this.outPage = in.readUTF();
        this.referal = in.readUTF();
        this.pageVisits = in.readInt();
    }

    @Override
    public String toString() {
        return session + "\001" + remote_addr + "\001" + inTime + "\001" + outTime + "\001" + inPage + "\001" + outPage + "\001" +
                referal + "\001" + pageVisits;

    }
}
