package org.dbiir.harp.utils.transcation;

import lombok.ToString;

import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.List;

public class CustomXID {

    byte[] myBqual;
    int myFormatId;
    byte[] myGtrid;
    String originStr;

    public CustomXID(byte[] gtrid, byte[] bqual, int formatId) {
        this.myGtrid = gtrid;
        this.myBqual = bqual;
        this.myFormatId = formatId;
        this.originStr = Arrays.toString(gtrid) + "," + Arrays.toString(bqual) + "," + formatId;
    }

    public CustomXID(byte[] gtrid, byte[] bqual) {
        this(gtrid, bqual, 1);
    }

    public CustomXID(String gtrid, String bqual) {
        this(gtrid.getBytes(), bqual.getBytes());
    }

    public CustomXID(String str) {
        List<String> list = List.of(str.split(","));

        if (list.size() == 1) {
            this.myGtrid = list.get(0).getBytes();
            this.myBqual = "".getBytes();
            this.myFormatId = 1;
        } else if (list.size() == 2) {
            this.myGtrid = list.get(0).getBytes();
            this.myBqual = list.get(1).getBytes();
            this.myFormatId = 1;
        } else {
            assert (list.size() <= 3);
            this.myGtrid = list.get(0).getBytes();
            this.myBqual = list.get(1).getBytes();
            if (list.get(2).startsWith("0x")) {
                this.myFormatId = Integer.parseInt(list.get(2).substring(2), 16);
            } else {
                this.myFormatId = Integer.parseInt(list.get(2));
            }
        }
        originStr = str;
    }

    @Override
    public boolean equals(Object obj) {
        boolean ret = false;
        if (obj instanceof CustomXID other) {
            ret = originStr.equals(other.originStr);
        }

        return ret;
    }

    public int hashCode() {
        return (this.originStr.hashCode() / 100) * 100 + Integer.parseInt(String.valueOf(myFormatId)) % 100;
    }

    public String toString() {
        return this.originStr;
    }
}
