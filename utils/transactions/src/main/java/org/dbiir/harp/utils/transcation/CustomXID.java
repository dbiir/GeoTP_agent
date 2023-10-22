package org.dbiir.harp.utils.transcation;

import com.mysql.jdbc.jdbc2.optional.MysqlXid;
import lombok.ToString;

import javax.transaction.xa.Xid;
import java.util.List;

@ToString
public class CustomXID {

    byte[] myBqual;
    int myFormatId;
    byte[] myGtrid;

    public CustomXID(byte[] gtrid, byte[] bqual, int formatId) {
        this.myGtrid = gtrid;
        this.myBqual = bqual;
        this.myFormatId = formatId;
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
            this.myFormatId = Integer.parseInt(list.get(2));
        }
    }
}
