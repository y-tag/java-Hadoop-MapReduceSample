package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class KeyWritable implements Writable {
    protected String realKey;
    protected int val;
   
    public void set(int val, String key) {
        this.val = val;
        this.realKey = key;
    }

    public int getVal() {
        return val;
    }

    public String getRealKey() {
        return realKey;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, realKey);
        out.writeInt(val);
    }

    public void readFields(DataInput in) throws IOException {
        realKey = Text.readString(in);
        val = in.readInt();
    }

    public boolean equals(Object obj) {
        if (obj instanceof KeyWritable) {
            KeyWritable that = (KeyWritable) obj;
            if (this.realKey.equals(that.realKey) && this.val == that.val) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return realKey.hashCode();
    }
    
    public String toString() {
        return this.realKey + "\t" + this.val;
    }
}