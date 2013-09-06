package test.hone.kmeans;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.hadoop.io.DynamicDirectByteBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ByteUtil;

public class DoublePoint implements WritableComparable {

    private double x;
    private double y;

    public DoublePoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public DoublePoint() {
    }

    public double getX() {
        return this.x;
    }

    public double getY() {
        return this.y;
    }

    public static DoublePoint create(DataInputStream in) throws IOException {
        DoublePoint m = new DoublePoint();
        m.readFields(in);

        return m;
    }

    public DoublePoint create(byte[] a, int offset) throws IOException {
        DoublePoint m = new DoublePoint();
        m.readFields(a, offset);

        return m;
    }

    public void readFields(DataInputStream in) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
        //leftElement = readInt(a, offset);
        //offset += 4;
        //rightElement = readInt(a, offset);
    }

    public void readFields(byte[] a, int offset) throws IOException {
        x = ByteUtil.toDouble(a, offset);
        y = ByteUtil.toDouble(a, offset + 8);
        //leftElement = readInt(a, offset);
        //offset += 4;
        //rightElement = readInt(a, offset);
    }

    public int write(DataOutputStream out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
        return 16;
    }

    @Override
    public int write(ByteBuffer buf) {
        buf.putDouble(x);
        buf.putDouble(y);
        return 16;
    }
    
    @Override
    public int write(DynamicDirectByteBuffer buf) {
        buf.putDouble(x);
        buf.putDouble(y);
        return 16;
    }

    public void setXY(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double distance(DoublePoint point) {
        double dx = this.getX() - point.getX();
        dx *= dx;

        double dy = this.getY() - point.getY();
        dy *= dy;

        return Math.sqrt(dx + dy);
    }

    public static DoublePoint average(Collection<DoublePoint> points) {
        double x = 0;
        double y = 0;
        for (DoublePoint point : points) {
            x += point.getX();
            y += point.getY();
        }

        return new DoublePoint(x / points.size(), y / points.size());
    }

    public String toString() {
        return "(" + this.x + "," + this.y + ")";
    }

    @Override
    public int compare(byte[] a, int i1, int j1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getOffset() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void set(WritableComparable obj) {
        throw new UnsupportedOperationException("Not supported yet.");
    }            
}
