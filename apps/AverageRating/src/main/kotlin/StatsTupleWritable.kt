package edu.classes.mr

import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

data class StatsTupleWritable (var sum: Double = 0.0, var count: Int = 0) : Writable {

    @Throws(IOException::class)
    override fun write(out: DataOutput) {
        out.writeDouble(sum)
        out.writeInt(count)
    }

    @Throws(IOException::class)
    override fun readFields(`in`: DataInput) {
        sum = `in`.readDouble()
        count = `in`.readInt()
    }

    override fun toString() = "${sum/count}"
}