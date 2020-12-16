package edu.classes.mr

import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

data class NameAvgTupleWritable(var name: String = "", var avg: String = "") : Writable {

    @Throws(IOException::class)
    override fun write(p0: DataOutput) {
        p0.writeUTF(name)
        p0.writeUTF(avg)
    }

    @Throws(IOException::class)
    override fun readFields(p0: DataInput) {
        name = p0.readUTF()
        avg = p0.readUTF()
    }

    override fun toString() = "$name\t$avg"

}