package edu.classes.mr

import com.google.gson.Gson
import com.google.gson.JsonParseException
import com.google.gson.annotations.SerializedName
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class AvgByNameMapper : Mapper<Any, Text, Text, NameAvgTupleWritable>() {

    private val productId = Text()
    private val nameAvgTuple = NameAvgTupleWritable()

    enum class NameAvgState { MISSING_VALUE, CORRECT }

    public override fun map(key: Any, value: Text, context: Context) {

        val valuesString = value.toString().split("\t")

        if (valuesString.size != 3) {
            // Increment counter for review json with missing values
            context.getCounter(NameAvgState.MISSING_VALUE).increment(1)
            return
        }

        // Set the key of output
        productId.set(valuesString[0])

        // Set the value of output
        nameAvgTuple.name = valuesString[1]
        nameAvgTuple.avg = valuesString[2]

        // Emit the key-value pair
        context.write(productId, nameAvgTuple)

        // Increment counter for correct review json
        context.getCounter(NameAvgState.CORRECT).increment(1)
    }

}