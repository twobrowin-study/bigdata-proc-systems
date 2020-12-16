package edu.classes.mr

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class ProductNameReducer : Reducer<Text, Text, Text, Text>() {

    private val outputValue: Text = Text()

    enum class ReduceState { MISSING_VALUE, CORRECT }

    public override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
        var name: String? = null
        var avg: Double? = null

        // Set name or avg value
        for (value in values) {
            val tmpStr = value.toString()
            avg = tmpStr.toDoubleOrNull()
            if (avg == null) {
                name = tmpStr
            }
        }

        // Increment counter for missing values
        if (name == null || avg == null) {
            // Increment counter for missing values
            context.getCounter(ReduceState.MISSING_VALUE).increment(1)
            return
        }

        // Set the value of output
        outputValue.set("$name\t$avg")

        // Emit the key-value pair
        context.write(key, outputValue)

        // Increment counter for correct output
        context.getCounter(ReduceState.CORRECT).increment(1)
    }
}