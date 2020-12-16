package edu.classes.mr

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class AvgByNameReducer : Reducer<Text, NameAvgTupleWritable, Text, NameAvgTupleWritable>() {

    enum class FindState { NOT_FOUND, FOUND }

    public override fun reduce(key: Text, values: Iterable<NameAvgTupleWritable>, context: Context) {
        val contained = context.configuration.get("contained")

        for (value in values) {
            if (value.toString().contains(contained)) {
                // Write founded value
                context.write(key, value)

                // Increment counter for founded value
                context.getCounter(FindState.FOUND).increment(1)
            } else {
                // Increment counter for not founded value
                context.getCounter(FindState.NOT_FOUND).increment(1)
            }
        }
    }
}