package edu.classes.mr

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class AvgRatingReducer : Reducer<Text, StatsTupleWritable, Text, StatsTupleWritable>() {

    private val result = StatsTupleWritable()

    public override fun reduce(key: Text, values: Iterable<StatsTupleWritable>, context: Context) {

        // Accumulative sum  of ratings of the given product
        var sum = 0.0

        // Number of ratings
        var count = 0

        // Iterate over all tuples from all maps
        for (value in values) {
            sum += value.sum
            count += value.count
        }

        // Set result values
        result.sum = sum
        result.count = count

        /*
           Emit final values
           For TextOutputFormat result.toString() will be used to write to a filesystem
         */
        context.write(key, result)
    }
}