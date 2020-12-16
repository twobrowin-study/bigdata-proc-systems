package edu.classes.mr

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class ProductNameAvgMapper : Mapper<Any, Text, Text, Text>() {

    private val productId = Text()
    private val avg = Text()

    enum class AvgState { MISSING_VALUE, CORRECT }

    public override fun map(key: Any?, value: Text, context: Context) {
        val (productId, avg) =
            with(value.toString().split("\t")) {
            if (size != 2) {
                // Increment counter for missing values
                context.getCounter(AvgState.MISSING_VALUE).increment(1)
                return
            }
            get(0) to get(1)
        }

        // Set the key of output
        this.productId.set(productId)

        // Set the value of output
        this.avg.set(avg)

        // Emit the key-value pair
        context.write(this.productId, this.avg)

        // Increment counter for correct output
        context.getCounter(AvgState.CORRECT).increment(1)
    }
}