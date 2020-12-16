package edu.classes.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val conf = Configuration()
    exitProcess(ToolRunner.run(conf, ProductNameDriver(), args))
}

class ProductNameDriver : Configured(), Tool {
    override fun run(args: Array<String>): Int {
        val job = Job.getInstance(conf, "ProductNameApp")
        return with(job) {
            setJarByClass(ProductNameDriver::class.java)

            // Meta input
            MultipleInputs.addInputPath(
                this,
                Path(args[0]),
                TextInputFormat::class.java,
                ProductNameMetaMapper::class.java
            )

            // Avg input
            MultipleInputs.addInputPath(
                this,
                Path(args[1]),
                TextInputFormat::class.java,
                ProductNameAvgMapper::class.java
            )

            // Reducer
            reducerClass = ProductNameReducer::class.java

            // Output
            FileOutputFormat.setOutputPath(job, Path(args[2]))
            outputFormatClass = TextOutputFormat::class.java
            outputKeyClass = Text::class.java
            outputValueClass = Text::class.java
            if (waitForCompletion(true)) 0 else 1
        }
    }
}