package edu.classes.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val conf = Configuration()
    exitProcess(ToolRunner.run(conf, AvgRatingDriver(), args))
}

class AvgRatingDriver : Configured(), Tool {
    override fun run(args: Array<String>): Int {
        val job = Job.getInstance(conf, "AverageRatingApp")
        return with(job) {
            setJarByClass(AvgRatingDriver::class.java)

            // Classes
            mapperClass = AvgRatingMapper::class.java
            combinerClass = AvgRatingReducer::class.java
            reducerClass = AvgRatingReducer::class.java

            // Input
            FileInputFormat.addInputPath(job, Path(args[0]))
            inputFormatClass = TextInputFormat::class.java

            // Output
            FileOutputFormat.setOutputPath(job, Path(args[1]))
            outputFormatClass = TextOutputFormat::class.java
            outputKeyClass = Text::class.java
            outputValueClass = StatsTupleWritable::class.java
            if (waitForCompletion(true)) 0 else 1
        }
    }
}