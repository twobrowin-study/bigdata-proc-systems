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
    exitProcess(ToolRunner.run(conf, AvgByNameDriver(), args))
}

class AvgByNameDriver : Configured(), Tool {
    override fun run(args: Array<String>): Int {
        conf.set("contained", args[2])
        val job = Job.getInstance(conf, "AvgByNameApp")
        return with(job) {
            setJarByClass(AvgByNameDriver::class.java)

            // Classes
            mapperClass = AvgByNameMapper::class.java
            combinerClass = AvgByNameReducer::class.java
            reducerClass = AvgByNameReducer::class.java

            // Input
            FileInputFormat.addInputPath(job, Path(args[0]))
            inputFormatClass = TextInputFormat::class.java

            // Output
            FileOutputFormat.setOutputPath(job, Path(args[1]))
            outputFormatClass = TextOutputFormat::class.java
            outputKeyClass = Text::class.java
            outputValueClass = NameAvgTupleWritable::class.java
            if (waitForCompletion(true)) 0 else 1
        }
    }
}