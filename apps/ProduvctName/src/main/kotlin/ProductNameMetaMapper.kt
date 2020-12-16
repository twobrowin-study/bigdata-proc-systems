package edu.classes.mr

import com.google.gson.Gson
import com.google.gson.JsonParseException
import com.google.gson.annotations.SerializedName
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class ProductNameMetaMapper : Mapper<Any, Text, Text, Text>() {

    private val productId = Text()
    private val name = Text()
    private val gson = Gson()

    data class Meta(
        @SerializedName("asin") val productId: String?,
        @SerializedName("title") val name: String?
    )

    enum class MetaState { INVALID_JSON, MISSING_VALUE, CORRECT }

    public override fun map(key: Any, value: Text, context: Context) {

        // Review variable
        val meta: Meta
        try {
            // Assign a review instance to the variable
            meta = gson.fromJson(value.toString(), Meta::class.java)
        } catch (e: JsonParseException) {
            // Increment counter for bad malformed json
            context.getCounter(MetaState.INVALID_JSON).increment(1)
            return
        }

        if (meta.productId == null || meta.name == null) {
            // Increment counter for json with missing values
            context.getCounter(MetaState.MISSING_VALUE).increment(1)
            return
        }

        // Set the key of output
        productId.set(meta.productId)

        // Set the value of output
        name.set(meta.name)

        // Emit the key-value pair
        context.write(productId, name)

        // Increment counter for correct output
        context.getCounter(MetaState.CORRECT).increment(1)
    }
}