package edu.classes.mr

import com.google.gson.Gson
import com.google.gson.JsonParseException
import com.google.gson.annotations.SerializedName
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class AvgRatingMapper : Mapper<Any, Text, Text, StatsTupleWritable>() {

    private val productId = Text()
    private val ratingTuple = StatsTupleWritable()
    private val gson = Gson()

    data class Review(
        @SerializedName("asin") val productId: String?,
        @SerializedName("overall") val rating: Double?
    )

    enum class ReviewState { INVALID_JSON, MISSING_VALUE, CORRECT }

    public override fun map(key: Any, value: Text, context: Context) {

        // Review variable
        val review: Review
        try {
            // Assign a review instance to the variable
            review = gson.fromJson(value.toString(), Review::class.java)
        } catch (e: JsonParseException) {
            // Increment counter for bad malformed json
            context.getCounter(ReviewState.INVALID_JSON).increment(1)
            return
        }

        if (review.productId == null || review.rating == null) {
            // Increment counter for review json with missing values
            context.getCounter(ReviewState.MISSING_VALUE).increment(1)
            return
        }

        // Set the key of output
        productId.set(review.productId)

        // Set the value of output
        ratingTuple.sum = review.rating
        ratingTuple.count = 1

        // Emit the key-value pair
        context.write(productId, ratingTuple)

        // Increment counter for correct review json
        context.getCounter(ReviewState.CORRECT).increment(1)

        // Rating interval counter
        if (review.rating > 4.0) {
            context.getCounter(RATING_INTERVAL_COUNTER_GROUP, "5").increment(1)
        } else if (review.rating > 3.0 && review.rating <= 4.0) {
            context.getCounter(RATING_INTERVAL_COUNTER_GROUP, "3").increment(1)
        } else if (review.rating > 2.0 && review.rating <= 3.0) {
            context.getCounter(RATING_INTERVAL_COUNTER_GROUP, "3").increment(1)
        } else if (review.rating > 1.0 && review.rating <= 2.0) {
            context.getCounter(RATING_INTERVAL_COUNTER_GROUP, "2").increment(1)
        } else {
            context.getCounter(RATING_INTERVAL_COUNTER_GROUP, "1").increment(1)
        }
    }

    companion object {
        private const val RATING_INTERVAL_COUNTER_GROUP = "RATING INTERVALS"
    }
}