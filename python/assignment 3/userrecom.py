# -*- coding: utf-8 -*-

"""
User-based collaborative filtering is used to recommend items to users.
The method predicts ratings of items based of historical ratings of items.
"""

# Author: Sergei Papulin <papulin.study@yandex.ru>, Egor Dubrovin <dubrovin.en@yandex.ru>

from pyspark import Row, RDD
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class UserBasedRecommend:
    """
    Main class that implements the user-based collaborative filtering.
    The approach includes:
    - cosine similarity calculation between users
    - weighted sum calculation for rating prediction
    Parameters
    ----------
    top_N_similarities : int, optional (default=20)
        Number of top similarities for a given user pair that will compose
        a similarity matrix. It is used in the train phase
    Attributes
    ----------
    spark : SparkSession
    top_N_similarities : int
    df_train : DataFrame
        Ratings of items in the following format: [user, item, rating]
    df_similarity : DataFrame
        Dictionary of similarities of user pairs
    """
    def __init__(self, spark, top_N_similarities=20):
        self.spark = spark
        self.top_N_similarities = int(top_N_similarities)

    def train(self, df_train, top_N=None, user_column_name="user", item_column_name="item",
              rating_column_name="rating"):
        """
        Calculate cosine similarities between user pairs

        Parameters
        ----------
        df_train : DataFrame
            Ratings of items in the following format: [user, item, rating]
        top_N : int or None
            Number of top similarities for a given user pair that will compose
            a similarity matrix. It is used in the train phase
        rating_column_name : str
        user_column_name : str
        item_column_name : str
        """
        top_N = int(top_N) if top_N else self.top_N_similarities
        user_column_name = str(user_column_name)
        item_column_name = str(item_column_name)
        rating_column_name = str(rating_column_name)

        clmn_names = [F.col(user_column_name).alias("user"),
                      F.col(item_column_name).alias("item"),
                      F.col(rating_column_name).alias("rating")]

        df_train = df_train.select(clmn_names)

        left_clmn_names = [F.col("item").alias("p"),
                   F.col("user").alias("u1"),
                   F.col("rating").alias("v1")]

        right_clmn_names = [F.col("item").alias("p"),
                    F.col("user").alias("u2"),
                    F.col("rating").alias("v2")]


        # Step 1. Create matrix
        
        df_dot = df_train.select(left_clmn_names)\
            .join(df_train.select(right_clmn_names), on="p")\
            .where(F.col("u1") != F.col("u2"))\
            .groupBy([F.col("u1"), F.col("u2")])\
            .agg(F.sum(F.col("v1") * F.col("v2")).alias("dot"))

        # Step 2. Calculate norms

        df_norm = df_train.select(left_clmn_names)\
            .groupBy(F.col("u1"))\
            .agg(F.sqrt(F.sum(F.col("v1") * F.col("v1"))).alias("norm"))
        
        similarity_clmns = [F.col("u1"), F.col("u2"), (F.col("dot")/F.col("n1")/F.col("n2")).alias("sim")]

        # Step 4. Calculate similarities

        df_similarity = df_dot.join(df_norm.select(F.col("u1"), F.col("norm").alias("n1")), on="u1")\
                    .join(df_norm.select(F.col("u1").alias("u2"), F.col("norm").alias("n2")), on="u2")\
                    .select(similarity_clmns)

        window = Window.partitionBy(F.col("u1"), F.col("u2"))

        # Step 5. Turncate similarities
        
        df_similarity_N = df_similarity.select("*", F.count("sim").over(window).alias("rank"))\
                    .filter(F.col("rank") <= top_N)

        # Step 6. Save data

        self.top_N_similarities = top_N
        self.df_similarity = df_similarity_N.select("u1", "u2", "sim").persist()
        self.df_train = df_train.persist()

        # Forse persists by calling to count
        self.df_similarity.count()
        self.df_train.count()

    def predict(self, df_test, user_column_name="user", item_column_name="item",
                predict_column_name="predict"):
        """
        Calculate predicted ratings for gived users and items

        Parameters
        ----------
        df_test : DataFrame
            List of items in the following format: [user, item]
        user_column_name : str
        item_column_name : str
        predict_column_name : str

        Returns
        -------
        DataFrame
            List of items in the following format: [user, item, predict]
        """

        user_column_name = str(user_column_name)
        item_column_name = str(item_column_name)
        predict_column_name = str(predict_column_name)

        test_clmn_names = [F.col(user_column_name).alias("u1"),
                           F.col(item_column_name).alias("p")]

        df_test_local = df_test.select(test_clmn_names)

        train_clmn_names = [F.col("user").alias("u2"),
                            F.col("item").alias("p"),
                            F.col("rating").alias("v")]

        df_train = self.df_train.select(train_clmn_names)


        # Step 1. Map Requested items on similarities and known values

        df_req = df_test_local.join(self.df_similarity, on = "u1").join(df_train, on = ["u2", "p"])

        predict_col = F.sum(F.col("sim")*F.col("v")) / F.sum(F.col("sim"))

        # Step 2. Calculating results

        df_predict = df_req.groupBy("u1", "p").agg(predict_col.alias("predicted_rating"))

        # Returning result joined with original df_test

        df_predict_to_return_clmn = [F.col("u1").alias(user_column_name),
                                     F.col("p").alias(item_column_name),
                                     F.col("predicted_rating").alias(predict_column_name)]

        df_predict_to_return = df_predict.select(df_predict_to_return_clmn)

        return df_test.join(df_predict_to_return, on = [user_column_name, item_column_name]).persist()


    def save(self):
        """Save similarities or ratings in external storage"""
        raise Exception("Not implemented.")

    def evaluate(self):
        raise Exception("Not implemented.")


def _test():

    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("itemBasedRecommendationTest") \
        .getOrCreate()

    Rating = Row("user", "item", "rating")

    data = [[1, 1, 5], [1, 2, 4], [1, 3, 2],
            [2, 1, 5], [2, 2, 5], [2, 3, 2],
            [3, 1, 2], [3, 2, 2], [3, 3, 5]]

    rdd_data = spark.sparkContext.parallelize(data, 2)
    df_rating_true = rdd_data.map(lambda x: Rating(x[0], x[1], x[2])).toDF()

    df_train, df_test = df_rating_true.randomSplit([0.8, 0.2], seed=7)
    df_train.persist(); df_test.persist()

    print("Loading data...")
    df_train.show()

    model = UserBasedRecommend(spark)

    print("Training...")
    model.train(df_train)
    print("Finished")

    print("Recommending...")
    model.predict(df_test).show()

    spark.stop()


if __name__ == "__main__":
    _test()