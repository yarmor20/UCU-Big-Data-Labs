from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w
from datetime import timedelta, datetime


spark = SparkSession.builder.master("local[*]").appName("lab9").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# ======================== TASK 2 ========================

def top_trending_videos(videos_df, limit=0):
    """
    Find Top 10 videos that were amongst the trending videos for the highest number of days 
    (it doesn't need to be a consecutive period of time). You should also include information 
    about different metrics for each day the video was trending.
    """
    # Cast and transform input data.
    df = videos_df.select(
        f.col("video_id").alias("id"),
        f.col("title"),
        f.col("description"),
        f.struct(
            f.to_date(f.from_unixtime(f.unix_timestamp(f.col("trending_date"), "yy.dd.MM"))).alias("date"), 
            f.col("views").cast(t.LongType()).alias("views"), 
            f.col("likes").cast(t.LongType()).alias("likes"), 
            f.col("dislikes").cast(t.LongType()).alias("dislikes")
        ).alias("trendingDay")
    )

    # Get a total number of days in trending for each video.
    total_trending_df = df \
        .groupBy(f.col("id")).agg(f.count(f.col("trendingDay.date")).alias("totalTrendingDays")) \
        .select(f.col("id"), f.col("totalTrendingDays"))

    # Rank the latest day of video being in trends as 1.
    # Get the latest views, likes and dislikes. 
    window = w.partitionBy("id").orderBy(f.col("trendingDay.date").desc())
    latest_stats_df = df \
        .withColumn("rank", f.row_number().over(window)) \
        .where(f.col("rank") == 1) \
        .select(
            f.col("id"), 
            f.col("trendingDay.views").alias("latestViews"), 
            f.col("trendingDay.likes").alias("latestLikes"), 
            f.col("trendingDay.dislikes").alias("latestDislikes")
        )

    # Collect all trendingDay statistics into a single array for each video.
    premerge_df = df \
        .groupBy("id", "title", "description").agg(f.collect_list(f.col("trendingDay")).alias("trendingDays")) \

    # Get top 10 videos by number of days in trending.
    merge_df = (
        premerge_df
            .join(total_trending_df, ["id"], how="left")
            .join(latest_stats_df, ["id"], how="left")
            .select(
                f.col("id"),
                f.col("title"),
                f.col("description"),
                f.col("latestViews"),
                f.col("latestLikes"),
                f.col("latestDislikes"),
                f.col("trendingDays"),
                f.col("totalTrendingDays")
            )
    )
    merge_df.count()

    # Collect all videos in an array.
    collected_df = merge_df.select(
        f.struct(
            f.col("id"),
            f.col("title"),
            f.col("description"),
            f.col("latestViews"),
            f.col("latestLikes"),
            f.col("latestDislikes"),
            f.col("trendingDays")
        ).alias("video")
    )
    
    if limit > 0:
        return collected_df.orderBy(f.col("totalTrendingDays").desc()).limit(limit)
    return collected_df  


# ======================== TASK 2 ========================

week_boundaries_schema = t.StructType([
    t.StructField("start_date", t.DateType()),
    t.StructField("end_date", t.DateType())
])


@f.udf(week_boundaries_schema)
def get_week_boundaries(date):
    date_dt = datetime.strptime(str(date), "%Y-%m-%d")
    start = date_dt - timedelta(days=date_dt.weekday())
    end = start + timedelta(days=6)
    return start, end  


def top_week_categories(videos_df, categories_df):
    """
    Find what was the most popular category for each week (7 days slices). 
    Popularity is decided based on the total number of views for videos of
    this category. Note, to calculate it you can’t just sum up the number of views. 
    If a particular video appeared only once during the given period, it shouldn’t 
    be counted. Only if it appeared more than once you should count the number of new 
    views. For example, if video A appeared on day 1 with 100 views, then on day 4 with 
    250 views and again on day 6 with 400 views, you should count it as 400.
    For our purpose, it will mean that this particular video was watched 300 times in 
    the given time period.
    """
    # Cast and transform input data.
    weeks_df = videos_df \
    .select(
        f.col("video_id").alias("id"),
        f.to_date(f.from_unixtime(f.unix_timestamp(f.col("trending_date"), "yy.dd.MM"))).alias("date"), 
        f.col("views").cast(t.LongType()).alias("views"), 
        f.col("category_id"),
    )
    
    # Get week start and end dates.
    weeks_df = weeks_df \
        .withColumn("week_boundaries", get_week_boundaries(f.col("date"))) \
        .select(
            f.col("id"),
            f.col("views"),
            f.col("date"),
            f.col("category_id"),
            f.col("week_boundaries.start_date").alias("start_date"),
            f.col("week_boundaries.end_date").alias("end_date")
    )
    
    # Get all videos that occur more than once a week. 
    # Calculate the amount of new views for each video every week.
    video_views_weekly_df = (
        weeks_df
            .groupBy("start_date", "end_date", "id", "category_id")
            .agg(
                f.count("id").alias("count"), 
                f.collect_list(f.col("views")).alias("views_list")
            )
            .where(f.col("count") >= 2).
            select(
                f.col("id"),
                f.col("start_date"),
                f.col("end_date"),
                f.col("category_id"),
                (f.array_max(f.col("views_list")) - f.array_min(f.col("views_list"))).alias("views_delta")
            )
    )
    
    # Get a single top category and its statistics for each week.
    window = w.partitionBy("start_date", "end_date").orderBy(f.col("total_views").desc())
    week_top_categories_df = (
        video_views_weekly_df
            .groupBy("start_date", "end_date", "category_id")
            .agg(
                f.count("id").alias("number_of_videos"), 
                f.sum(f.col("views_delta")).alias("total_views"),
                f.collect_list(f.col("id")).alias("video_ids")
            )
            .withColumn("rank", f.row_number().over(window))
            .where(f.col("rank") == 1)
    )
    
    # Join with categories DF to get category title.
    # Collect all weeks in an array.
    week_top_categories_df = (
    week_top_categories_df.alias("wtc")
        .join(categories_df.alias("ce"), f.col("wtc.category_id") == f.col("ce.id"), how="left")
        .select(
            f.struct(
                f.col("wtc.start_date"),
                f.col("wtc.end_date"),
                f.col("wtc.category_id"),
                f.col("ce.title").alias("category_name"),
                f.col("wtc.number_of_videos"),
                f.col("wtc.total_views"),
                f.col("wtc.video_ids")
            ).alias("weeks")
        )
    )
    return week_top_categories_df


# ======================== TASK 3 ========================

date_ranges_schema = t.ArrayType(
    t.StructType([
        t.StructField("start_date", t.DateType()),
        t.StructField("end_date", t.DateType())
    ])
)


@f.udf(date_ranges_schema)
def date_ranges_udf(date):
    delta = timedelta(days=30)
    
    dates = []
    
    min_start_date = date - delta
    max_end_date = date + delta
    
    start_date = min_start_date
    end_date = date
    while end_date != max_end_date:
        dates.append([start_date, end_date])
        
        start_date += timedelta(days=1)
        end_date += timedelta(days=1)
    
    return dates


def top_tags_monthly(videos_df):
    """
    What were the 10 most used tags amongst trending videos for each 30days time period? 
    Note, if during the specified period the same video appears multiple times,
    you should count tags related to that video only once.
    """
    # Split tags by | and explode them into separate rows.
    # Get rid from tags that appear twice.
    tags_exploaded_df = (
        videos_df
            .select(
                f.col("video_id"),
                f.split(f.regexp_replace(f.col("tags"), '\"', ''), "\|").alias("tags_array"),
                f.to_date(f.from_unixtime(f.unix_timestamp(f.col("trending_date"), "yy.dd.MM"))).alias("date")
            )
            .withColumn("tag", f.explode(f.col("tags_array")))
            .select(
                f.col("video_id"),
                f.col("tag"),
                f.col("date")
            ).distinct()
    )
    
    # Calculate the date ranges for each record. (date - 30 days; date + 30 days)
    # Explode those ranges.
    tags_time_periods_df = (
        tags_exploaded_df
            .withColumn("start_end_dates", date_ranges_udf(f.col("date")))
            .withColumn("ranges", f.explode(f.col("start_end_dates")))
            .select(
                f.col("video_id"),
                f.col("tag"),
                f.col("date"),
                f.col("ranges.start_date").alias("start_date"),
                f.col("ranges.end_date").alias("end_date")
            )      
    )
    
    # Get top 10 tags for each date range.
    window =  w.partitionBy("start_date", "end_date").orderBy(f.col("number_of_videos").desc())
    top_tags_ranged_df = (
        tags_time_periods_df
            .groupBy("tag", "start_date", "end_date")
            .agg(
                f.count(f.col("video_id")).alias("number_of_videos"), 
                f.collect_list(f.col("video_id")).alias("video_ids")
            )
            .withColumn("rank", f.row_number().over(window))
            .where(f.col("rank") <= 10)
            .select(
                f.col("start_date"),
                f.col("end_date"),
                f.struct(
                    f.col("tag"),
                    f.col("number_of_videos"),
                    f.col("video_ids")
                ).alias("tag_stat")
            )
    )
    
    # Transform data to the final form.
    top_used_tags_df = (
        top_tags_ranged_df.
            groupBy("start_date", "end_date")
            .agg(f.collect_list(f.col("tag_stat")).alias("tags"))
            .select(f.struct(
                f.col("start_date"),
                f.col("end_date"),
                f.col("tags")
            ).alias("month"))
    )
    return top_used_tags_df


# ======================== TASK 4 ========================

def top_channels(videos_df):
    """
    Show the top 20 channels by the number of views for the whole period. Note, 
    if there are multiple appearances of the same video for some channel, you should 
    take into account only the last appearance (with the highest number of views).
    """
    # Cast and transform input data.
    channel_videos_df = videos_df.select(
        f.col("channel_title"),
        f.col("video_id"),
        f.to_date(f.from_unixtime(f.unix_timestamp(f.col("trending_date"), "yy.dd.MM"))).alias("date"), 
        f.col("views").cast(t.LongType()).alias("views"), 
    )
    
    # Take only last appearances for each video of a channel.
    window = w.partitionBy("channel_title", "video_id").orderBy(f.col("views").desc())
    channel_videos_once_df = (
        channel_videos_df
            .withColumn("rank", f.row_number().over(window))
            .where(f.col("rank") == 1)
    )
    
    # Get top 20 channels by total number of views for the whole period.
    channel_stats_df = (
        channel_videos_once_df
            .withColumn("video_stats", f.struct(f.col("video_id"), f.col("views")))
            .groupBy("channel_title")
            .agg(
                f.min("date").alias("start_date"),
                f.max("date").alias("end_date"),
                f.sum("views").alias("total_views"),
                f.collect_list("video_stats").alias("video_views")
            )
            .select(
                f.struct(
                    f.col("channel_title"),
                    f.col("start_date"),
                    f.col("end_date"),
                    f.col("total_views"),
                    f.col("video_views")
                ).alias("channel")
            )
            .orderBy(f.col("channel.total_views").desc())
            .limit(20)
    )
    return channel_stats_df


# ======================== TASK 5 ========================

def top_trending_channels(videos_df):
    """
    Show the top 10 channels with videos trending for the highest number of days 
    (it doesn't need to be a consecutive period of time) for the whole period.
    In order to calculate it, you may use the results from the question No1.
    The total_trending_days count will be a sum of the numbers of trending days 
    for videos from this channel.
    """
    # Get all trending periods for each video.
    trending_videos_df = top_trending_videos(videos_df)
    
    # Get channel name - video id mapping.
    channel_video_df = videos_df.select("video_id", "channel_title").distinct()
    
    # Add channel name column to trending videos.
    videos_trending_days_df = (
        trending_videos_df.alias("tv")
            .join(channel_video_df.alias("cv"), f.col("tv.video.id") == f.col("cv.video_id"), how="left")
            .select(
                f.col("cv.channel_title").alias("channel_name"),
                f.struct(
                    f.col("tv.video.id").alias("video_id"),
                    f.col("tv.video.title").alias("video_title"),
                    f.size(f.col("tv.video.trendingDays")).alias("trending_days")
                ).alias("video_day")
            )
    )
    
    # Get top 10 channels by the ammount of trending videos.
    top_channels_df = (
        videos_trending_days_df
            .groupBy("channel_name")
            .agg(
                f.sum(f.col("video_day.trending_days")).alias("total_trending_days"),
                f.collect_list(f.col("video_day")).alias("videos_days")
            )
            .select(f.struct(
                f.col("channel_name"),
                f.col("total_trending_days"),
                f.col("videos_days")
            ).alias("channel"))
            .orderBy(f.col("channel.total_trending_days").desc())
            .limit(10)
    )
    return top_channels_df


# ======================== TASK 6 ========================

def top_category_videos(videos_df, categories_df):
    """
    Show the top 10 videos by the ratio of likes/dislikes for each category
    for the whole period. You should consider only videos with more than 100K views. 
    If the same video occurs multiple times you should take the record when
    the ratio was the highest.
    """
    # Cast and transform input data.
    # Filter out video with less than 100K views.
    # Calculate like/dislike ratio for each video.
    videos_ratio_df = (
        videos_df
            .select(
                f.col("video_id"),
                f.col("title").alias("video_title"),
                f.col("views").cast(t.LongType()).alias("views"),
                f.col("likes").cast(t.LongType()).alias("likes"), 
                f.col("dislikes").cast(t.LongType()).alias("dislikes"), 
                f.col("category_id")
            )
            .where(f.col("views") > 100000)
            .withColumn("ratio_likes_dislikes", f.col("likes") / f.col("dislikes"))
            .select(
                f.col("category_id"),
                f.col("video_id"),
                f.col("video_title"),
                f.col("views"),
                f.col("ratio_likes_dislikes")
            )
    )
    
    # Get the highest liked/disliked ratio for videos that appear more than once.
    video_window = w.partitionBy("video_id").orderBy(f.col("ratio_likes_dislikes").desc())
    ratio_videos_df = (
        videos_ratio_df
            .withColumn("video_rank", f.row_number().over(video_window))
            .where(f.col("video_rank") == 1)
    )

    # Get top 10 videos by ratio for each category.
    category_window =  w.partitionBy("category_id").orderBy(f.col("ratio_likes_dislikes").desc())
    category_ratio_df = (
        ratio_videos_df
            .withColumn("category_rank", f.row_number().over(category_window))
            .where(f.col("category_rank") <= 10)
            .select(
                f.col("category_id"),
                f.struct(
                    f.col("video_id"),
                    f.col("video_title"),
                    f.col("ratio_likes_dislikes"),
                    f.col("views"),
                ).alias("video")
            )
    )

    # Transform data to its final shape.
    # Join with categories DF to get category name.
    top_category_videos_df = (
        category_ratio_df.alias("cr")
            .groupBy("category_id")
            .agg(f.collect_list(f.col("video")).alias("videos"))
            .join(categories_df.alias("c"), f.col("cr.category_id") == f.col("c.id"), how="left")
            .select(f.struct(
                f.col("cr.category_id").alias("category_id"),
                f.col("c.title").alias("category_name"),
                f.col("videos")
            ).alias("category")) 
    )
    return top_category_videos_df


if __name__ == "__main__":
    # Read categories DF.
    categories_df = spark.read.format("json") \
        .option("multiline","true") \
        .load("./data/GB_category_id.json")

    categories_df = (
        categories_df
            .withColumn("categories_exploaded", f.explode(f.arrays_zip("items.id", "items.snippet.title")))
            .select(
                f.col("categories_exploaded.id").alias("id"),
                f.col("categories_exploaded.title").alias("title"),
            )
    )

    # Read videos DF.
    videos_df = spark.read.format("csv") \
        .option("header", True) \
        .option("sep", ",") \
        .option("multiline", True) \
        .load("./data/GBvideos.csv")

    # =================== TASK 1 ===================    
    top_trending_videos_df = top_trending_videos(videos_df, limit=10)
    print("=" * 12, "TASK 1", "=" * 12)
    top_trending_videos_df.printSchema()
    top_trending_videos_df.show()

    # =================== TASK 2 ===================    
    week_top_categories_df = top_week_categories(videos_df, categories_df)
    print("=" * 12, "TASK 2", "=" * 12)
    week_top_categories_df.printSchema()
    week_top_categories_df.show()

    # =================== TASK 3 ===================    
    top_tags_monthly_df = top_tags_monthly(videos_df)
    print("=" * 12, "TASK 3", "=" * 12)
    top_tags_monthly_df.printSchema()
    top_tags_monthly_df.show()

    # =================== TASK 4 ===================    
    top_channels_df = top_channels(videos_df)
    print("=" * 12, "TASK 4", "=" * 12)
    top_channels_df.printSchema()
    top_channels_df.show()

    # =================== TASK 5 ===================    
    top_trending_channels_df = top_trending_channels(videos_df)
    print("=" * 12, "TASK 5", "=" * 12)
    top_trending_channels_df.printSchema()
    top_trending_channels_df.show()

    # =================== TASK 6 ===================    
    top_category_videos_df = top_category_videos(videos_df, categories_df)
    print("=" * 12, "TASK 6", "=" * 12)
    top_category_videos_df.printSchema()
    top_category_videos_df.show()
