#!/usr/bin/env python3
"""
Spark ETL: Reads from Kafka, cleans all 30 columns, writes to PostgreSQL
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, lower, initcap, to_date, datediff, abs,
    round as spark_round, lit, year, month, dayofweek, from_unixtime,
    unix_timestamp, from_json, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType
)

# Configuration
KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC_NAME = "ecommerce-transactions"
POSTGRES_URL = "jdbc:postgresql://postgres_db:5432/metastore"
POSTGRES_TABLE = "ecommerce_cleaned"
POSTGRES_PROPERTIES = {
    "user": "bigdata",
    "password": "bigdata123",
    "driver": "org.postgresql.Driver"
}
CHECKPOINT_LOCATION = "/tmp/spark-checkpoint-ecommerce"

def create_spark_session():
    """Create Spark session with Kafka and PostgreSQL packages"""
    return (SparkSession.builder
        .appName("E-Commerce-ETL-Pipeline")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,"
                "org.postgresql:postgresql:42.2.18")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.cores.max", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate())

def define_schema():
    """Define schema for incoming Kafka JSON"""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_price", StringType(), True),
        StructField("discount_percent", StringType(), True),
        StructField("final_price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("payment_mode", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("delivery_date", StringType(), True),
        StructField("delivery_days", StringType(), True),
        StructField("return_flag", StringType(), True),
        StructField("return_reason", StringType(), True),
        StructField("return_date", StringType(), True),
        StructField("seller_rating", StringType(), True),
        StructField("customer_rating", StringType(), True),
        StructField("buyer_state", StringType(), True),
        StructField("seller_state", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("gst_amount", StringType(), True),
        StructField("shipping_cost", StringType(), True),
        StructField("refund_amount", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser_type", StringType(), True),
        StructField("session_duration_min", StringType(), True),
        StructField("has_reviews", StringType(), True),
        StructField("loyalty_member", StringType(), True),
    ])

def parse_kafka_stream(spark):
    """Read streaming data from Kafka and parse JSON"""
    schema = define_schema()
    
    kafka_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10000)
        .load())
    
    # Parse JSON value
    parsed_df = (kafka_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*"))
    
    return parsed_df

def clean_data(df):
    """Apply comprehensive cleaning rules for all 30 columns"""
    print("🧹 Applying data cleaning rules...")
    
    # ===== PHASE 1: HIGH PRIORITY CLEANING =====
    
    # 1. transaction_id: Remove NULLs and duplicates
    df = df.filter(col("transaction_id").isNotNull())
    df = df.filter(length(col("transaction_id")) > 0)
    df = df.dropDuplicates(["transaction_id"])
    
    # 2. user_id: Convert to integer, remove negatives and NULLs
    df = df.withColumn("user_id", col("user_id").cast(IntegerType()))
    df = df.filter(col("user_id").isNotNull())
    df = df.filter(col("user_id") > 0)
    
    # 3. product_price: Convert to float, remove outliers and negatives
    df = df.withColumn("product_price", col("product_price").cast(FloatType()))
    df = df.filter(col("product_price").isNotNull())
    df = df.filter((col("product_price") > 0) & (col("product_price") <= 500000))
    
    # 4. discount_percent: Fix negatives and >100%
    df = df.withColumn("discount_percent", col("discount_percent").cast(FloatType()))
    df = df.withColumn("discount_percent",
        when(col("discount_percent").isNull(), 0)
        .when(col("discount_percent") < 0, 0)
        .when(col("discount_percent") > 100, 100)
        .otherwise(col("discount_percent"))
    )
    
    # 5. final_price: Recalculate if inconsistent
    df = df.withColumn("final_price", col("final_price").cast(FloatType()))
    df = df.withColumn("calculated_price",
        col("product_price") * (1 - col("discount_percent") / 100.0)
    )
    df = df.withColumn("final_price",
        when(col("final_price").isNull() | (abs(col("final_price") - col("calculated_price")) > 10),
             col("calculated_price"))
        .otherwise(col("final_price"))
    )
    df = df.withColumn("final_price",
        when(col("final_price") < 0, col("calculated_price"))
        .otherwise(col("final_price"))
    ).drop("calculated_price")
    
    # 6. quantity: Ensure positive integer
    df = df.withColumn("quantity", col("quantity").cast(IntegerType()))
    df = df.filter(col("quantity").isNotNull())
    df = df.filter(col("quantity") > 0)
    df = df.withColumn("quantity",
        when(col("quantity") > 100, 100).otherwise(col("quantity"))
    )
    
    # 7. transaction_date: Parse mixed date formats
    df = df.withColumn("transaction_date",
        when(col("transaction_date").rlike("^\\d{4}-\\d{2}-\\d{2}"),
             to_date(col("transaction_date"), "yyyy-MM-dd"))
        .when(col("transaction_date").rlike("^\\d{2}/\\d{2}/\\d{4}"),
              to_date(col("transaction_date"), "dd/MM/yyyy"))
        .when(col("transaction_date").rlike("^\\d{2}-\\d{2}-\\d{4}"),
              to_date(col("transaction_date"), "dd-MM-yyyy"))
        .otherwise(None)
    )
    df = df.filter(col("transaction_date").isNotNull())
    
    # ===== PHASE 2: MEDIUM PRIORITY CLEANING =====
    
    # 8. product_category & sub_category: Standardize case
    df = df.withColumn("product_category", initcap(trim(col("product_category"))))
    df = df.withColumn("sub_category", initcap(trim(col("sub_category"))))
    
    # 9. payment_mode: Standardize
    df = df.withColumn("payment_mode", initcap(trim(col("payment_mode"))))
    df = df.withColumn("payment_mode",
        when(lower(col("payment_mode")) == "cod", "COD")
        .when(lower(col("payment_mode")) == "upi", "UPI")
        .when(lower(col("payment_mode")) == "card", "Card")
        .when(lower(col("payment_mode")) == "net banking", "Net Banking")
        .when(lower(col("payment_mode")) == "wallet", "Wallet")
        .otherwise("Unknown")
    )
    
    # 10. order_status: Standardize
    df = df.withColumn("order_status", initcap(trim(col("order_status"))))
    
    # 11. delivery_date: Parse and validate (overwrite existing column)
    df = df.withColumn("delivery_date",
        when(col("delivery_date").isNull() | (length(col("delivery_date")) == 0), None)
        .when(col("delivery_date").rlike("^\\d{4}-\\d{2}-\\d{2}"),
              to_date(col("delivery_date"), "yyyy-MM-dd"))
        .when(col("delivery_date").rlike("^\\d{2}/\\d{2}/\\d{4}"),
              to_date(col("delivery_date"), "dd/MM/yyyy"))
        .otherwise(None)
    )
    
    # 12. delivery_days: Recalculate from dates, remove negatives
    df = df.withColumn("delivery_days",
        when(col("delivery_date").isNotNull(),
             datediff(col("delivery_date"), col("transaction_date")))
        .otherwise(None)
    )
    df = df.withColumn("delivery_days",
        when(col("delivery_days") < 0, None).otherwise(col("delivery_days"))
    )
    
    # 13. return_flag: Standardize to 0/1
    df = df.withColumn("return_flag",
        when(lower(col("return_flag")).isin("1", "yes", "true"), 1)
        .otherwise(0)
    )
    
    # 14. return_reason: Fix mismatches (only for returned items)
    df = df.withColumn("return_reason",
        when(col("return_flag") == 0, None)
        .otherwise(initcap(trim(col("return_reason"))))
    )
    
    # 15. return_date: Parse and validate (overwrite existing column)
    df = df.withColumn("return_date",
        when(col("return_flag") == 0, None)
        .when(col("return_date").isNull() | (length(col("return_date")) == 0), None)
        .when(col("return_date").rlike("^\\d{4}-\\d{2}-\\d{2}"),
              to_date(col("return_date"), "yyyy-MM-dd"))
        .otherwise(None)
    )
    
    # 16. seller_rating & customer_rating: Clip to 1-5
    df = df.withColumn("seller_rating", col("seller_rating").cast(FloatType()))
    df = df.withColumn("seller_rating",
        when(col("seller_rating") < 1, 1)
        .when(col("seller_rating") > 5, 5)
        .otherwise(col("seller_rating"))
    )
    
    df = df.withColumn("customer_rating", col("customer_rating").cast(FloatType()))
    df = df.withColumn("customer_rating",
        when(col("customer_rating") < 1, 1)
        .when(col("customer_rating") > 5, 5)
        .otherwise(col("customer_rating"))
    )
    
    # 17. buyer_state & seller_state: Standardize to uppercase
    df = df.withColumn("buyer_state", upper(trim(col("buyer_state"))))
    df = df.withColumn("seller_state", upper(trim(col("seller_state"))))
    
    # 18. customer_segment: Standardize
    df = df.withColumn("customer_segment", initcap(trim(col("customer_segment"))))
    
    # 19. gst_amount: Recalculate based on category (simplified: use 18% as default)
    df = df.withColumn("gst_amount", col("gst_amount").cast(FloatType()))
    df = df.withColumn("gst_amount",
        when(col("gst_amount").isNull() | (col("gst_amount") < 0),
             col("final_price") * col("quantity") * 0.18)
        .otherwise(col("gst_amount"))
    )
    
    # 20. shipping_cost: Fix negatives and outliers
    df = df.withColumn("shipping_cost", col("shipping_cost").cast(FloatType()))
    df = df.withColumn("shipping_cost",
        when(col("shipping_cost").isNull(), 0)
        .when(col("shipping_cost") < 0, 0)
        .when(col("shipping_cost") > 500, 200)
        .otherwise(col("shipping_cost"))
    )
    
    # 21. refund_amount: Fix for non-returns
    df = df.withColumn("refund_amount", col("refund_amount").cast(FloatType()))
    df = df.withColumn("refund_amount",
        when(col("return_flag") == 0, 0)
        .when(col("refund_amount").isNull() | (col("refund_amount") < 0),
             col("final_price") * col("quantity"))
        .otherwise(col("refund_amount"))
    )
    
    # 22. session_duration_min: Remove extreme outliers
    df = df.withColumn("session_duration_min", col("session_duration_min").cast(FloatType()))
    df = df.withColumn("session_duration_min",
        when(col("session_duration_min") > 600, None)
        .when(col("session_duration_min") < 0, None)
        .otherwise(col("session_duration_min"))
    )
    
    # 23. device_type, browser_type: Standardize
    df = df.withColumn("device_type", initcap(trim(col("device_type"))))
    df = df.withColumn("browser_type", initcap(trim(col("browser_type"))))
    
    # 24. has_reviews: Standardize to 0/1
    df = df.withColumn("has_reviews",
        when(lower(col("has_reviews")).isin("1", "yes", "true"), 1)
        .otherwise(0)
    )
    
    # 25. loyalty_member: Standardize
    df = df.withColumn("loyalty_member",
        when(lower(col("loyalty_member")).isin("yes", "true", "1"), "Yes")
        .otherwise("No")
    )
    
    # ===== ADD ANALYTICS COLUMNS =====
    df = df.withColumn("transaction_year", year(col("transaction_date")))
    df = df.withColumn("transaction_month", month(col("transaction_date")))
    df = df.withColumn("day_of_week", dayofweek(col("transaction_date")))
    df = df.withColumn("total_revenue", col("final_price") * col("quantity"))
    df = df.withColumn("is_on_time_delivery",
        when(col("delivery_days") <= 7, True).otherwise(False)
    )
    
    print("✅ Data cleaning complete!")
    return df

def write_to_postgres_batch(batch_df, epoch_id):
    """Write a single batch to PostgreSQL"""
    if batch_df.count() > 0:
        print(f"📝 Writing batch {epoch_id}: {batch_df.count()} records")
        
        (batch_df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", POSTGRES_TABLE)
            .option("user", POSTGRES_PROPERTIES["user"])
            .option("password", POSTGRES_PROPERTIES["password"])
            .option("driver", POSTGRES_PROPERTIES["driver"])
            .mode("append")
            .save())
        
        print(f"✅ Batch {epoch_id} written successfully")

def main():
    print("=" * 70)
    print("🚀 Starting E-Commerce Spark ETL Pipeline")
    print("=" * 70)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read from Kafka
        print("📖 Reading streaming data from Kafka...")
        parsed_df = parse_kafka_stream(spark)
        
        # Clean data
        cleaned_df = clean_data(parsed_df)
        
        # Write to PostgreSQL using foreachBatch
        print("💾 Starting write to PostgreSQL...")
        query = (cleaned_df.writeStream
            .foreachBatch(write_to_postgres_batch)
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(processingTime="30 seconds")
            .start())
        
        print("\n⏳ ETL pipeline is running. Press Ctrl+C to stop.")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping ETL pipeline...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("✅ Spark session stopped")

if __name__ == "__main__":
    main()
