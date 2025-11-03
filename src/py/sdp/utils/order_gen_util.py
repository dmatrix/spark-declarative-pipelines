from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import random
from datetime import datetime, timedelta
import uuid
from typing import Optional

def create_random_order_items(num_items: int = 100, random_state: Optional[int] = None) -> DataFrame:
    """
    Generates a DataFrame with random order items.

    Args:
        num_items (int): Number of random order items to generate. Defaults to 100.
        random_state (Optional[int]): Random seed for reproducibility. If None,
                                      results will be different each time. Defaults to None.

    Returns:
        pyspark.sql.DataFrame: DataFrame containing random order items.

    Examples:
        # Generate 100 random orders (non-reproducible)
        df = create_random_order_items(100)

        # Generate 100 random orders with reproducible results
        df = create_random_order_items(100, random_state=42)
    """
    # Set random seed for reproducibility if provided
    if random_state is not None:
        random.seed(random_state)

    # Initialize Spark session
    spark = SparkSession.active()

    # Define schema with US state
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("items_ordered", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("state", StringType(), False),
        StructField("date_ordered", DateType(), False)
    ])

    # Possible order items (toys, sports, electronics, etc.)
    items = [
        "Toy Car", "Basketball", "Laptop", "Action Figure", "Tennis Racket",
        "Smartphone", "Board Game", "Football", "Headphones", "Drone",
        "Puzzle", "Tablet", "Skateboard", "Camera", "Video Game",
        "Scooter", "Smartwatch", "Baseball Bat", "VR Headset", "Electric Guitar"
    ]

    # Possible statuses
    statuses = ["approved", "fulfilled", "pending", "cancelled"]

    # US states (all 50 states)
    us_states = [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
        "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
        "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
        "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
        "New Hampshire", "New Jersey", "New Mexico", "New York",
        "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
        "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
        "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
        "West Virginia", "Wisconsin", "Wyoming"
    ]

    # Generate random rows based on num_items parameter
    data = []
    for i in range(num_items):
        # Use UUID for order_id (note: UUID generation is not affected by random.seed)
        # For full reproducibility with random_state, we could use a deterministic approach
        if random_state is not None:
            # Generate deterministic UUID based on random state and index
            order_id = str(uuid.UUID(int=random.getrandbits(128), version=4))
        else:
            order_id = str(uuid.uuid4())

        order_item = random.choice(items)
        price = round(random.uniform(10.0, 1000.0), 2)  # price between $10 and $1000
        items_ordered = random.randint(1, 10)
        status = random.choice(statuses)
        state = random.choice(us_states)
        date_ordered = (datetime.now() - timedelta(days=random.randint(0, 30))).date()
        data.append((order_id, order_item, price, items_ordered, status, state, date_ordered))

    # Create DataFrame
    orders_df = spark.createDataFrame(data, schema)
    return orders_df

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("OrderGenUtilTest") \
        .master("local[*]") \
        .getOrCreate()

    # Test creating 10 random order items
    print("Testing with 10 items (no random state):")
    orders_df_10 = create_random_order_items(num_items=10)
    orders_df_10.show(truncate=False)

    print(f"Number of rows generated: {orders_df_10.count()}")

    # Test with random state for reproducibility
    print("\n" + "="*80)
    print("Testing reproducibility with random_state=42")
    print("="*80)
    print("\nRun 1:")
    orders_df_seed1 = create_random_order_items(num_items=5, random_state=42)
    orders_df_seed1.show(truncate=False)

    print("\nRun 2 (should be identical to Run 1):")
    orders_df_seed2 = create_random_order_items(num_items=5, random_state=42)
    orders_df_seed2.show(truncate=False)

    # Also test with default (100 items) - just show count
    print("\nTesting with default 100 items:")
    orders_df_default = create_random_order_items()
    print(f"Number of rows with default: {orders_df_default.count()}")
    print("\nSample of 5 rows:")
    orders_df_default.show(5, truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
