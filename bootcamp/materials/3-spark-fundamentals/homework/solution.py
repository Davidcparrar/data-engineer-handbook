from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    desc,
)
from pyspark.sql.functions import (
    sum as sparksum,
)

ROOT = "/home/iceberg/data"


def drop_tables(spark):
    """DROP Queries."""
    drop_match_details_bucketed = "DROP TABLE IF EXISTS bootcamp.match_details_bucketed"
    drop_matches_bucketed = "DROP TABLE IF EXISTS bootcamp.matches_bucketed"
    drop_medal_matches_players_bucketed = (
        "DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed"
    )

    spark.sql(drop_match_details_bucketed)
    spark.sql(drop_matches_bucketed)
    spark.sql(drop_medal_matches_players_bucketed)


def create_tables(spark):
    # Create DDL Statements
    match_details_bucketed_ddl = """
    CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
         match_id STRING,
         player_gamertag STRING,
         player_total_kills INTEGER
     )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """

    matches_bucketed_ddl = """
    CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
         match_id STRING,
         map_id STRING,
         playlist_id STRING
     )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """

    medal_matches_player_bucketed_ddl = """
    CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
         match_id STRING,
         player_game_tag STRING,
         medal_id STRING,
         count INTEGER
     )
     USING iceberg
     PARTITIONED BY (bucket(16, match_id));
     """

    spark.sql(match_details_bucketed_ddl)
    spark.sql(matches_bucketed_ddl)
    spark.sql(medal_matches_player_bucketed_ddl)


def insert_data(spark, match_details, matches, medals_matches_players):
    """Save data to iceberg tables."""
    match_details.select(
        col("match_id"),
        col("player_gamertag"),
        col("player_total_kills").cast("int").alias("player_total_kills"),
    ).sortWithinPartitions(col("match_id"), col("player_gamertag")).write.mode(
        "overwrite"
    ).bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")

    matches.select(
        col("match_id"),
        col("mapid").alias("map_id"),
        col("playlist_id"),
    ).sortWithinPartitions(col("match_id"), col("map_id")).write.mode(
        "overwrite"
    ).bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed")

    medals_matches_players.select(
        col("match_id"),
        col("player_gamertag"),
        col("medal_id"),
        col("count"),
    ).sortWithinPartitions(
        col("match_id"), col("player_gamertag"), col("medal_id")
    ).write.mode("overwrite").bucketBy(16, "match_id").saveAsTable(
        "bootcamp.medal_matches_players_bucketed"
    )


def bucketed_join(spark, medals, maps):
    """Bucketed Join.

    This function performs a bucketed join on the five tables and returns the
    result dataframe.
    """
    match_details_bucketed = spark.table("bootcamp.match_details_bucketed")
    matches_bucketed = spark.table("bootcamp.matches_bucketed")
    medal_matches_players_bucketed = spark.table(
        "bootcamp.medal_matches_players_bucketed"
    )

    result_df = (
        match_details_bucketed.join(
            matches_bucketed,
            match_details_bucketed.match_id == matches_bucketed.match_id,
        )
        .join(
            medal_matches_players_bucketed,
            medal_matches_players_bucketed.match_id == matches_bucketed.match_id,
        )
        .join(medals, medal_matches_players_bucketed.medal_id == medals.medal_id)
        .join(maps, maps.mapid == matches_bucketed.map_id)
        .select(
            matches_bucketed.match_id,
            matches_bucketed.map_id,
            matches_bucketed.playlist_id,
            match_details_bucketed.player_gamertag,
            match_details_bucketed.player_total_kills,
            medal_matches_players_bucketed.medal_id,
            medal_matches_players_bucketed["count"].alias("medal_count"),
            medals.name.alias("medal_name"),
            maps.name.alias("map_name"),
        )
    )

    return result_df


if __name__ == "__main__":
    # Start spark session and add configuration
    spark = SparkSession.builder.appName("matches").getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Read csv files
    match_details = spark.read.option("header", "true").csv(f"{ROOT}/match_details.csv")
    matches = spark.read.option("header", "true").csv(f"{ROOT}/matches.csv")
    medals_matches_players = spark.read.option("header", "true").csv(
        f"{ROOT}/medals_matches_players.csv"
    )
    medals = spark.read.option("header", "true").csv(f"{ROOT}/medals.csv")
    maps = spark.read.option("header", "true").csv(f"{ROOT}/maps.csv")

    medals = broadcast(medals)
    maps = broadcast(maps)

    # Drop tables
    drop_tables(spark)

    # Create tables
    create_tables(spark)

    # Insert data
    insert_data(spark, match_details, matches, medals_matches_players)

    # Perform bucketed join
    result_df = bucketed_join(spark, medals, maps)

    # Answer the questions

    # - Which player averages the most kills per game?

    result_df.groupBy("player_gamertag").agg(
        sparksum(col("player_total_kills")).alias("total_kills")
    ).orderBy(desc("total_kills")).limit(1).show()

    # +---------------+-----------+
    # |player_gamertag|total_kills|
    # +---------------+-----------+
    # |       EcZachly|    1503498|
    # +---------------+-----------+

    # - Which playlist gets played the most?
    result_df.groupBy("playlist_id").count().orderBy(desc("count")).limit(1).show(
        truncate=False
    )

    # +------------------------------------+-------+
    # |playlist_id                         |count  |
    # +------------------------------------+-------+
    # |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1565529|
    # +------------------------------------+-------+

    #     - Which map gets played the most?
    result_df.groupBy("map_id", "map_name").count().orderBy(desc("count")).limit(
        1
    ).show(truncate=False)

    # +------------------------------------+--------+-------+
    # |map_id                              |map_name|count  |
    # +------------------------------------+--------+-------+
    # |c74c9d0f-f206-11e4-8330-24be05e24f7e|Alpine  |1445545|
    # +------------------------------------+--------+-------+

    #  - Which map do players get the most Killing Spree medals on?

    result_df.where(col("medal_name") == "Killing Frenzy").groupBy(
        "map_id", "map_name", "medal_name"
    ).count().orderBy(desc("count")).limit(1).show(truncate=False)

    # +------------------------------------+--------+--------------+-----+
    # |map_id                              |map_name|medal_name    |count|
    # +------------------------------------+--------+--------------+-----+
    # |c74c9d0f-f206-11e4-8330-24be05e24f7e|Alpine  |Killing Frenzy|5872 |
    # +------------------------------------+--------+--------------+-----+

    spark.stop()
