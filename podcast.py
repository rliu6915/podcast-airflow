import os
import json
import requests
import xmltodict

from airflow.decorators import dag, task
import pendulum

# from airflow.providers.sqlite.operators.sqlite import SqliteOperator
# from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime

from airflow.providers.mongo.hooks.mongo import MongoHook

# URL of the podcast RSS feed
PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"

# MongoDB connection details
DB_NAME = "podcasts"  # Database name in MongoDB
COLLECTION_NAME = "episodes"  # Collection name where episodes will be stored

# Define the Airflow DAG
@dag(
    dag_id='podcast_summary_final',  # Unique identifier for the DAG
    # dag_id='podcast_summary_mongodb',  # Unique identifier for the DAG
    schedule_interval="@daily",  # Schedule interval (daily in this case)
    start_date=pendulum.datetime(2024, 8, 12),  # Start date for the DAG
    catchup=False,  # Whether to catch up on missed runs
)

def podcast_summary():

    # Helper function to get a MongoDB collection object
    def get_mongo_collection():
        mongo_hook = MongoHook(conn_id='podcasts_mongo')  # Use the connection ID you've set up in Airflow
        client = mongo_hook.get_conn()  # Establish connection to MongoDB
        db = client[DB_NAME]  # Access the specified database
        collection = db[COLLECTION_NAME]  # Access the specified collection
        return collection

    # Task to fetch episodes from the podcast RSS feed
    @task()
    def get_episodes():
        print("Fetching episodes from the podcast RSS feed...")
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = get_episodes()

    # Task to add metadata to each episode
    @task()
    def add_metadata(episodes):
        for episode in episodes:
            # Calculate the length of the title and description
            title_length = len(episode["title"])
            description_length = len(episode["description"])

            # Generate a hash from the episode link (could be used for indexing)
            link_hash = hash(episode["link"])

            # Add these calculated fields as new metadata to the episode
            episode["title_length"] = title_length
            episode["description_length"] = description_length
            episode["link_hash"] = link_hash

        return episodes  # Return the modified list of episodes with metadata

    episodes_with_metadata = add_metadata(podcast_episodes)  # Call the add_metadata task

    # Task to load episodes into MongoDB
    @task()
    def load_episodes(episodes):
        collection = get_mongo_collection()  # Get the MongoDB collection object
        new_episodes = []  # Initialize a list to hold new episodes

        # Loop through the episodes to check if they already exist in the database
        for episode in episodes:
            # Check if the episode already exists in the MongoDB collection by its link
            if not collection.find_one({"link": episode["link"]}):
                # If the episode does not exist, append it to the new_episodes list
                new_episodes.append({
                    "link": episode["link"],
                    "title": episode["title"],
                    "published": episode["pubDate"], 
                    "description": episode["description"],
                    "filename": f"{episode['link'].split('/')[-1]}.mp3",  # Generate a filename from the episode link
                    "transcript": None,  # Initialize the transcript field as None (to be filled later)
                    "title_length": episode["title_length"],  # Include the title length metadata
                    "description_length": episode["description_length"],  # Include the description length metadata
                    "link_hash": episode["link_hash"],  # Include the link hash metadata
                    # "_id": str(episode["_id"])
                })

        # Insert the new episodes into the MongoDB collection
        if new_episodes:
            collection.insert_many(new_episodes)
            # It does not change the mongoDB
            for i in range(0, len(new_episodes)):
              new_episodes[i]["_id"] = str(new_episodes[i]["_id"])
        
        return new_episodes  # Return the list of new episodes inserted into the database

    new_episodes = load_episodes(episodes_with_metadata)  # Call the load_episodes task


summary = podcast_summary()