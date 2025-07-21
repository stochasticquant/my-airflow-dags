import json
import requests
from airflow.hooks.base import BaseHook
import psycopg2
import pymongo
from neo4j import GraphDatabase
import qdrant_client
from qdrant_client.models import PointStruct, VectorParams, Distance
import hashlib


def get_conn_params(conn_id):
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "port": conn.port,
        "username": conn.login,
        "password": conn.password,
        "schema": conn.schema
    }


def load_to_postgres(launch):
    params = get_conn_params("postgres_conn")
    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["schema"],
        user=params["username"],
        password=params["password"]
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS launches (
            id TEXT PRIMARY KEY,
            mission TEXT,
            net TIMESTAMP,
            provider TEXT,
            rocket TEXT,
            pad TEXT,
            orbit TEXT,
            description TEXT
        )
    """)
    cursor.execute("""
        INSERT INTO launches (id, mission, net, provider, rocket, pad, orbit, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            mission = EXCLUDED.mission,
            net = EXCLUDED.net,
            provider = EXCLUDED.provider,
            rocket = EXCLUDED.rocket,
            pad = EXCLUDED.pad,
            orbit = EXCLUDED.orbit,
            description = EXCLUDED.description
    """, (
        launch["id"], launch["mission"], launch["net"], launch["provider"],
        launch["rocket"], launch["pad"], launch["orbit"], launch["description"]
    ))
    conn.commit()
    cursor.close()
    conn.close()


def load_to_mongodb(launch):
    params = get_conn_params("mongo_conn")
    client = pymongo.MongoClient(f"mongodb://{params['username']}:{params['password']}@{params['host']}:{params['port']}")
    db = client[params["schema"]]
    collection = db.launches
    collection.replace_one({"id": launch["id"]}, launch, upsert=True)


def load_to_neo4j(launch):
    params = get_conn_params("neo4j_conn")
    uri = f"bolt://{params['host']}:{params['port']}"
    driver = GraphDatabase.driver(uri, auth=(params['username'], params['password']))

    with driver.session() as session:
        session.run("""
            MERGE (l:Launch {id: $id})
            SET l.mission = $mission,
                l.net = $net,
                l.provider = $provider,
                l.rocket = $rocket,
                l.pad = $pad,
                l.orbit = $orbit,
                l.description = $description
        """, launch)
    driver.close()


def load_to_qdrant(launch):
    params = get_conn_params("qdrant_conn")
    client = qdrant_client.QdrantClient(host=params['host'], port=params['port'])

    # Ensure the collection exists with expected vector parameters
    collection_name = "launch_vectors"
    client.recreate_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=384, distance=Distance.COSINE)  # placeholder size
    )

    # Create a fake embedding (for now, hash-based deterministic vector)
    vector = [float((int(hashlib.sha256((launch["id"] + k).encode()).hexdigest(), 16) % 100) / 100) for k in ["a", "b", "c"] * 128][:384]

    client.upsert(
        collection_name=collection_name,
        points=[
            PointStruct(id=launch["id"], vector=vector, payload=launch)
        ]
    )
