from __future__ import annotations
from typing import Any, Dict, List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from bson import json_util, ObjectId
import json
import os

class MongoProcessor:
    def __init__(
        self,
        uri: Optional[str] = None,
        database_name: str = "osint",
        topic_collection_name: str = "topic_v2",
        group_collection_name: str = "topic_group",
        tenancy_collection_name: str = "tenancies",
        server_selection_timeout_ms: int = 5000,
    ) -> None:
        if uri is None:
            host = os.getenv("MONGOS_HOST")
            port = os.getenv("MONGOS_PORT")
            uri = f"mongodb://{host}:{port}"

        self.client = MongoClient(uri, serverSelectionTimeoutMS=server_selection_timeout_ms)
        self.db = self.client[database_name]
        self.collection: Collection = self.db[topic_collection_name]
        self.topic_group_collection: Collection = self.db[group_collection_name]
        self.tenancies_collection: Collection = self.db[tenancy_collection_name]

    def __enter__(self) -> "MongoProcessor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def get_active_tenancy_ids(self) -> List[str]:
        cursor = self.tenancies_collection.find({"status": 0}, {"tenancy_id": 1})
        tenancy_ids: List[str] = []
        for doc in cursor:
            tid = doc.get("tenancy_id")
            if isinstance(tid, str) and tid:
                tenancy_ids.append(tid)
            elif "_id" in doc and doc["_id"]:
                tenancy_ids.append(str(doc["_id"]))
        return tenancy_ids

    def get_active_topic_group_ids(self) -> List[ObjectId]:
        return list(self.topic_group_collection.distinct("_id", {"is_deleted": False, "status": 0}))

    @staticmethod
    def _normalize_keywords(raw: Any) -> Optional[List[str]]:
        try:
            if isinstance(raw, str):
                parsed = json.loads(raw)
            else:
                parsed = raw
        except json.JSONDecodeError:
            return None

        if isinstance(parsed, list):
            out: List[str] = []
            for item in parsed:
                if isinstance(item, dict) and "key" in item and isinstance(item["key"], str):
                    out.append(item["key"])
                elif isinstance(item, str):
                    out.append(item)
            return out if out else None

        if isinstance(parsed, dict) and "key" in parsed and isinstance(parsed["key"], str):
            return [parsed["key"]]
        return None

    @staticmethod
    def _extract_tenancy_id_from_topic(doc: Dict[str, Any]) -> Optional[str]:
        for f in ("tenancy_id", "tenant_id", "tenant"):
            if f in doc and doc[f] is not None:
                v = doc[f]
                if isinstance(v, str) and v:
                    return v
                if isinstance(v, ObjectId):
                    return str(v)
        return None

    def get_grouped_topic_ids_by_tenant(self) -> List[Dict[str, Any]]:

        active_tenancies = set(self.get_active_tenancy_ids())
        if not active_tenancies:
            return []

        active_group_ids = self.get_active_topic_group_ids()
        if not active_group_ids:
            return []

        query = {
            "is_deleted": False,
            "status": 0,
            "group_id": {"$in": active_group_ids},
        }
        projection = {
            "_id": 1,
            "name": 1,
            "keyword": 1,
            "group_id": 1,
            "tenancy_id": 1,
            "tenant_id": 1,
            "tenant": 1,
        }

        grouped: Dict[str, List[str]] = {} 
        for doc in self.collection.find(query, projection):
            tenancy_id = self._extract_tenancy_id_from_topic(doc)
            if tenancy_id is None or tenancy_id not in active_tenancies:
                continue
            topic_id_str = str(doc.get("_id"))
            grouped.setdefault(tenancy_id, []).append(topic_id_str)

        results: List[Dict[str, Any]] = []
        for tenancy_id, topics in grouped.items():
            results.append({"tenant": tenancy_id, "topic_id": topics})
        return results

    def close(self) -> None:
        self.client.close()


def query_topic_id_grouped_by_tenant() -> List[Dict[str, Any]]:
    with MongoProcessor() as db:
        return db.get_grouped_topic_ids_by_tenant()



