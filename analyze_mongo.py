import csv
import json
from pymongo import MongoClient

mongo_client = MongoClient("mongodb://mongo:27017/")
users_collection = mongo_client["adtech"]["users_engagement"]

def dump_to_json(data, filename):
    with open(filename, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def dump_to_csv(data, filename):
    if not data:
        return
    keys = data[0].keys()
    with open(filename, "w", newline='', encoding="utf8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

# 1. Get all ad interactions (impressions + clicks) for a user
def get_user_interactions(user_id, filename="user_interactions.json"):
    doc = users_collection.find_one({"user_id": user_id})
    interactions = []
    if doc:
        for session in doc['ad_sessions']:
            for imp in session['ad_impressions']:
                record = {
                    "session_id": session["session_id"],
                    "device": session["device"],
                    "impression_id": imp["impression_id"],
                    "ad_id": imp["ad_id"],
                    "campaign_id": imp.get("campaign_id"),
                    "timestamp": imp["timestamp"],
                    "was_clicked": imp.get("was_clicked"),
                    "click": imp.get("click")
                }
                interactions.append(record)
    dump_to_json(interactions, filename)
    print(f"Wrote {len(interactions)} user interactions to {filename}")

# 2. Last 5 sessions for a user with timestamps and clicks
def get_last5_sessions(user_id, filename="last5_sessions.json"):
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$project": {"ad_sessions": {"$slice": ["$ad_sessions", -5]}, "_id":0}}
    ]
    docs = list(users_collection.aggregate(pipeline))
    sessions = []
    if docs:
        for session in docs[0]["ad_sessions"]:
            clicks_in_session = sum(1 for imp in session["ad_impressions"] if imp.get("was_clicked"))
            sessions.append({
                "session_id": session["session_id"],
                "device": session["device"],
                "start_time": session["start_time"],
                "num_ads": len(session["ad_impressions"]),
                "num_clicks": clicks_in_session,
            })
    dump_to_json(sessions, filename)
    print(f"Wrote last 5 sessions for user to {filename}")

# 3. Clicks per hour per campaign in the last 24 hours for an advertiser
def clicks_per_hour_campaign(advertiser_campaign_ids, filename="clicks_per_hour.json"):
    from datetime import datetime, timedelta
    last_24h = (datetime.utcnow() - timedelta(hours=24)).isoformat()

    pipeline = [
        {"$unwind": "$ad_sessions"},
        {"$unwind": "$ad_sessions.ad_impressions"},
        {"$match": {
            "ad_sessions.ad_impressions.campaign_id": {"$in": advertiser_campaign_ids},
            "ad_sessions.ad_impressions.timestamp": {"$gte": last_24h},
            "ad_sessions.ad_impressions.was_clicked": True
        }},
        {"$project": {
            "campaign_id": "$ad_sessions.ad_impressions.campaign_id",
            "click_hour": {"$hour": {"$dateFromString":{"dateString":"$ad_sessions.ad_impressions.timestamp"}}}
        }},
        {"$group": {
            "_id": {"campaign_id":"$campaign_id", "hour":"$click_hour"},
            "num_clicks": {"$sum":1}
        }},
        {"$sort": {"_id.campaign_id":1, "_id.hour":1}}
    ]
    out = list(users_collection.aggregate(pipeline))
    records = [{
        "campaign_id": row["_id"]["campaign_id"],
        "hour": row["_id"]["hour"],
        "clicks": row["num_clicks"]
    } for row in out]
    dump_to_csv(records, filename)
    print(f"Wrote campaign clicks per hour to {filename}")

# 4. Users who have seen the same ad 5+ times but never clicked
def find_ad_fatigued_users(filename="ad_fatigued_users.json"):
    pipeline = [
        {"$unwind": "$ad_sessions"},
        {"$unwind": "$ad_sessions.ad_impressions"},
        {"$group": {
            "_id": {"user_id":"$user_id", "ad_id":"$ad_sessions.ad_impressions.ad_id"},
            "impressions": {"$sum": 1},
            "total_clicked": {"$sum":{
                "$cond":[{"$eq":["$ad_sessions.ad_impressions.was_clicked", True]},1,0]
            }},
        }},
        {"$match": {"impressions": {"$gte":5}, "total_clicked":0}},
        {"$group": {
            "_id": "$_id.user_id",
            "ad_ids": {"$addToSet":"$_id.ad_id"},
            "num_ads": {"$sum":1}
        }}
    ]
    fatigued = list(users_collection.aggregate(pipeline))
    dump_to_json(fatigued, filename)
    print(f"Wrote ad-fatigued users to {filename}")

# 5. User's top 3 engaged ad categories based on past clicks
def top3_ad_categories(user_id, filename="top3_ad_categories.json"):
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$unwind": "$ad_sessions"},
        {"$unwind": "$ad_sessions.ad_impressions"},
        {"$match": {"ad_sessions.ad_impressions.was_clicked": True}},
        {"$group": {
            "_id": "$ad_sessions.ad_impressions.ad_category",
            "clicks": {"$sum": 1}
        }},
        {"$sort": {"clicks": -1}},
        {"$limit": 3}
    ]
    top = list(users_collection.aggregate(pipeline))
    result = [{"category": x["_id"], "clicks": x["clicks"]} for x in top]
    dump_to_csv(result, filename)
    print(f"Wrote top 3 categories to {filename}")


if __name__ == "__main__":
    get_user_interactions(user_id=12345)                # Example user_id
    get_last5_sessions(user_id=12345)
    clicks_per_hour_campaign(advertiser_campaign_ids=[11,12,13])
    find_ad_fatigued_users()
    top3_ad_categories(user_id=12345)
