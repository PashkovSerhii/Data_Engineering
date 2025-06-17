import pandas as pd
from pymongo import MongoClient
import uuid
from datetime import datetime

# Connect to MongoDB
mongo_client = MongoClient("mongodb://mongo:27017/")
mongodb = mongo_client["adtech"]
users_collection = mongodb["users_engagement"]

# Load users, ad_events, and campaigns datasets
users = pd.read_csv("data/raw/users.csv")

columns = [
    'EventID', 'AdvertiserName', 'CampaignName', 'CampaignStartDate', 'CampaignEndDate',
    'TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3',
    'AdSlotSize', 'UserID', 'Device', 'Location', 'Timestamp',
    'BidAmount', 'AdCost', 'WasClicked', 'ClickTimestamp',
    'AdRevenue', 'Budget', 'RemainingBudget'
]
ad_events = pd.read_csv('data/raw/ad_events.csv', names=columns, nrows=1000, skiprows=1)

# Об'єднуємо три частини TargetingCriteria в один стовпець
ad_events['TargetingCriteria'] = ad_events[['TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3']].apply(
    lambda row: ', '.join(row.astype(str)).strip(), axis=1
)

# Видаляємо зайві колонки
ad_events.drop(['TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3'], axis=1, inplace=True)

campaigns = pd.read_csv("data/raw/campaigns.csv")

# Preprocess ad events by user and session (simple approach: group by UserID, Device, every 30 minutes == new session)
ad_events["Timestamp"] = pd.to_datetime(ad_events["Timestamp"])
ad_events = ad_events.sort_values(["UserID", "Device", "Timestamp"])

def sessionize(df, time_window="30min"):
    df['session_marker'] = df['Timestamp'].diff().gt(pd.Timedelta(time_window)).cumsum()
    return df

result_docs = []
for user_id, user_group in ad_events.groupby("UserID"):
    user_row = users[users['UserID'] == user_id].iloc[0]
    # Assign sessions
    sessions = []
    for device, device_group in user_group.groupby("Device"):
        # Sessionize
        device_group = sessionize(device_group)
        for session_id, sess_df in device_group.groupby("session_marker"):
            ad_impressions = []
            for _, row in sess_df.iterrows():
                impression = {
                    "impression_id": str(uuid.uuid4()),
                    "campaign_id": int(row["CampaignID"]) if "CampaignID" in row and not pd.isna(row["CampaignID"]) else None,
                    "ad_id": str(row["EventID"]),
                    "timestamp": row["Timestamp"].isoformat(),
                    "ad_category": None,  # Optional: map from campaigns if needed
                    "was_clicked": bool(row["WasClicked"]),
                }
                if impression["was_clicked"]:
                    impression["click"] = {
                        "click_timestamp": pd.to_datetime(row.get("ClickTimestamp")).isoformat() if pd.notna(row.get("ClickTimestamp")) else None
                    }
                ad_impressions.append(impression)
            sessions.append({
                "session_id": str(uuid.uuid4()),
                "device": device,
                "start_time": sess_df["Timestamp"].min().isoformat(),
                "ad_impressions": ad_impressions
            })

    doc = {
        "user_id": int(user_row.UserID),
        "age": int(user_row.Age),
        "gender": user_row.Gender,
        "location": user_row.Location,
        "interests": [i.strip() for i in str(user_row.Interests).split(",") if i and i != "nan"],
        "ad_sessions": sessions
    }
    result_docs.append(doc)

# Insert into MongoDB (erase collection first)
users_collection.drop()
users_collection.insert_many(result_docs)
print(f"Inserted {len(result_docs)} user engagement docs into MongoDB")