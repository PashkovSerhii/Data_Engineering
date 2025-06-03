import pandas as pd

# Налаштування pandas для показу всіх стовпців
pd.set_option('display.max_columns', None)  # Показати всі стовпці
pd.set_option('display.width', None)        # Без обмеження ширини
pd.set_option('display.max_colwidth', None) # Показати повний вміст стовпців

# Читання перших 5 рядків
# ad_events = pd.read_csv('data/raw/ad_events.csv', nrows=5)
# Зазначаємо правильні імена колонок
columns = [
    'EventID', 'AdvertiserName', 'CampaignName', 'CampaignStartDate', 'CampaignEndDate',
    'TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3',
    'AdSlotSize', 'UserID', 'Device', 'Location', 'Timestamp',
    'BidAmount', 'AdCost', 'WasClicked', 'ClickTimestamp',
    'AdRevenue', 'Budget', 'RemainingBudget'
]

# Зчитуємо CSV без заголовків
ad_events = pd.read_csv('data/raw/ad_events.csv', names=columns, header=0, nrows=5)

# Об'єднуємо три частини TargetingCriteria в один стовпець
ad_events['TargetingCriteria'] = ad_events[['TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3']].apply(
    lambda row: ', '.join(row.astype(str)).strip(), axis=1
)

# Видаляємо зайві колонки
ad_events.drop(['TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3'], axis=1, inplace=True)
print("=== AD EVENTS ===")
print(ad_events)
print("\n")

campaigns = pd.read_csv('data/raw/campaigns.csv', nrows=5)
print("=== CAMPAIGNS ===")
print(campaigns)
print("\n")

users = pd.read_csv('data/raw/users.csv', nrows=5)
print("=== USERS ===")
print(users)

print("=== СТРУКТУРА ТАБЛИЦЬ ===")
print(f"ad_events: {ad_events.shape[0]} рядків, {ad_events.shape[1]} стовпців")
print("Стовпці:", list(ad_events.columns))
print("\n")

print(f"campaigns: {campaigns.shape[0]} рядків, {campaigns.shape[1]} стовпців")
print("Стовпці:", list(campaigns.columns))
print("\n")

print(f"users: {users.shape[0]} рядків, {users.shape[1]} стовпців")
print("Стовпці:", list(users.columns))