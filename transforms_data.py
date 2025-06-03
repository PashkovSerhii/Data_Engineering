import pandas as pd
import mysql.connector
from mysql.connector import Error


class AdTechDataTransformer:
    def __init__(self, connection_config):
        self.config = connection_config
        self.connection = None

    def connect_db(self):
        """Підключення до MySQL"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            print("✅ Підключення до MySQL успішне")
        except Error as e:
            print(f"❌ Помилка підключення: {e}")

    def load_csv_data(self):
        """Завантаження CSV файлів"""
        print("📂 Завантаження CSV файлів...")
        # self.ad_events = pd.read_csv('data/raw/ad_events.csv')
        columns = [
            'EventID', 'AdvertiserName', 'CampaignName', 'CampaignStartDate', 'CampaignEndDate',
            'TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3',
            'AdSlotSize', 'UserID', 'Device', 'Location', 'Timestamp',
            'BidAmount', 'AdCost', 'WasClicked', 'ClickTimestamp',
            'AdRevenue', 'Budget', 'RemainingBudget'
        ]

        # Зчитуємо CSV без заголовків
        ad_events = pd.read_csv('data/raw/ad_events.csv', names=columns, header=None)

        # Об'єднуємо три частини TargetingCriteria в один стовпець
        ad_events['TargetingCriteria'] = ad_events[
            ['TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3']].apply(
            lambda row: ', '.join(row.astype(str)).strip(), axis=1
        )

        # Видаляємо зайві колонки
        self.ad_events = ad_events.drop(['TargetingCriteria1', 'TargetingCriteria2', 'TargetingCriteria3'], axis=1)
        self.campaigns = pd.read_csv('data/raw/campaigns.csv')
        self.users = pd.read_csv('data/raw/users.csv')
        print(
            f"✅ Завантажено: {len(self.ad_events)} подій, {len(self.campaigns)} кампаній, {len(self.users)} користувачів")

    def clear_existing_data(self):
        """Очищення існуючих даних (опціонально)"""
        cursor = self.connection.cursor()
        try:
            # Видаляємо в правильному порядку (зворотно до залежностей)
            cursor.execute("DELETE FROM clicks")
            cursor.execute("DELETE FROM ad_events")
            cursor.execute("DELETE FROM user_interests")
            cursor.execute("DELETE FROM users")
            cursor.execute("DELETE FROM campaigns")
            cursor.execute("DELETE FROM advertisers")

            # Скидаємо AUTO_INCREMENT
            cursor.execute("ALTER TABLE advertisers AUTO_INCREMENT = 1")
            cursor.execute("ALTER TABLE campaigns AUTO_INCREMENT = 1")
            cursor.execute("ALTER TABLE clicks AUTO_INCREMENT = 1")

            self.connection.commit()
            print("🧹 Існуючі дані очищено")
        except Error as e:
            print(f"⚠️ Помилка при очищенні: {e}")
            self.connection.rollback()
        finally:
            cursor.close()

    def get_or_create_advertiser_mapping(self):
        """Отримання або створення мапінгу рекламодавців"""
        cursor = self.connection.cursor()
        advertiser_mapping = {}

        try:
            # Отримуємо існуючих рекламодавців
            cursor.execute("SELECT advertiser_id, advertiser_name FROM advertisers")
            existing_advertisers = cursor.fetchall()

            for adv_id, adv_name in existing_advertisers:
                advertiser_mapping[adv_name] = adv_id

            # Додаємо нових рекламодавців
            unique_advertisers = self.campaigns['AdvertiserName'].unique()

            for advertiser_name in unique_advertisers:
                if advertiser_name not in advertiser_mapping:
                    try:
                        cursor.execute(
                            "INSERT INTO advertisers (advertiser_name) VALUES (%s)",
                            (advertiser_name,)
                        )
                        advertiser_mapping[advertiser_name] = cursor.lastrowid
                    except Error as e:
                        if e.errno == 1062:  # Duplicate entry
                            # Якщо дублікат, отримуємо існуючий ID
                            cursor.execute(
                                "SELECT advertiser_id FROM advertisers WHERE advertiser_name = %s",
                                (advertiser_name,)
                            )
                            result = cursor.fetchone()
                            if result:
                                advertiser_mapping[advertiser_name] = result[0]
                        else:
                            raise e

            self.connection.commit()
            print(f"✅ Оброблено {len(advertiser_mapping)} рекламодавців")
            return advertiser_mapping

        except Error as e:
            print(f"❌ Помилка при роботі з рекламодавцями: {e}")
            self.connection.rollback()
            return {}
        finally:
            cursor.close()

    def transform_campaigns(self, advertiser_mapping):
        """Трансформація кампаній з ID рекламодавців"""
        campaigns_transformed = self.campaigns.copy()
        campaigns_transformed['advertiser_id'] = campaigns_transformed['AdvertiserName'].map(advertiser_mapping)

        # Перевіряємо, чи всі рекламодавці знайдені
        missing_advertisers = campaigns_transformed[campaigns_transformed['advertiser_id'].isna()]
        if not missing_advertisers.empty:
            print(f"⚠️ Не знайдено advertiser_id для: {missing_advertisers['AdvertiserName'].unique()}")

        return campaigns_transformed[['CampaignID', 'advertiser_id', 'CampaignName',
                                      'CampaignStartDate', 'CampaignEndDate',
                                      'TargetingCriteria', 'AdSlotSize', 'Budget', 'RemainingBudget']].dropna()

    def transform_users_and_interests(self):
        """Трансформація користувачів та їх інтересів"""
        users_clean = self.users[['UserID', 'Age', 'Gender', 'Location', 'SignupDate']].copy()

        # Обробка інтересів
        interests_data = []
        for _, row in self.users.iterrows():
            user_id = row['UserID']
            interests = str(row['Interests']).split(',')
            for interest in interests:
                interest = interest.strip()
                if interest and interest != 'nan':
                    interests_data.append({'user_id': user_id, 'interest': interest})

        interests_df = pd.DataFrame(interests_data)
        return users_clean, interests_df

    def transform_ad_events_and_clicks(self):
        """Трансформація подій та кліків"""
        # Створюємо мапінг назв кампаній до ID
        campaign_mapping = dict(zip(self.campaigns['CampaignName'], self.campaigns['CampaignID']))

        events_clean = self.ad_events.copy()
        events_clean['campaign_id'] = events_clean['CampaignName'].map(campaign_mapping)

        # Видаляємо події без campaign_id
        events_clean = events_clean.dropna(subset=['campaign_id'])

        # Розділяємо на події та кліки
        ad_events_final = events_clean[['EventID', 'campaign_id', 'UserID', 'Device',
                                        'Location', 'Timestamp', 'BidAmount', 'AdCost', 'AdRevenue']].copy()

        # Тільки кліки
        clicks_data = events_clean[events_clean['WasClicked'] == True][['EventID', 'ClickTimestamp']].copy()
        clicks_data = clicks_data.dropna(subset=['ClickTimestamp'])

        return ad_events_final, clicks_data

    def insert_data_batch(self, cursor, query, data, batch_size=1000):
        """Пакетна вставка даних"""
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            try:
                cursor.executemany(query, batch)
                self.connection.commit()
            except Error as e:
                print(f"❌ Помилка в пакеті {i // batch_size + 1}: {e}")
                self.connection.rollback()
                # Спробуємо вставити по одному запису
                for row in batch:
                    try:
                        cursor.execute(query, row)
                        self.connection.commit()
                    except Error as row_error:
                        print(f"⚠️ Пропускаємо запис: {row_error}")
                        self.connection.rollback()
                        continue

    def insert_data_to_db(self):
        """Вставка даних до бази"""
        cursor = self.connection.cursor()

        try:
            # 1. Рекламодавці (з перевіркою дублікатів)
            advertiser_mapping = self.get_or_create_advertiser_mapping()

            # 2. Кампанії
            campaigns_transformed = self.transform_campaigns(advertiser_mapping)
            campaign_data = [tuple(row) for _, row in campaigns_transformed.iterrows()]

            self.insert_data_batch(cursor, """
                INSERT IGNORE INTO campaigns (campaign_id, advertiser_id, campaign_name, 
                                     start_date, end_date, targeting_criteria, 
                                     ad_slot_size, budget, remaining_budget)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, campaign_data)
            print(f"✅ Вставлено {len(campaigns_transformed)} кампаній")

            # 3. Користувачі
            users_clean, interests_df = self.transform_users_and_interests()
            user_data = [tuple(row) for _, row in users_clean.iterrows()]

            self.insert_data_batch(cursor, """
                INSERT IGNORE INTO users (user_id, age, gender, location, signup_date)
                VALUES (%s, %s, %s, %s, %s)
            """, user_data)
            print(f"✅ Вставлено {len(users_clean)} користувачів")

            # 4. Інтереси користувачів
            if not interests_df.empty:
                interest_data = [(row['user_id'], row['interest']) for _, row in interests_df.iterrows()]
                self.insert_data_batch(cursor, """
                    INSERT IGNORE INTO user_interests (user_id, interest) VALUES (%s, %s)
                """, interest_data)
                print(f"✅ Вставлено {len(interests_df)} інтересів")

            # 5. Рекламні події
            ad_events_final, clicks_data = self.transform_ad_events_and_clicks()
            event_data = [tuple(row) for _, row in ad_events_final.iterrows()]

            self.insert_data_batch(cursor, """
                INSERT IGNORE INTO ad_events (event_id, campaign_id, user_id, device, 
                                     location, timestamp, bid_amount, ad_cost, ad_revenue)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, event_data)
            print(f"✅ Вставлено {len(ad_events_final)} рекламних подій")

            # 6. Кліки
            if not clicks_data.empty:
                click_data = [(row['EventID'], row['ClickTimestamp']) for _, row in clicks_data.iterrows()]
                self.insert_data_batch(cursor, """
                    INSERT IGNORE INTO clicks (event_id, click_timestamp) VALUES (%s, %s)
                """, click_data)
                print(f"✅ Вставлено {len(clicks_data)} кліків")

            print("🎉 Всі дані успішно перенесені!")

        except Error as e:
            print(f"❌ Загальна помилка при вставці: {e}")
            self.connection.rollback()
        finally:
            cursor.close()

    def run_full_import(self, clear_data=False):
        """Повний імпорт даних"""
        try:
            self.connect_db()
            if not self.connection:
                return

            self.load_csv_data()

            if clear_data:
                self.clear_existing_data()

            self.insert_data_to_db()

        finally:
            if self.connection:
                self.connection.close()
                print("🔌 З'єднання закрито")


# Конфігурація підключення
config = {
    'host': 'mysql',
    'port': 3306,
    'database': 'adtech_db',
    'user': 'adtech_user',
    'password': 'adtech_pass'
}

# # Використання
# if __name__ == "__main__":
transformer = AdTechDataTransformer(config)

# Запуск з очищенням даних (якщо потрібно)
transformer.run_full_import(clear_data=True)  # True для очищення існуючих даних

# Або без очищення (дані додадуться до існуючих)
# transformer.run_full_import(clear_data=False)