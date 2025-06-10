import mysql.connector
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine

# Формуємо connection string (з docker-compose!):
# dialect+driver://username:password@host:port/database

db_url = "mysql+mysqlconnector://adtech_user:adtech_pass@mysql:3306/adtech_db"
engine = create_engine(db_url)
# conn = mysql.connector.connect(
#     host='mysql',           # ← має бути так!
#     user='adtech_user',     # Бери користувача і пароль з docker-compose.yml
#     password='adtech_pass',
#     database='adtech_db'
# )
date1, date2 = '2024-10-10', '2024-11-09'
filter_ = '''
                --WHERE
                --    ae.timestamp BETWEEN '{date1}' AND '{date2}' '''
queries = {
    # Топ-5 кампаній за CTR (Click-Through Rate)
    'top_campaigns_by_ctr': f'''
                        SELECT
                    c.campaign_id,
                    c.campaign_name,
                    COUNT(ae.event_id) AS impressions,
                    COUNT(cl.click_id) AS clicks,
                    ROUND(COUNT(cl.click_id)/COUNT(ae.event_id), 4) AS ctr
                FROM
                    ad_events ae
                LEFT JOIN campaigns c ON ae.campaign_id = c.campaign_id
                LEFT JOIN clicks cl ON ae.event_id = cl.event_id
                GROUP BY
                    c.campaign_id, c.campaign_name
                ORDER BY
                    ctr DESC
                LIMIT 5;
    ''',
    # 1.2. Рекламодавці з найбільшими витратами (impressions)
    'top_advertisers_spending': f'''
                        SELECT
                    adv.advertiser_id,
                    adv.advertiser_name,
                    ROUND(SUM(ae.ad_cost),2) AS total_spent,
                    COUNT(ae.event_id) AS impressions,
                    COUNT(cl.click_id) AS clicks
                FROM
                    ad_events ae
                JOIN campaigns c ON ae.campaign_id = c.campaign_id
                JOIN advertisers adv ON c.advertiser_id = adv.advertiser_id
                LEFT JOIN clicks cl ON ae.event_id = cl.event_id
                GROUP BY
                    adv.advertiser_id, adv.advertiser_name
                ORDER BY
                    total_spent DESC
                LIMIT 10;
    ''',
    # 1.3. Середній CPC та CPM по кампаніях
    'avg_cpc_cpm_per_campaign': f'''
                    SELECT
                c.campaign_id,
                c.campaign_name,
                ROUND(SUM(ae.ad_cost)/NULLIF(COUNT(cl.click_id),0), 4) as avg_cpc,
                ROUND(SUM(ae.ad_cost)/NULLIF(COUNT(ae.event_id),0) * 1000, 4) as avg_cpm
            FROM
                ad_events ae
            JOIN campaigns c ON ae.campaign_id = c.campaign_id
            LEFT JOIN clicks cl ON ae.event_id = cl.event_id
            GROUP BY
                c.campaign_id, c.campaign_name
            ORDER BY
                avg_cpc ASC;
    ''',
    # 1.4. ТОП-локації за доходом (ad_revenue)
    'top_locations_by_revenue': f'''
                        SELECT
                    ae.location,
                    ROUND(SUM(ae.ad_revenue), 2) as total_revenue
                FROM
                    ad_events ae
                GROUP BY
                    ae.location
                ORDER BY
                    total_revenue DESC
                LIMIT 10;
    ''',
    # 5. Топ-10 найактивніших користувачів (кліки по рекламі)
    'top_users_by_clicks': f'''
                    SELECT
                u.user_id,
                u.age,
                u.gender,
                u.location,
                COUNT(cl.click_id) as clicks
            FROM
                users u
            JOIN ad_events ae ON u.user_id = ae.user_id
            JOIN clicks cl ON ae.event_id = cl.event_id
            GROUP BY
                u.user_id, u.age, u.gender, u.location
            ORDER BY
                clicks DESC
            LIMIT 10;
    ''',
    # 6. Кампанії, що витратили більше 80% бюджету
    'campaigns_near_budget_limit': f'''
                    SELECT
                campaign_id,
                campaign_name,
                budget,
                remaining_budget,
                ROUND((budget-remaining_budget)/budget*100,2) as percent_spent
            FROM
                campaigns
            WHERE
                (budget-remaining_budget)/budget > 0.8;
    ''',
    # 7. Порівняння CTR по пристроях
    'device_ctr_comparison': f'''
                    SELECT
                ae.device,
                COUNT(ae.event_id) as impressions,
                COUNT(cl.click_id) as clicks,
                ROUND(COUNT(cl.click_id)/COUNT(ae.event_id), 4) as ctr
            FROM
                ad_events ae
            LEFT JOIN clicks cl ON ae.event_id = cl.event_id
            GROUP BY
                ae.device;
    '''
}

# # Збирати результати
# report = {}
#
# for key, sql in queries.items():
#     df = pd.read_sql(sql, conn)
#     report[key] = df
#     df.to_csv(f'results_{key}.csv', index=False)
#     df.to_json(f'results_{key}.json', orient='records')
#
# conn.close()
#
# # Можна ще сформувати загальний файл:
# with pd.ExcelWriter("report.xlsx") as writer:
#     for key, df in report.items():
#         df.to_excel(writer, sheet_name=key[:30], index=False)


report = {}
for key, sql in queries.items():
    df = pd.read_sql(sql, engine)   # ← тепер через engine
    report[key] = df
    # df.to_csv(f'results_{key}.csv', index=False)
    # df.to_json(f'results_{key}.json', orient='records')

with pd.ExcelWriter("/app/output/report.xlsx") as writer:
    for key, df in report.items():
        df.to_excel(writer, sheet_name=key[:30], index=False)
