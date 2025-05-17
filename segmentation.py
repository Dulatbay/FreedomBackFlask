import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.cluster import KMeans
import seaborn as sns
from sqlalchemy import create_engine
from urllib.parse import quote_plus

DB_USER = 'postgres_user'
DB_PASS = 'postgres_password'
DB_HOST = '18.157.112.83'
DB_PORT = '8082'
DB_NAME = 'postgres_db'
timeout = 10


DB_PASS_ENCODED = quote_plus(DB_PASS)
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

sql_query = """
WITH accounts_map AS (
  SELECT login, client_id FROM freedom_broker.accounts
),

commission_agg AS (
  SELECT
    c.login,
    COALESCE(SUM(c.comission), 0) AS total_commission
  FROM freedom_broker.comissions c
  GROUP BY c.login
),

last_activity AS (
  SELECT
    am.client_id,
    GREATEST(
      COALESCE(MAX(t.date_last_activity), '1900-01-01'),
      COALESCE(MAX(e.date_last_conv), '1900-01-01'),
      COALESCE(MAX(io.date_last_inouts), '1900-01-01')
    ) AS last_activity_date
  FROM accounts_map am
  LEFT JOIN freedom_broker.trades t ON am.login = t.login
  LEFT JOIN freedom_broker.exchange e ON am.client_id = e.client_id
  LEFT JOIN freedom_broker.inouts io ON am.client_id = io.client_id
  GROUP BY am.client_id
),

frequency AS (
  SELECT
    am.client_id,
    COALESCE(SUM(t.cnt_trades), 0) AS total_trades,
    COALESCE(SUM(io.cnt_inouts), 0) AS total_inouts,
    COALESCE(SUM(t.cnt_trades), 0) + COALESCE(SUM(io.cnt_inouts), 0) AS total_frequency
  FROM accounts_map am
  LEFT JOIN freedom_broker.trades t ON am.login = t.login
  LEFT JOIN freedom_broker.inouts io ON am.client_id = io.client_id
  GROUP BY am.client_id
)

SELECT
  a.client_id,
  la.last_activity_date,
  f.total_frequency,
  b.balance,
  ins.sum_ins,
  co.total_commission,
  f.total_trades
FROM accounts_map a
LEFT JOIN last_activity la ON a.client_id = la.client_id
LEFT JOIN frequency f ON a.client_id = f.client_id
LEFT JOIN freedom_broker.balance b ON a.client_id = b.client_id
LEFT JOIN freedom_broker.ins ins ON a.client_id = ins.client_id
LEFT JOIN commission_agg co ON a.login = co.login
;
"""

def get_data():
    df = pd.read_sql(sql_query, engine)

    df['last_activity_date'] = pd.to_datetime(df['last_activity_date'])
    today = pd.Timestamp(datetime.utcnow().date())
    df['recency'] = (today - df['last_activity_date']).dt.days

    df.fillna({
        'total_frequency': 0,
        'balance': 0,
        'sum_ins': 0,
        'total_commission': 0,
        'total_trades': 0,
        'recency': df['recency'].max()
    }, inplace=True)

    multiplier = 1 + 0.05 * df['total_trades']
    df['monetary_value'] = df['sum_ins'] * multiplier + df['total_commission']

    df['total_frequency'] = df['total_frequency'].clip(lower=0)
    df['monetary_value'] = df['monetary_value'].clip(lower=0)

    half_life_days = 30
    df['recency_score'] = np.exp(-df['recency'] / half_life_days)

    df['frequency_score'] = np.log1p(df['total_frequency'])
    df['frequency_score'] = (df['frequency_score'] - df['frequency_score'].min()) / (df['frequency_score'].max() - df['frequency_score'].min())

    df['monetary_score'] = np.log1p(df['monetary_value'])
    df['monetary_score'] = (df['monetary_score'] - df['monetary_score'].min()) / (df['monetary_score'].max() - df['monetary_score'].min())

    df = df.dropna(subset=['recency_score', 'frequency_score', 'monetary_score'])

    corrs = df[['recency_score', 'frequency_score', 'monetary_score', 'total_commission']].corr()
    weights_raw = corrs.loc[['recency_score', 'frequency_score', 'monetary_score'], 'total_commission'].abs()
    weights = weights_raw / weights_raw.sum()
    print("Normalized weights based on correlation:\n", weights)

    # Composite RFM score
    df['rfm_score'] = (
        df['recency_score'] * weights['recency_score'] +
        df['frequency_score'] * weights['frequency_score'] +
        df['monetary_score'] * weights['monetary_score']
    )

    # KMeans clustering
    kmeans = KMeans(n_clusters=5, random_state=42)
    df['cluster'] = kmeans.fit_predict(df[['recency_score', 'frequency_score', 'monetary_score']])

    print("Cluster counts:")
    print(df['cluster'].value_counts())


    cluster_counts = df['cluster'].value_counts().sort_index()
    cluster_order = sorted(df['cluster'].unique())
    commission_by_cluster = df.groupby('cluster')['total_commission'].sum().reindex(cluster_order)

    # Создаем DataFrame с информацией о кластерах
    cluster_summary = pd.DataFrame({
        'cluster_number': cluster_order,
        'cluster_count': cluster_counts,
        'total_commission': commission_by_cluster
    }).reset_index(drop=True)

    return cluster_summary



