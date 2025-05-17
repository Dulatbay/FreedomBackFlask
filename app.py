from flask import Flask, Response
from flask_caching import Cache
import pandas as pd
from sqlalchemy import create_engine
import json
import os

app = Flask(__name__)

# 🔌 Настройка Redis-кэша
app.config['CACHE_TYPE'] = 'RedisCache'
app.config['CACHE_REDIS_HOST'] = os.getenv('REDIS_HOST', '18.157.112.83')
app.config['CACHE_REDIS_PORT'] = 6379
app.config['CACHE_REDIS_DB'] = 0
app.config['CACHE_DEFAULT_TIMEOUT'] = 3600  # 1 час

# ✅ Создание объекта Cache
cache = Cache(app)

db_user = os.getenv("POSTGRES_USER", "postgres_user")
db_pass = os.getenv("POSTGRES_PASSWORD", "postgres_password")
db_host = os.getenv("POSTGRES_HOST", "18.157.112.83")
db_port = os.getenv("POSTGRES_PORT", 5432)
db_name = os.getenv("POSTGRES_DB", "postgres_db")

connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)

from flask import request

@app.route('/api/channels', methods=['GET'])
def get_channel_distribution():
    req_type = request.args.get('type', 'users_count')
    cache_key = f'channels_data_{req_type}'

    @cache.cached(timeout=3600, key_prefix=cache_key)
    def load_data():
        try:
            if req_type == 'commission_sum':
                query = """
                    SELECT a."Канал привлечения" as channel, SUM(c.comission) as total_commission
                    FROM freedom_broker.accounts a
                    JOIN freedom_broker.comissions c ON a.login = c.login
                    GROUP BY a."Канал привлечения"
                    ORDER BY total_commission DESC
                """
                df = pd.read_sql(query, engine)
                df.columns = ['channel', 'total_commission']
                result = df.to_dict(orient='records')

            elif req_type == 'lifetime':
                query = """
                    SELECT
                      a."Канал привлечения" as channel,
                      a.login,
                      MIN(COALESCE(c.date_trunc, t.date_last_activity, e.date_last_conv, i.date_last_inouts)) as first_activity,
                      MAX(COALESCE(c.date_trunc, t.date_last_activity, e.date_last_conv, i.date_last_inouts)) as last_activity
                    FROM freedom_broker.accounts a
                    LEFT JOIN freedom_broker.comissions c ON a.login = c.login
                    LEFT JOIN freedom_broker.trades t ON a.login = t.login
                    LEFT JOIN freedom_broker.exchange e ON a.client_id = e.client_id
                    LEFT JOIN freedom_broker.inouts i ON a.client_id = i.client_id
                    GROUP BY a."Канал привлечения", a.login
                """
                df = pd.read_sql(query, engine)
                df['first_activity'] = pd.to_datetime(df['first_activity'])
                df['last_activity'] = pd.to_datetime(df['last_activity'])
                df['lifetime_days'] = (df['last_activity'] - df['first_activity']).dt.days.clip(lower=0)

                lifetime_df = df.groupby('channel')['lifetime_days'].mean().reset_index()
                lifetime_df.columns = ['channel', 'avg_lifetime_days']
                result = lifetime_df.to_dict(orient='records')

            else:
                query = "SELECT \"Канал привлечения\" as channel, COUNT(*) as user_count FROM freedom_broker.accounts GROUP BY channel ORDER BY user_count DESC"
                df = pd.read_sql(query, engine)
                df.columns = ['channel', 'user_count']
                result = df.to_dict(orient='records')

            return result

        except Exception as e:
            return {"error": str(e)}

    data = load_data()
    if isinstance(data, dict) and "error" in data:
        return Response(json.dumps(data, ensure_ascii=False), status=500, content_type="application/json; charset=utf-8")

    return Response(json.dumps(data, ensure_ascii=False), content_type="application/json; charset=utf-8")

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
