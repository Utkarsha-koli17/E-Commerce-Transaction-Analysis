from flask import Flask, jsonify, render_template
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from decimal import Decimal

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'metastore',
    'user': 'bigdata',
    'password': 'bigdata123'
}

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn

def convert_value(val):
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except:
            return 0.0
    return float(val)

@app.route('/')
def overview():
    return render_template('overview.html')

@app.route('/categories')
def categories():
    return render_template('categories.html')

@app.route('/geography')
def geography():
    return render_template('geography.html')

@app.route('/payments')
def payments():
    return render_template('payments.html')

@app.route('/trends')
def trends():
    return render_template('trends.html')

@app.route('/customers')
def customers():
    return render_template('customers.html')

@app.route('/api/summary')
def api_summary():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT user_id) as unique_customers,
            ROUND(SUM(total_revenue)::numeric, 2) as total_revenue,
            ROUND(AVG(total_revenue)::numeric, 2) as avg_order_value,
            ROUND(SUM(CASE WHEN return_flag = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as return_rate,
            ROUND(AVG(customer_rating)::numeric, 2) as avg_rating,
            ROUND(SUM(CASE WHEN is_on_time_delivery THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as on_time_delivery
        FROM ecommerce_cleaned
    """)
    data = cur.fetchone()
    conn.close()
    return jsonify({
        'total_orders': int(data['total_orders']) if data['total_orders'] else 0,
        'unique_customers': int(data['unique_customers']) if data['unique_customers'] else 0,
        'total_revenue': convert_value(data['total_revenue']),
        'avg_order_value': convert_value(data['avg_order_value']),
        'return_rate': convert_value(data['return_rate']),
        'avg_rating': convert_value(data['avg_rating']),
        'on_time_delivery': convert_value(data['on_time_delivery'])
    })

@app.route('/api/category_revenue')
def api_category_revenue():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT product_category,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            COUNT(*) as orders,
            ROUND(AVG(customer_rating)::numeric, 2) as avg_rating,
            ROUND(SUM(CASE WHEN return_flag = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as return_rate
        FROM ecommerce_cleaned
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY revenue DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify([{
        'product_category': row['product_category'],
        'revenue': convert_value(row['revenue']),
        'orders': int(row['orders']) if row['orders'] else 0,
        'avg_rating': convert_value(row['avg_rating']),
        'return_rate': convert_value(row['return_rate'])
    } for row in data])

@app.route('/api/monthly_revenue')
def api_monthly_revenue():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT transaction_year, transaction_month,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            COUNT(*) as orders
        FROM ecommerce_cleaned
        WHERE transaction_year IS NOT NULL
        GROUP BY transaction_year, transaction_month
        ORDER BY transaction_year, transaction_month
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify([{
        'transaction_year': int(row['transaction_year']) if row['transaction_year'] else 0,
        'transaction_month': int(row['transaction_month']) if row['transaction_month'] else 0,
        'revenue': convert_value(row['revenue']),
        'orders': int(row['orders']) if row['orders'] else 0
    } for row in data])

@app.route('/api/category_subcategory')
def api_category_subcategory():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT product_category, sub_category,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            COUNT(*) as orders
        FROM ecommerce_cleaned
        WHERE product_category IS NOT NULL AND sub_category IS NOT NULL
        GROUP BY product_category, sub_category
        ORDER BY product_category, revenue DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/category_price_distribution')
def api_category_price_distribution():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT product_category,
            ROUND(AVG(product_price)::numeric, 2) as avg_price,
            ROUND(AVG(discount_percent)::numeric, 2) as avg_discount
        FROM ecommerce_cleaned
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY avg_price DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/return_reasons')
def api_return_reasons():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT product_category, return_reason, COUNT(*) as count
        FROM ecommerce_cleaned
        WHERE return_flag = 1 AND return_reason IS NOT NULL
        GROUP BY product_category, return_reason
        ORDER BY product_category, count DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/state_performance')
def api_state_performance():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT buyer_state,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            COUNT(*) as orders,
            COUNT(DISTINCT user_id) as customers,
            ROUND(AVG(total_revenue)::numeric, 2) as aov,
            ROUND(SUM(CASE WHEN return_flag = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as return_rate,
            ROUND(COALESCE(AVG(delivery_days), 0)::numeric, 2) as avg_delivery_days,
            ROUND(SUM(CASE WHEN is_on_time_delivery THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as on_time_pct
        FROM ecommerce_cleaned
        WHERE buyer_state IS NOT NULL
        GROUP BY buyer_state
        ORDER BY revenue DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify([{
        'buyer_state': row['buyer_state'],
        'revenue': convert_value(row['revenue']),
        'orders': int(row['orders']) if row['orders'] else 0,
        'customers': int(row['customers']) if row['customers'] else 0,
        'aov': convert_value(row['aov']),
        'return_rate': convert_value(row['return_rate']),
        'avg_delivery_days': convert_value(row['avg_delivery_days']),
        'on_time_pct': convert_value(row['on_time_pct'])
    } for row in data])

@app.route('/api/interstate_flow')
def api_interstate_flow():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT buyer_state, seller_state,
            COUNT(*) as transactions,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            ROUND(COALESCE(AVG(delivery_days), 0)::numeric, 2) as avg_delivery_days
        FROM ecommerce_cleaned
        WHERE buyer_state IS NOT NULL AND seller_state IS NOT NULL
        GROUP BY buyer_state, seller_state
        ORDER BY revenue DESC
        LIMIT 20
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify([{
        'buyer_state': row['buyer_state'],
        'seller_state': row['seller_state'],
        'transactions': int(row['transactions']) if row['transactions'] else 0,
        'revenue': convert_value(row['revenue']),
        'avg_delivery_days': convert_value(row['avg_delivery_days'])
    } for row in data])

@app.route('/api/payment_distribution')
def api_payment_distribution():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT payment_mode,
            COUNT(*) as count,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            ROUND(COUNT(*)::numeric / (SELECT COUNT(*) FROM ecommerce_cleaned) * 100, 2) as percentage,
            ROUND(SUM(CASE WHEN return_flag = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) as return_rate
        FROM ecommerce_cleaned
        WHERE payment_mode IS NOT NULL
        GROUP BY payment_mode
        ORDER BY count DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify([{
        'payment_mode': row['payment_mode'],
        'count': int(row['count']) if row['count'] else 0,
        'revenue': convert_value(row['revenue']),
        'percentage': convert_value(row['percentage']),
        'return_rate': convert_value(row['return_rate'])
    } for row in data])

@app.route('/api/payment_by_state')
def api_payment_by_state():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT buyer_state, payment_mode, COUNT(*) as count
        FROM ecommerce_cleaned
        WHERE buyer_state IS NOT NULL AND payment_mode IS NOT NULL
        GROUP BY buyer_state, payment_mode
        ORDER BY buyer_state, count DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/payment_by_segment')
def api_payment_by_segment():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT customer_segment, payment_mode, COUNT(*) as count
        FROM ecommerce_cleaned
        WHERE customer_segment IS NOT NULL AND payment_mode IS NOT NULL
        GROUP BY customer_segment, payment_mode
        ORDER BY customer_segment, count DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/yearly_comparison')
def api_yearly_comparison():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT transaction_year,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            COUNT(*) as orders
        FROM ecommerce_cleaned
        WHERE transaction_year IS NOT NULL
        GROUP BY transaction_year
        ORDER BY transaction_year
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/day_of_week')
def api_day_of_week():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT day_of_week,
            COUNT(*) as orders,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue
        FROM ecommerce_cleaned
        WHERE day_of_week IS NOT NULL
        GROUP BY day_of_week
        ORDER BY day_of_week
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/category_trends')
def api_category_trends():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT transaction_year, transaction_month, product_category,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue
        FROM ecommerce_cleaned
        WHERE transaction_year IS NOT NULL AND product_category IS NOT NULL
        GROUP BY transaction_year, transaction_month, product_category
        ORDER BY transaction_year, transaction_month
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/segment_analysis')
def api_segment_analysis():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT customer_segment,
            COUNT(*) as orders,
            COUNT(DISTINCT user_id) as customers,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            ROUND(AVG(total_revenue)::numeric, 2) as aov
        FROM ecommerce_cleaned
        WHERE customer_segment IS NOT NULL
        GROUP BY customer_segment
        ORDER BY revenue DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/device_usage')
def api_device_usage():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT device_type, COUNT(*) as count
        FROM ecommerce_cleaned
        WHERE device_type IS NOT NULL
        GROUP BY device_type
        ORDER BY count DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/rating_distribution')
def api_rating_distribution():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            FLOOR(customer_rating)::int as rating,
            COUNT(*) as count
        FROM ecommerce_cleaned
        WHERE customer_rating IS NOT NULL
        GROUP BY FLOOR(customer_rating)
        ORDER BY rating
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

@app.route('/api/loyalty_analysis')
def api_loyalty_analysis():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT loyalty_member,
            COUNT(*) as orders,
            ROUND(SUM(total_revenue)::numeric, 2) as revenue,
            ROUND(AVG(total_revenue)::numeric, 2) as aov
        FROM ecommerce_cleaned
        GROUP BY loyalty_member
        ORDER BY revenue DESC
    """)
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify(data)

if __name__ == '__main__':
    print("🚀 Starting Multi-Page Dashboard on http://localhost:5000")
    print("📊 Pages: Overview | Categories | Geography | Payments | Trends | Customers")
    app.run(host='0.0.0.0', port=5000, debug=True)
