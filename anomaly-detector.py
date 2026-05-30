from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as spark_sum, when, avg, round as spark_round, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import smtplib
import os
import uuid
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

spark = SparkSession.builder.appName("EnterpriseAnomalyDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema Definition
product_schema = StructType([
    StructField("category", StringType(), True),
    StructField("cart_amount_usd", DoubleType(), True)
])

json_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("server_node", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("payment_gateway", StringType(), True),
    StructField("status", StringType(), True),
    StructField("response_time_ms", DoubleType(), True),
    StructField("product_context", product_schema, True)
])

df_kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "100.84.105.9:9092").option("subscribe", "ecommerce-events").load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data")).select("data.*") \
    .withColumn("cart_amount_usd", col("product_context.cart_amount_usd")) \
    .repartition(16) 

# --- MULTI-DIMENSIONAL AGGREGATION ---
df_aggregated = df_parsed \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "30 seconds"), 
        col("server_node"),
        col("event_type"),
        col("payment_gateway") # Added to track specific gateway failures
    ) \
    .agg(
        spark_sum(when(col("status") == "failed", 1).otherwise(0)).alias("fail_count"),
        spark_sum(when(col("status") == "success", 1).otherwise(0)).alias("success_count"),
        spark_round(spark_sum(when(col("status") == "failed", col("cart_amount_usd")).otherwise(0.0)), 2).alias("revenue_at_risk_usd"),
        spark_round(avg(col("response_time_ms")), 2).alias("avg_latency_ms"),
        spark_round(stddev(col("response_time_ms")), 2).alias("latency_stddev") 
    ).fillna(0.0)

# --- ENTERPRISE ALERT RULES ---
df_alerts = df_aggregated \
    .withColumn("total_events", col("fail_count") + col("success_count")) \
    .withColumn("failure_rate_pct", spark_round((col("fail_count") / col("total_events")) * 100, 2)) \
    .withColumn("ANOMALY_ALERT", 
        when((col("event_type") == "payment") & (col("revenue_at_risk_usd") > 5000), "CRITICAL: Revenue Loss Threshold Exceeded")
        .when((col("event_type") == "payment") & (col("failure_rate_pct") > 20), "HIGH: Gateway Experiencing Elevated Failure Rates")
        .when((col("event_type") == "checkout_start") & (col("failure_rate_pct") > 30), "HIGH: Checkout Funnel Disruption")
        .when((col("event_type") == "add_to_cart") & (col("fail_count") > col("success_count")), "HIGH: Add-To-Cart Functionality Degraded")
        .when((col("event_type") == "search") & (col("latency_stddev") > (col("avg_latency_ms") * 1.5)), "WARNING: Search API Performance Degradation")
        .otherwise("NORMAL")
    )

# --- DYNAMIC EMAIL DISPATCHER ---
def send_enterprise_email(row, alert_severity, incident_id):
    sender_email = os.getenv("GMAIL_SENDER")
    receiver_email = os.getenv("GMAIL_RECEIVER")
    app_password = os.getenv("GMAIL_APP_PASSWORD")

    # Dynamic Styling based on Severity
    if "CRITICAL" in alert_severity:
        color, icon = "#d9534f", "🚨"
    elif "HIGH" in alert_severity:
        color, icon = "#f0ad4e", "⚠️"
    elif "WARNING" in alert_severity:
        color, icon = "#f0e68c", "🔍"
    elif "RESOLVED" in alert_severity:
        color, icon = "#5cb85c", "✅"
    else:
        return

    msg = MIMEMultipart("alternative")
    msg['Subject'] = f"{icon} {alert_severity.split(':')[0]}: {row['Event'].upper()} on {row['Node']} - {row['Gateway']}"
    msg['From'], msg['To'] = sender_email, receiver_email

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px;">
        <div style="max-width: 600px; margin: 0 auto; background: #fff; border-top: 5px solid {color}; padding: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
          <h2 style="color: {color}; margin-top: 0;">{icon} {alert_severity}</h2>
          <p><strong>Incident ID:</strong> INC-{incident_id}</p>
          <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
          <table style="width: 100%; text-align: left;">
            <tr><td style="padding: 8px 0;"><strong>Node:</strong></td><td>{row['Node']}</td></tr>
            <tr><td style="padding: 8px 0;"><strong>Gateway:</strong></td><td>{row['Gateway']}</td></tr>
            <tr><td style="padding: 8px 0;"><strong>Event Type:</strong></td><td>{row['Event']}</td></tr>
            <tr><td style="padding: 8px 0;"><strong>Failure Rate:</strong></td><td>{row['Fail_Pct']}%</td></tr>
            <tr><td style="padding: 8px 0;"><strong>Revenue at Risk:</strong></td><td style="color: #d9534f;">${row['Rev_At_Risk']}</td></tr>
            <tr><td style="padding: 8px 0;"><strong>Avg Latency:</strong></td><td>{row['Avg_Lat_ms']} ms</td></tr>
          </table>
        </div>
      </body>
    </html>
    """
    msg.attach(MIMEText(html_body, 'html'))
    
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print(f"--> [{alert_severity}] Email sent for INC-{incident_id} <--")
    except Exception as e:
        print(f"Failed to send email: {e}")

# --- INCIDENT STATE TRACKER ---
# Dictionary to remember failing systems across micro-batches
active_incidents = {}

def process_batch(df, batch_id):
    # 1. SAVE THE DATA PATH (Added this back in)
    if df.count() > 0:
        df.coalesce(1).write \
          .mode("append") \
          .json(f"hdfs://100.84.105.9:9000/user/waqar/ecommerce_alerts/raw_events/batch_{batch_id}")

    # 2. EVALUATE ALERTS & SEND EMAILS
    records = df.collect()
    
    for row in records:
        node = row['Node']
        event = row['Event']
        gateway = row['Gateway']
        status_message = row['System_Status']
        
        # Unique identifier for the specific system component
        system_key = f"{node}-{event}-{gateway}" 

        if status_message != "NORMAL":
            # New or Ongoing Incident
            if system_key not in active_incidents:
                new_incident_id = str(uuid.uuid4())[:8].upper()
                active_incidents[system_key] = new_incident_id
                send_enterprise_email(row.asDict(), status_message, new_incident_id)
        else:
            # System Recovered
            if system_key in active_incidents:
                resolved_incident_id = active_incidents.pop(system_key)
                send_enterprise_email(row.asDict(), "RESOLVED: System Operations Restored", resolved_incident_id)
query_hdfs = df_alerts \
    .select(
        col("window.start").cast("string").alias("Time_Window"), 
        col("server_node").alias("Node"), 
        col("event_type").alias("Event"), 
        col("payment_gateway").alias("Gateway"),
        col("revenue_at_risk_usd").alias("Rev_At_Risk"),
        col("failure_rate_pct").alias("Fail_Pct"), 
        col("avg_latency_ms").alias("Avg_Lat_ms"), 
        col("latency_stddev").alias("Volatility"), 
        col("ANOMALY_ALERT").alias("System_Status")
    ) \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "hdfs://100.84.105.9:9000/user/waqar/ecommerce_alerts/checkpoint_01") \
    .start()

query_hdfs.awaitTermination()