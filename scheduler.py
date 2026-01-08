
import schedule
import time
import subprocess

def run_pipeline():
    print("Running daily data pipeline...")
    subprocess.run(["python", "src/collect_data.py"])
    subprocess.run(["python", "src/processed_data.py"])
    subprocess.run(["python", "src/forecast.py"])
    print("Pipeline finished!")

# Schedule: Every day at 9 am
schedule.every().day.at("09:00").do(run_pipeline)

while True:
    schedule.run_pending()
    time.sleep(60)  