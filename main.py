from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import requests
from datetime import datetime, timedelta

srvc_acct = json.loads(os.getenv("BIGQUERY_SERVICE_ACCT"))
credentials = service_account.Credentials.from_service_account_info(srvc_acct)

client = bigquery.Client(
    credentials=credentials, project="hot-or-not-feed-intelligence"
)

WEBHOOK_URL = os.getenv("GOOGLE_CHAT_WEBHOOK_URL")

REQUIRED_EVENTS = [
    "video_viewed",
    "video_duration_watched",
    "login_join_overlay_viewed",
    "video_upload_initiated",
    "refer",
    "login_cta",
    "login_method_selected",
    "login_successful",
    "refer_share_link",
    "logout_clicked",
    "logout_confirmation",
    "profile_view_video",
    "video_upload_successful",
    "video_upload_upload_button_clicked",
    "video_upload_video_selected",
    "like_video",
    "video_upload_unsuccessful",
    "share_video",
]


def send_text_card(title: str, subtitle: str, para1: str, para2: str):
    header = {"title": title, "subtitle": subtitle}
    widget1 = {"textParagraph": {"text": para1}}
    widget2 = {"textParagraph": {"text": para2}}
    cards = [
        {
            "header": header,
            "sections": [{"widgets": [widget1]}, {"widgets": [widget2]}],
        },
    ]
    requests.post(WEBHOOK_URL, json={"cards": cards})


if __name__ == "__main__":
    query = "SELECT event, COUNT(*) as count \
            FROM analytics_335143420.test_events_analytics \
            WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND CURRENT_TIMESTAMP() \
            GROUP BY event;"
    query_job = client.query(query)
    results = query_job.result()
    events_list = []
    events_str_list = []
    for row in results:
        # print(dict(row))
        events_list.append(dict(row)["event"])
        events_str_list.append(f"{dict(row)['event']} ({dict(row)['count']})")

    not_present_events = list(set(REQUIRED_EVENTS) - set(events_list))

    events_str_list_str = "\n".join(events_str_list)
    now = datetime.now()
    pretty_now = now.strftime("%Y-%m-%d %H:%M:%S")
    one_day_ago = now - timedelta(days=1)
    pretty_one_day_ago = one_day_ago.strftime("%Y-%m-%d %H:%M:%S")
    PARA1 = f"Present -\n {events_str_list_str}"
    PARA2 = f"Not present - {', '.join(not_present_events)}"

    # send to google webhook
    send_text_card(
        "Events Check Update", f"Date {pretty_one_day_ago} - {pretty_now}", PARA1, PARA2
    )
