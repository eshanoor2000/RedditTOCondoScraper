import os
import json
import time
import sys
import requests
import praw
import pymongo
from pymongo.errors import BulkWriteError
import logging
from datetime import datetime, date, timedelta
import re
from calendar import month_name
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from rapidfuzz import fuzz
import io
import PyPDF2
from bs4 import BeautifulSoup
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
load_dotenv()

# Logging Configuration
def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# Initialize logging
configure_logging()

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

SCRAPED_COUNT = {
    "reddit": 0,
    "tocondo": 0
}

ONTARIO_TERMS = {
    "ontario", 
    "toronto",
    "canada",
    "ottawa",
    "mississauga",
    "brampton",
    "hamilton",
    "london",
    "markham",
    "vaughan",
    "kitchener",
    "windsor",
    "richmond hill",
    "oakville",
    "burlington",
    "oshawa",
    "barrie",
    "st. catharines",
    "cambridge",
    "kingston",
    "whitby",
    "guelph",
    "thunder Bay",
    "waterloo",
    "brantford",
    "niagara Falls"
}

STANDARD_KEYWORDS = [
    "condominium authority of ontario",
    "condo authority",
    "condo authority of ontario",
    "ontario condominium act",
    "condominium act",
    "Condominium Authority of Ontario",
    "Condo Authority of Ontario",
    "CAO Ontario",
    "Condominium Authority",
    "Condo Authority",
    "Condo Tribunal Ontario",
    "Condominium Tribunal Ontario",
    "Condo Authority Tribunal",
    "Condominium Authority Tribunal",
    "CAO Tribunal",
    "Condominium Act Ontario",
    "Ontario Condominium Act",
    "Ontario condo laws",
    "Condo governance Ontario",
    "Ontario condo regulations",
    "Condo regulatory compliance Ontario",
    "Condo law reform Ontario",
    "Condo bylaws Ontario",
    "Condo policy changes Ontario",
    "Condo tribunal cases Ontario",
    "Condo tribunal decisions Ontario",
    "Condo tribunal rulings Ontario",
    "CAO tribunal rulings",
    "Condominium tribunal hearings",
    "Condo tribunal evidence submission",
    "Condo tribunal appeals Ontario",
    "Ontario condo tribunal process",
    "Condo tribunal enforcement Ontario",
    "Condo tribunal complaint process",
    "Condo legal disputes Ontario",
    "Condo legal challenges Ontario",
    "Condo tribunal case law Ontario",
    "Condo board disputes Ontario",
    "Condo board complaints Ontario",
    "Condo board corruption Ontario",
    "Ontario condo board governance",
    "Condo board misconduct Ontario",
    "Condo board election fraud Ontario",
    "Condo board mismanagement Ontario",
    "Condo board conflicts Ontario",
    "Condo board transparency issues Ontario",
    "Condo board legal responsibilities Ontario",
    "Condo board regulations Ontario",
    "Condo owner rights Ontario",
    "Condo owner disputes Ontario",
    "Condo owners legal issues Ontario",
    "Condo dispute resolution Ontario",
    "Condo resident rights Ontario",
    "Condo complaint process Ontario",
    "How to file condo complaint Ontario",
    "Condo tenant rights Ontario",
    "Condo transparency issues Ontario",
    "Ontario condo act violations",
    "Condo maintenance fees Ontario",
    "Condo fee increases Ontario",
    "Unfair condo fees Ontario",
    "Condo reserve fund Ontario",
    "Condo financial mismanagement Ontario",
    "Condo special assessments Ontario",
    "Condo financial fraud Ontario",
    "Condo board financial transparency Ontario",
    "Condo developer fraud Ontario",
    "Condo management fraud Ontario",
    "Condo property management Ontario",
    "Condo management disputes Ontario",
    "Condo management complaints Ontario",
    "Condo management corruption Ontario",
    "Ontario condo rental rules",
    "Ontario condo tenant disputes",
    "Condo property maintenance Ontario",
    "Condo security fraud Ontario",
    "Condo contract violations Ontario",
    "CAO vs Landlord and Tenant Board",
    "FSRA condo insurance Ontario",
    "Ontario Securities Commission condo fraud",
    "Ontario Human Rights Tribunal condo cases",
    "Ontario Building Code condo regulations",
    "Ontario Ministry of Municipal Affairs and Housing condo rules",
    "Ontario condo tribunal vs landlord tenant board"
]

TOCONDO_KEYWORDS = STANDARD_KEYWORDS + ["CAT", "CAO"]

REDDIT_KEYWORDS = STANDARD_KEYWORDS + [
    "CAO",
    "CAO tribunal",
    "CAO condo",
    "CAO Ontario",
    "CAO tribunal Ontario",
    "CAO complaint",
    "CAO decision",
    "condo issues Ontario",
    "condo complaint Ontario",
    "condo fees Ontario",
    "condo fraud Ontario",
    "condo nightmare Ontario",
    "condo problems Ontario",
    "condo scam Ontario",
    "condo rules Ontario",
    "condo restrictions Ontario",
    "condo living Ontario",
    "condo association Ontario",
    "property manager issues Ontario",
    "condo management Ontario",
    "condo disputes Ontario",
    "condo regulations Ontario",
    "Condo Authority ON",
    "CAO condo issues",
    "Condo Auth Ontario",
    "Condo tribunal Ontario",
    "Condo board corruption Ontario",
    "CAT Ontario"
]

SUBREDDITS = [
    "TorontoRealEstate", "PersonalFinanceCanada", "CanadaHousing", "CanadaHousing2",
    "askTO", "OntarioLandlord", "Hamilton", "Landlord", "landlords", "LawCanada",
    "legaladvicecanada", "londonontario", "mississauga", "MississaugaRealEstate",
    "ontario", "ottawa", "toronto", "PersonalFinance", "REBubble", "Vaughan", "waterloo"
]

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "brand_monitoring")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_articles")

def get_collection(collection_name):
    if not hasattr(get_collection, "client"):
        get_collection.client = pymongo.MongoClient(MONGO_URI)
    return get_collection.client[MONGO_DB][collection_name]

def validate_db_connection():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        logging.info("MongoDB connection verified.")
    except Exception as e:
        logging.error(f"MongoDB connection failed: {e}")
        sys.exit(1)

def is_within_scrape_window(timestamp_str):
    try:
        timestamp = datetime.fromisoformat(timestamp_str)
        today = datetime.utcnow()
        
        if today.day < 6:
            end_date = datetime(today.year, today.month, 6)
            if today.month == 1:
                start_date = datetime(today.year - 1, 12, 6)
            else:
                start_date = datetime(today.year, today.month - 1, 6)
        else:
            start_date = datetime(today.year, today.month, 6)
            if today.month == 12:
                end_date = datetime(today.year + 1, 1, 6)
            else:
                end_date = datetime(today.year, today.month + 1, 6)
                
        return start_date <= timestamp < end_date
    except ValueError:
        return False

def is_relevant_location(text):
    text_lower = text.lower()
    return any(term.lower() in text_lower for term in ONTARIO_TERMS)

def get_valid_date(date_str):
    if not date_str:
        return datetime.utcnow().isoformat()
    
    date_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%d %b %Y %H:%M:%S %Z",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    
    try:
        if date_str.isdigit():
            return datetime.utcfromtimestamp(int(date_str)).isoformat()
    except:
        pass
    
    return datetime.utcnow().isoformat()

def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg["From"] = os.getenv("EMAIL_SENDER")
        msg["To"] = os.getenv("EMAIL_RECEIVER")
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(os.getenv("SMTP_SERVER"), int(os.getenv("SMTP_PORT"))) as server:
            server.starttls()
            server.login(os.getenv("EMAIL_SENDER"), os.getenv("EMAIL_PASSWORD"))
            server.sendmail(os.getenv("EMAIL_SENDER"), os.getenv("EMAIL_RECEIVER"), msg.as_string())

        logging.info("Alert email sent.")
    except Exception as e:
        logging.error(f"Failed to send email alert: {e}")

def get_matched_keywords(text, keywords, fuzzy_threshold=90):
    matched_keywords = []
    text_lower = text.lower()
    
    for keyword in keywords:
        keyword_lower = keyword.lower()

        if keyword_lower in text_lower:
            matched_keywords.append(keyword)
            continue

        if any(fuzz.partial_ratio(keyword_lower, sentence.lower()) >= fuzzy_threshold
               for sentence in text_lower.split(".") if sentence):
            matched_keywords.append(keyword)

    return matched_keywords

def extract_pdf_publish_date(title):
    try:
        match = re.search(rf"({'|'.join(month_name[1:])})[_\s]?(\d{{4}})", title, re.IGNORECASE)
        if match:
            month_str = match.group(1).capitalize()
            year = int(match.group(2))
            month_num = list(month_name).index(month_str)
            return datetime(year, month_num, 1)
    except Exception as e:
        logging.warning(f"Failed to extract publish date from title: {title} ({e})")

    return datetime.utcnow()

def save_scraped_data(source, data):
    if not data:
        logging.info(f"No {source} articles to save.")
        return 0

    collection = get_collection(RAW_COLLECTION)

    for item in data:
        item.update({
            "source": source,
            "scraped_date": datetime.utcnow().isoformat(),
            "processing_status": "pending",
            "processed_at": None
        })

    try:
        result = collection.insert_many(data, ordered=False)
        count = len(result.inserted_ids)
        SCRAPED_COUNT[source] += count
        logging.info(f"Saved {count} {source} articles to {RAW_COLLECTION}")
        return count
    except BulkWriteError as e:
        inserted = len(e.details['nInserted'])
        SCRAPED_COUNT[source] += inserted
        logging.info(f"Saved {inserted} {source} articles to {RAW_COLLECTION} (some duplicates skipped)")
        return inserted

def fetch_reddit_posts():
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT")
    )
    
    articles = []
    try:
        for subreddit_name in SUBREDDITS:
            try:
                sub = reddit.subreddit(subreddit_name)
                for post in sub.new(limit=50):
                    post_time = datetime.utcfromtimestamp(post.created_utc).isoformat()
                    if not is_within_scrape_window(post_time):
                        continue

                    text = f"{post.title} {post.selftext}"
                    if not (is_relevant_location(text) and get_matched_keywords(text, REDDIT_KEYWORDS)):
                        continue

                    articles.append({
                        "title": post.title,
                        "link": f"https://reddit.com{post.permalink}",
                        "published_date": post_time,
                        "tags": get_matched_keywords(text, REDDIT_KEYWORDS),
                        "subreddit": subreddit_name,
                        "upvotes": post.score,
                        "comments": post.num_comments,
                        "content": post.selftext
                    })

            except Exception as e:
                logging.error(f"Error in r/{subreddit_name}: {str(e)}")

        count = save_scraped_data("reddit", articles)
        return count
    except Exception as e:
        logging.error(f"Fatal Reddit scraping error: {str(e)}")
        return 0

def fetch_tocondo_pdfs():
    articles = []
    try:
        response = requests.get("https://tocondonews.com/", timeout=15)
        soup = BeautifulSoup(response.text, "html.parser")
        pdf_links = [link["href"] for link in soup.find_all("a", href=True) if link["href"].endswith(".pdf")]

        for link in pdf_links:
            try:
                title = link.split("/")[-1]
                content = process_pdf(link)

                if not content.strip():
                    logging.warning(f"Skipping blank PDF: {link}")
                    continue

                published_dt = extract_pdf_publish_date(title)
                published_date = published_dt.isoformat()

                if not is_within_scrape_window(published_date):
                    continue

                if not (is_relevant_location(title + content) and get_matched_keywords(title + content, TOCONDO_KEYWORDS)):
                    continue

                articles.append({
                    "title": title,
                    "link": link,
                    "content": content,
                    "published_date": published_date,
                    "tags": get_matched_keywords(title + content, TOCONDO_KEYWORDS)
                })

            except Exception as e:
                logging.warning(f"Error processing PDF {link}: {e}")

        count = save_scraped_data("tocondo", articles)
        return count
    except Exception as e:
        logging.error(f"TOCondo scraping failed: {str(e)}")
        return 0

def process_pdf(link):
    """Process PDF with memory-efficient streaming"""
    try:
        with requests.get(link, stream=True, timeout=10) as r:
            r.raise_for_status()
            with io.BytesIO() as pdf_file:
                for chunk in r.iter_content(chunk_size=8192):
                    pdf_file.write(chunk)
                pdf_file.seek(0)
                reader = PyPDF2.PdfReader(pdf_file)
                content = "\n".join(page.extract_text() for page in reader.pages[:3] if page.extract_text())
                return content
    except Exception as e:
        logging.warning(f"PDF processing failed for {link}: {e}")
        return ""

def run_reddit_tocondo_scrapers():
    reddit_ok = fetch_reddit_posts()
    tocondo_ok = fetch_tocondo_pdfs()

    total = SCRAPED_COUNT["reddit"] + SCRAPED_COUNT["tocondo"]
    status = "SUCCESS" if reddit_ok or tocondo_ok else "FAILURE"

    subject = f"[Scraper {status}] Reddit: {SCRAPED_COUNT['reddit']} | TOCondo: {SCRAPED_COUNT['tocondo']}"
    body = f"""Reddit/TOCondo Scraping Run Completed

Reddit Articles Scraped: {SCRAPED_COUNT['reddit']}
TOCondo PDFs Scraped: {SCRAPED_COUNT['tocondo']}

Overall Status: {status}
Time: {datetime.utcnow().isoformat()} UTC
"""
    send_email(subject, body)
    return reddit_ok or tocondo_ok

# Main Function
if __name__ == "__main__":
    configure_logging()
    validate_db_connection()

    logging.info("Starting GitHub-scheduled Reddit/TOCondo scraper job...")
    success = run_reddit_tocondo_scrapers()

    if success:
        logging.info("Job completed successfully.")
    else:
        logging.error("Job failed.")
