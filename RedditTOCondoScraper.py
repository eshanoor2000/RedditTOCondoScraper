import os
import json
import time
import sys
import requests
import praw
import pymongo
from pymongo.errors import BulkWriteError
import logging
from datetime import datetime, date, timedelta, timezone
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
import spacy
import dateutil.parser
import random
from urllib.parse import urlparse
load_dotenv()

# --- Configuration from environment variables ---
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "brand_monitoring")
RAW_COLLECTION = os.getenv("RAW_COLLECTION", "raw_articles")
PROCESSED_COLLECTION = os.getenv("PROCESSED_COLLECTION", "processed_articles")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

# User agents for rotation to avoid blocking
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

# Logging Configuration
def configure_logging():
    """Configure logging settings for the application.
    
    Sets up logging with a standard format and output to stdout.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# Initialize logging
configure_logging()

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
    "CAT Ontario",
    "Condominium Management Regulatory Authority of Ontario",
    "CMRAO",
    "condominium management regulatory authority of ontario",
    "condo management regulatory authority of ontario"
]

SUBREDDITS = [
    "TorontoRealEstate", "PersonalFinanceCanada", "CanadaHousing", "CanadaHousing2",
    "askTO", "OntarioLandlord", "Hamilton", "Landlord", "landlords", "LawCanada",
    "legaladvicecanada", "londonontario", "mississauga", "MississaugaRealEstate",
    "ontario", "ottawa", "toronto", "PersonalFinance", "REBubble", "Vaughan", "waterloo"
]

def get_collection(collection_name):
    if not hasattr(get_collection, "client"):
        get_collection.client = pymongo.MongoClient(MONGO_URI)
    return get_collection.client[MONGO_DB][collection_name]

def validate_db_connection():
    """Validate MongoDB connection.
    
    Checks if the database connection is working and raises an exception if not.
    """
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
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())

        logging.info("Alert email sent.")
    except Exception as e:
        logging.error(f"Failed to send email alert: {e}")

nlp = spacy.load("en_core_web_sm")

def lemmatize(text):
    doc = nlp(text)
    return " ".join([token.lemma_.lower() for token in doc if not token.is_punct and not token.is_space])

def normalize_datetime(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def get_matched_keywords(text, keywords, max_tags=10):
    text_lower = (text or "").lower()
    matched = [kw for kw in keywords if kw.lower() in text_lower]
    return matched[:max_tags]

def robust_fetch_url(url, max_retries=3, timeout=10):
    """Robustly fetch URL with retries and error handling."""
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Add delay between retries
            if attempt > 0:
                time.sleep(random.uniform(2, 5))
            
            response = requests.get(url, timeout=timeout, headers=headers)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Rate limited
                logging.warning(f"Rate limited on {url}, waiting longer...")
                time.sleep(random.uniform(10, 20))
                continue
            else:
                logging.warning(f"HTTP {response.status_code} for {url}")
                continue
                
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout on attempt {attempt + 1} for {url}")
            continue
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request error on attempt {attempt + 1} for {url}: {e}")
            continue
    
    return None

def extract_pdf_publish_date(title):
    """Enhanced PDF date extraction with multiple patterns."""
    try:
        # Pattern 1: Month-Year format (e.g., "May-2023-Toronto-Condo-News.pdf")
        match = re.search(rf"({'|'.join(month_name[1:])})[_\s-]?(\d{{4}})", title, re.IGNORECASE)
        if match:
            month_str = match.group(1).capitalize()
            year = int(match.group(2))
            month_num = list(month_name).index(month_str)
            dt = datetime(year, month_num, 1, tzinfo=timezone.utc)
            return dt.isoformat()
        
        # Pattern 2: YYYY-MM format
        match = re.search(r"(\d{4})[_\s-](\d{1,2})", title)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            if 1 <= month <= 12 and 2020 <= year <= 2030:  # Sanity check
                dt = datetime(year, month, 1, tzinfo=timezone.utc)
                return dt.isoformat()
        
        # Pattern 3: MM-YYYY format
        match = re.search(r"(\d{1,2})[_\s-](\d{4})", title)
        if match:
            month = int(match.group(1))
            year = int(match.group(2))
            if 1 <= month <= 12 and 2020 <= year <= 2030:  # Sanity check
                dt = datetime(year, month, 1, tzinfo=timezone.utc)
                return dt.isoformat()
        
        # Pattern 4: Just year (fallback to January)
        match = re.search(r"(\d{4})", title)
        if match:
            year = int(match.group(1))
            if 2020 <= year <= 2030:  # Sanity check
                dt = datetime(year, 1, 1, tzinfo=timezone.utc)
                return dt.isoformat()
                
    except Exception as e:
        logging.warning(f"Failed to extract publish date from title: {title} ({e})")

    return None  # Return None instead of current date to properly discard articles without dates

def safe_get_published_date(parsed_date):
    try:
        if parsed_date:
            if isinstance(parsed_date, str):
                try:
                    dt = datetime.fromisoformat(parsed_date)
                    return dt.isoformat()
                except Exception:
                    return parsed_date  # Already ISO or fallback
            elif isinstance(parsed_date, datetime):
                return parsed_date.isoformat()
        return datetime.utcnow().isoformat()
    except Exception:
        return datetime.utcnow().isoformat()

def is_within_date_range(published_date_str):
    """Check if published date is within acceptable range (not older than 30 days from run date)."""
    try:
        # Parse the published date
        pub_dt = datetime.fromisoformat(published_date_str)
        pub_dt = normalize_datetime(pub_dt)
        
        # Get current run date (today)
        run_date = datetime.utcnow().replace(tzinfo=timezone.utc)
        
        # Calculate 30 days ago from run date
        thirty_days_ago = run_date - timedelta(days=30)
        
        # Check if published date is within the last 30 days
        return thirty_days_ago <= pub_dt <= run_date
    except Exception as e:
        logging.debug(f"Error checking date range for {published_date_str}: {e}")
        return False

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

META_DATE_PRIORITY = [
    'article:published_time', 'datePublished', 'pubdate', 'publishdate', 'date', 'og:published_time'
]

def extract_published_date_from_entry(entry):
    """Extract published date from entry with robust error handling."""
    # Try all likely fields for Reddit and TOCondo
    for key in ['created_utc', 'published', 'updated', 'created', 'date']:
        if key in entry and entry[key]:
            try:
                if key == 'created_utc':
                    return datetime.utcfromtimestamp(float(entry[key])).replace(tzinfo=timezone.utc).isoformat()
                dt = dateutil.parser.parse(entry[key], fuzzy=True)
                dt = normalize_datetime(dt)
                return dt.isoformat()
            except Exception:
                continue
    
    # Try canonical/original link if present
    if 'link' in entry and entry['link']:
        try:
            response = robust_fetch_url(entry['link'], timeout=5)
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                for meta_name in META_DATE_PRIORITY:
                    meta = soup.find('meta', attrs={'property': meta_name}) or soup.find('meta', attrs={'name': meta_name})
                    if meta and meta.get('content'):
                        try:
                            dt = dateutil.parser.parse(meta['content'], fuzzy=True)
                            dt = normalize_datetime(dt)
                            return dt.isoformat()
                        except Exception:
                            continue
        except Exception as e:
            logging.debug(f"Failed to fetch canonical/original link for date extraction: {e}")
    
    return None

def extract_published_date_from_pdf_title(title):
    """Wrapper function for PDF date extraction to maintain compatibility."""
    return extract_pdf_publish_date(title)

def fetch_reddit_posts():
    """Fetch Reddit posts with robust error handling."""
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        
        articles = []
        total_processed = 0
        
        logging.info(f"Fetching Reddit posts from {len(SUBREDDITS)} subreddits")
        
        for subreddit_name in SUBREDDITS:
            try:
                logging.debug(f"Processing subreddit: r/{subreddit_name}")
                sub = reddit.subreddit(subreddit_name)
                subreddit_count = 0
                
                for post in sub.new(limit=50):
                    total_processed += 1
                    try:
                        # Check if post has required fields
                        if not hasattr(post, 'title') or not post.title:
                            continue
                        if not hasattr(post, 'permalink') or not post.permalink:
                            continue
                        if not hasattr(post, 'created_utc') or not post.created_utc:
                            continue
                        
                        # Extract and validate date
                        published_date = None
                        try:
                            published_date = datetime.utcfromtimestamp(post.created_utc).replace(tzinfo=timezone.utc).isoformat()
                        except Exception as e:
                            logging.debug(f"Failed to parse Reddit post date: {e}")
                            continue
                        
                        # Restrict to articles published between March 6, 2025 and today
                        start_date = datetime(2025, 3, 6, tzinfo=timezone.utc)
                        end_date = datetime.utcnow().replace(tzinfo=timezone.utc)
                        try:
                            pub_dt = datetime.fromisoformat(published_date)
                            pub_dt = normalize_datetime(pub_dt)
                        except Exception:
                            continue
                        if not (start_date <= pub_dt <= end_date):
                            continue
                        
                        # Check if published date is within 30 days of run date
                        if not is_within_date_range(published_date):
                            logging.debug(f"Discarding Reddit post (published more than 30 days ago): {post.title[:50]}...")
                            continue
                        
                        # Check relevance and keywords
                        text = f"{post.title} {getattr(post, 'selftext', '')}"
                        
                        if not is_relevant_location(text):
                            continue
                        
                        matched_keywords = get_matched_keywords(text, REDDIT_KEYWORDS)
                        if not matched_keywords:
                            logging.debug(f"Discarding Reddit post (no tags): {post.title[:50]}...")
                            continue
                        
                        articles.append({
                            "title": post.title,
                            "link": f"https://reddit.com{post.permalink}",
                            "published_date": published_date,
                            "scraped_date": datetime.utcnow().isoformat(),
                            "tags": matched_keywords,
                            "source": "reddit",
                            "subreddit": subreddit_name,
                            "upvotes": getattr(post, 'score', None),
                            "comments": getattr(post, 'num_comments', None),
                            "content": getattr(post, 'selftext', None) or None
                        })
                        subreddit_count += 1
                        
                    except Exception as e:
                        logging.warning(f"Error processing Reddit post in r/{subreddit_name}: {e}")
                        continue
                
                logging.debug(f"Found {subreddit_count} relevant posts in r/{subreddit_name}")

            except Exception as e:
                logging.error(f"Error accessing subreddit r/{subreddit_name}: {str(e)}")
                continue

        logging.info(f"Processed {total_processed} Reddit posts, found {len(articles)} relevant articles")
        count = save_scraped_data("reddit", articles)
        return count
        
    except Exception as e:
        logging.error(f"Fatal Reddit scraping error: {str(e)}")
        return 0

def fetch_tocondo_pdfs():
    """Fetch TOCondo PDFs with robust error handling."""
    articles = []
    try:
        logging.info("Fetching TOCondo PDFs from https://tocondonews.com/")
        response = robust_fetch_url("https://tocondonews.com/", timeout=15)
        if not response:
            logging.error("Failed to fetch TOCondo main page")
            return 0
            
        soup = BeautifulSoup(response.text, "html.parser")
        pdf_links = []
        
        # Find all PDF links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".pdf"):
                # Handle relative URLs
                if href.startswith("http"):
                    pdf_links.append(href)
                else:
                    pdf_links.append(f"https://tocondonews.com{href}" if href.startswith("/") else f"https://tocondonews.com/{href}")
        
        logging.info(f"Found {len(pdf_links)} PDF links")

        for link in pdf_links:
            try:
                title = link.split("/")[-1]
                logging.debug(f"Processing PDF: {title}")
                
                # Extract date first to avoid processing PDFs without valid dates
                published_date = extract_pdf_publish_date(title)
                if not published_date:
                    logging.debug(f"Discarding TOCondo PDF (no valid date): {title}")
                    continue
                
                # Restrict to articles published between March 6, 2025 and today
                start_date = datetime(2025, 3, 6, tzinfo=timezone.utc)
                end_date = datetime.utcnow().replace(tzinfo=timezone.utc)
                try:
                    pub_dt = datetime.fromisoformat(published_date)
                    pub_dt = normalize_datetime(pub_dt)
                except Exception:
                    continue
                if not (start_date <= pub_dt <= end_date):
                    logging.debug(f"Discarding TOCondo PDF (outside date range): {title}")
                    continue
                
                content = process_pdf(link)
                if not content.strip():
                    logging.warning(f"Skipping blank PDF: {link}")
                    continue
                
                # Check keyword matching
                search_text = f"{title} {content}"
                matched_keywords = get_matched_keywords(search_text, TOCONDO_KEYWORDS)
                if not matched_keywords:
                    logging.debug(f"Discarding TOCondo PDF (no tags): {title}")
                    continue
                
                published_date = safe_get_published_date(published_date)
                if not published_date:
                    continue
                    
                articles.append({
                    "title": title,
                    "link": link,
                    "published_date": published_date,
                    "scraped_date": datetime.utcnow().isoformat(),
                    "tags": matched_keywords,
                    "source": "tocondo",
                    "subreddit": None,
                    "upvotes": None,
                    "comments": None,
                    "content": content or None
                })

            except Exception as e:
                logging.warning(f"Error processing PDF {link}: {e}")

        count = save_scraped_data("tocondo", articles)
        return count
    except Exception as e:
        logging.error(f"TOCondo scraping failed: {str(e)}")
        return 0

def process_pdf(link):
    """Process PDF with robust error handling and memory-efficient streaming."""
    try:
        response = robust_fetch_url(link, timeout=15)
        if not response:
            logging.warning(f"Failed to fetch PDF: {link}")
            return ""
            
        with io.BytesIO(response.content) as pdf_file:
            try:
                reader = PyPDF2.PdfReader(pdf_file)
                content = "\n".join(page.extract_text() for page in reader.pages[:3] if page.extract_text())
                return content
            except Exception as e:
                logging.warning(f"PDF parsing failed for {link}: {e}")
                return ""
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
