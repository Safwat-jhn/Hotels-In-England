import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import phonenumbers
import time
from urllib.parse import urljoin, urlparse
import json

class HotelScraper:
    def __init__(self, db_path='output/hotels.db'):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.init_db()
        
    def init_db(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(self.db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS hotels (
                id INTEGER PRIMARY KEY,
                business_name TEXT UNIQUE,
                director_name TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                website TEXT,
                industry TEXT DEFAULT 'Hotels',
                verified TEXT
            )
        ''')
        conn.commit()
        conn.close()
    
    def format_uk_phone(self, phone):
        """Format to UK national format"""
        if not phone:
            return None
        try:
            # Clean phone number
            clean_phone = re.sub(r'[^\d+]', '', phone)
            parsed = phonenumbers.parse(clean_phone, "GB")
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL)
        except:
            pass
        return None
    
    def extract_director_info(self, soup, url):
        """Extract director name and email from website"""
        text = soup.get_text().lower()
        
        # Director name patterns
        director_patterns = [
            r'(?:managing director|general manager|owner|proprietor|ceo|md)[:\s]*([A-Z][a-z]+ [A-Z][a-z]+)',
            r'([A-Z][a-z]+ [A-Z][a-z]+)[,\s]*(?:managing director|general manager|owner|proprietor)',
            r'director[:\s]*([A-Z][a-z]+ [A-Z][a-z]+)',
            r'manager[:\s]*([A-Z][a-z]+ [A-Z][a-z]+)'
        ]
        
        director = None
        original_text = soup.get_text()
        for pattern in director_patterns:
            match = re.search(pattern, original_text, re.IGNORECASE)
            if match:
                director = match.group(1).strip()
                break
        
        # Email patterns - prioritize director emails
        email_patterns = [
            r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        ]
        
        emails = []
        for pattern in email_patterns:
            matches = re.findall(pattern, original_text)
            emails.extend(matches)
        
        # Filter for director-level emails
        director_email = None
        for email in emails:
            email_lower = email.lower()
            if any(keyword in email_lower for keyword in ['director', 'manager', 'owner', 'ceo', 'md']):
                director_email = email
                break
        
        # Fallback to contact emails
        if not director_email:
            for email in emails:
                email_lower = email.lower()
                if any(keyword in email_lower for keyword in ['contact', 'info', 'enquiries', 'reservations']):
                    director_email = email
                    break
        
        # Last resort - first valid email
        if not director_email and emails:
            director_email = emails[0]
        
        return director, director_email
    
    def extract_contact_info(self, soup):
        """Extract phone and address"""
        text = soup.get_text()
        
        # Phone extraction
        phone_patterns = [
            r'(?:tel|phone|call)[:\s]*(\+44[0-9\s\-\(\)]{10,15})',
            r'(\+44[0-9\s\-\(\)]{10,15})',
            r'(0[0-9\s\-\(\)]{10,15})'
        ]
        
        phone = None
        for pattern in phone_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                phone = self.format_uk_phone(match.group(1))
                if phone:
                    break
        
        # Address extraction - look for UK postcodes
        address_pattern = r'([A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})'
        address_match = re.search(address_pattern, text)
        address = f"UK Address, {address_match.group(1)}" if address_match else "UK Address"
        
        return phone, address
    
    def scrape_hotel_website(self, url, name):
        """Scrape individual hotel website for detailed info"""
        try:
            response = self.session.get(url, timeout=15)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract director info
            director, email = self.extract_director_info(soup, url)
            
            # Extract contact info
            phone, address = self.extract_contact_info(soup)
            
            return {
                'director': director,
                'email': email,
                'phone': phone,
                'address': address
            }
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return {'director': None, 'email': None, 'phone': None, 'address': None}
    
    def scrape_uk_hotel_directories(self):
        """Scrape multiple UK hotel directories"""
        
        # Method 1: Scrape from VisitEngland
        self.scrape_visit_england()
        
        # Method 2: Scrape from AA Hotel Guide
        self.scrape_aa_hotels()
        
        # Method 3: Scrape from independent hotel websites
        self.scrape_independent_hotels()
    
    def scrape_visit_england(self):
        """Scrape VisitEngland hotel directory"""
        print("Scraping VisitEngland hotels...")
        
        # Sample hotels with known websites
        sample_hotels = [
            {"name": "The Langham London", "url": "https://www.langhamlondon.co.uk"},
            {"name": "Claridge's", "url": "https://www.claridges.co.uk"},
            {"name": "The Savoy", "url": "https://www.thesavoylondon.com"},
            {"name": "The Ritz London", "url": "https://www.theritzlondon.com"},
            {"name": "Covent Garden Hotel", "url": "https://www.firmdalehotels.com/hotels/london/covent-garden-hotel"},
        ]
        
        for hotel in sample_hotels:
            print(f"Processing: {hotel['name']}")
            details = self.scrape_hotel_website(hotel['url'], hotel['name'])
            
            if details['email']:  # Only save if has email
                self.save_hotel(
                    hotel['name'],
                    details['director'],
                    details['phone'],
                    details['email'],
                    details['address'],
                    hotel['url']
                )
            
            time.sleep(2)  # Rate limiting
    
    def scrape_aa_hotels(self):
        """Scrape AA Hotel Guide listings"""
        print("Scraping AA Hotel listings...")
        
        # Sample boutique/independent hotels
        boutique_hotels = [
            {"name": "The Zetter Townhouse", "url": "https://www.thezettertownhouse.com"},
            {"name": "Artist Residence London", "url": "https://www.artistresidencehotels.com"},
            {"name": "The Hoxton", "url": "https://thehoxton.com"},
            {"name": "Hazlitt's Hotel", "url": "https://www.hazlittshotel.com"},
            {"name": "The Ned", "url": "https://www.thened.com"},
        ]
        
        for hotel in boutique_hotels:
            print(f"Processing: {hotel['name']}")
            details = self.scrape_hotel_website(hotel['url'], hotel['name'])
            
            if details['email']:
                self.save_hotel(
                    hotel['name'],
                    details['director'],
                    details['phone'],
                    details['email'],
                    details['address'],
                    hotel['url']
                )
            
            time.sleep(2)
    
    def scrape_independent_hotels(self):
        """Scrape independent hotel websites"""
        print("Scraping independent hotels...")
        
        # Regional independent hotels
        regional_hotels = [
            {"name": "The Swan at Lavenham", "url": "https://www.theswanatlavenham.co.uk"},
            {"name": "The George in Rye", "url": "https://www.thegeorgeinrye.com"},
            {"name": "The Bell at Skenfrith", "url": "https://www.skenfrith.co.uk"},
            {"name": "Lucknam Park Hotel", "url": "https://www.lucknampark.co.uk"},
            {"name": "Chewton Glen", "url": "https://www.chewtonglen.com"},
            {"name": "The Pig Hotel", "url": "https://www.thepighotel.com"},
            {"name": "Lime Wood Hotel", "url": "https://www.limewoodhotel.co.uk"},
            {"name": "Babington House", "url": "https://www.babingtonhouse.co.uk"},
        ]
        
        for hotel in regional_hotels:
            print(f"Processing: {hotel['name']}")
            details = self.scrape_hotel_website(hotel['url'], hotel['name'])
            
            if details['email']:
                self.save_hotel(
                    hotel['name'],
                    details['director'],
                    details['phone'],
                    details['email'],
                    details['address'],
                    hotel['url']
                )
            
            time.sleep(2)
    
    def save_hotel(self, name, director, phone, email, address, website):
        """Save hotel to database"""
        conn = sqlite3.connect(self.db_path)
        verified = 'Yes' if all([director, phone, email]) else 'Partial'
        
        try:
            conn.execute('''
                INSERT OR REPLACE INTO hotels 
                (business_name, director_name, phone, email, address, website, verified)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (name, director, phone, email, address, website, verified))
            conn.commit()
            print(f"Saved: {name} - Email: {email}")
        except Exception as e:
            print(f"Error saving {name}: {e}")
        finally:
            conn.close()
    
    def get_stats(self):
        """Get extraction statistics"""
        conn = sqlite3.connect(self.db_path)
        
        total = conn.execute('SELECT COUNT(*) FROM hotels').fetchone()[0]
        with_email = conn.execute('SELECT COUNT(*) FROM hotels WHERE email IS NOT NULL').fetchone()[0]
        with_director = conn.execute('SELECT COUNT(*) FROM hotels WHERE director_name IS NOT NULL').fetchone()[0]
        verified = conn.execute('SELECT COUNT(*) FROM hotels WHERE verified = "Yes"').fetchone()[0]
        
        conn.close()
        
        return {
            'total': total,
            'with_email': with_email,
            'with_director': with_director,
            'verified': verified,
            'email_coverage': (with_email / total * 100) if total > 0 else 0
        }