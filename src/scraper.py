import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import phonenumbers
import time

class HotelScraper:
    def __init__(self, db_path='output/hotels.db'):
        self.db_path = db_path
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
        try:
            parsed = phonenumbers.parse(phone, "GB")
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL)
        except:
            pass
        return None
    
    def extract_director_email(self, text):
        """Extract director-level email"""
        patterns = [
            r'(?:director|manager|owner|ceo|md).*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
            r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}).*?(?:director|manager|owner|ceo|md)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                return match.group(1)
        
        # Fallback to contact emails
        contact_match = re.search(r'(?:contact|info)@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text.lower())
        return contact_match.group(0) if contact_match else None
    
    def scrape_hotel_details(self, url):
        """Scrape individual hotel website"""
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text()
            
            # Extract director name
            director_pattern = r'(?:managing director|general manager|owner)[:\s]*([A-Z][a-z]+ [A-Z][a-z]+)'
            director_match = re.search(director_pattern, text, re.IGNORECASE)
            director = director_match.group(1) if director_match else None
            
            # Extract phone
            phone_matches = re.findall(r'(?:\+44|0)[0-9\s\-\(\)]{10,15}', text)
            phone = self.format_uk_phone(phone_matches[0]) if phone_matches else None
            
            # Extract email
            email = self.extract_director_email(text)
            
            return director, phone, email
        except:
            return None, None, None
    
    def scrape_directory(self, base_url, max_pages=3):
        """Scrape hotel directory"""
        for page in range(1, max_pages + 1):
            try:
                response = requests.get(f"{base_url}?page={page}", timeout=10)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find hotel listings
                hotels = soup.find_all(['div', 'article'], class_=re.compile(r'hotel|listing'))
                
                for hotel in hotels:
                    name_elem = hotel.find(['h2', 'h3', 'a'])
                    if not name_elem:
                        continue
                        
                    name = name_elem.get_text().strip()
                    website_elem = hotel.find('a', href=True)
                    website = website_elem['href'] if website_elem else None
                    
                    # Get detailed info
                    director, phone, email = None, None, None
                    if website and email is None:
                        director, phone, email = self.scrape_hotel_details(website)
                        time.sleep(1)
                    
                    # Only save if has director email
                    if email:
                        self.save_hotel(name, director, phone, email, "UK Address", website)
                
                time.sleep(2)
            except Exception as e:
                print(f"Error on page {page}: {e}")
    
    def save_hotel(self, name, director, phone, email, address, website):
        """Save hotel to database"""
        conn = sqlite3.connect(self.db_path)
        verified = 'Yes' if all([director, phone, email]) else 'Partial'
        
        try:
            conn.execute('''
                INSERT OR IGNORE INTO hotels 
                (business_name, director_name, phone, email, address, website, verified)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (name, director, phone, email, address, website, verified))
            conn.commit()
        except:
            pass
        finally:
            conn.close()
    
    def get_hotel_count(self):
        """Get total hotels with director emails"""
        conn = sqlite3.connect(self.db_path)
        count = conn.execute('SELECT COUNT(*) FROM hotels WHERE email IS NOT NULL').fetchone()[0]
        conn.close()
        return count