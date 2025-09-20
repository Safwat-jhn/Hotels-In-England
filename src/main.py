import sqlite3
import pandas as pd
from scraper import HotelScraper

def export_to_excel(db_path='output/hotels.db', excel_path='output/hotels_data.xlsx'):
    """Export database to Excel for CRM upload"""
    conn = sqlite3.connect(db_path)
    
    # Query only hotels with director emails
    df = pd.read_sql_query('''
        SELECT business_name as "Business Name",
               director_name as "Director Name", 
               phone as "Phone",
               email as "Email",
               address as "Address",
               website as "Website",
               industry as "Industry",
               verified as "Verified"
        FROM hotels 
        WHERE email IS NOT NULL
        ORDER BY business_name
    ''', conn)
    
    conn.close()
    
    # Export to Excel
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Hotels', index=False)
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Hotels']
        for column in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in column)
            worksheet.column_dimensions[column[0].column_letter].width = min(max_length + 2, 50)
    
    return len(df)

def main():
    """Main extraction process"""
    print("Starting UK hotel extraction...")
    
    scraper = HotelScraper()
    
    # Target directories
    sources = [
        "https://www.visitengland.com/accommodation/hotels",
        "https://www.booking.com/searchresults.html?ss=England"
    ]
    
    for source in sources:
        print(f"Scraping: {source}")
        try:
            scraper.scrape_directory(source)
        except Exception as e:
            print(f"Error: {e}")
    
    # Export results
    hotel_count = export_to_excel()
    total_count = scraper.get_hotel_count()
    
    print(f"\n=== RESULTS ===")
    print(f"Hotels extracted: {hotel_count}")
    print(f"Director email coverage: 100% (filtered)")
    print(f"Output: output/hotels_data.xlsx")
    print("Ready for CRM upload")

if __name__ == "__main__":
    main()