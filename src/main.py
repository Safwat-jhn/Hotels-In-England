import sqlite3
import pandas as pd
from scraper import HotelScraper
import os

def ensure_output_dir():
    """Create output directory if it doesn't exist"""
    if not os.path.exists('output'):
        os.makedirs('output')

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
    
    if df.empty:
        print("No hotels with emails found to export")
        return 0
    
    # Export to Excel
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Hotels', index=False)
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Hotels']
        for column in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in column)
            worksheet.column_dimensions[column[0].column_letter].width = min(max_length + 2, 50)
    
    print(f"Exported {len(df)} hotels to {excel_path}")
    return len(df)

def main():
    """Main extraction process"""
    print("=== UK Hotel Data Extraction ===")
    print("Target: Director emails, UK phone format, CRM-ready")
    print()
    
    # Ensure output directory exists
    ensure_output_dir()
    
    # Initialize scraper
    scraper = HotelScraper()
    
    # Run extraction from multiple sources
    try:
        scraper.scrape_uk_hotel_directories()
    except KeyboardInterrupt:
        print("\nExtraction interrupted by user")
    except Exception as e:
        print(f"Extraction error: {e}")
    
    # Get statistics
    stats = scraper.get_stats()
    
    # Export to Excel
    exported_count = export_to_excel()
    
    # Print results
    print("\n=== EXTRACTION RESULTS ===")
    print(f"Total hotels processed: {stats['total']}")
    print(f"Hotels with emails: {stats['with_email']}")
    print(f"Hotels with director names: {stats['with_director']}")
    print(f"Fully verified records: {stats['verified']}")
    print(f"Email coverage: {stats['email_coverage']:.1f}%")
    print(f"Exported to Excel: {exported_count} records")
    print()
    print("Files created:")
    print("- output/hotels.db (SQLite database)")
    print("- output/hotels_data.xlsx (Excel for CRM)")
    print()
    print("✅ Ready for CRM upload!")

if __name__ == "__main__":
    main()