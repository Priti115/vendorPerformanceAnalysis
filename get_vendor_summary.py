import sqlite3
import pandas as pd
import logging
from ingestion_db import  ingest_db
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''this function will merge the different tables to get the overall vendor summary and adding columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
        ),
    PurchaseSummary AS (
    SELECT 
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price as ActualPrice,
        pp.Volume,
        SUM(p.Quantity) as TotalPurchaseQuantity,
        SUM(p.Dollars) as TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
),
    SalesSummary AS (
        SELECT
           VendorNo,
           Brand,
           SUM(SalesQuantity) as TotalSalesQuantity,
           SUM(SalesDollars) as TotalSalesDollars,
           SUM(SalesPrice) as TotalSalesPrice,
           SUM(ExciseTax) as TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost        -- ✅ FIXED HERE
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary

def clean_data(df):
    # Convert Volume to float64
    df['Volume'] = df['Volume'].astype('float64')
    
    # Fill NA values with 0
    df.fillna(0, inplace=True)
    
    # Clean string columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()  # Fixed: was using VendorName twice
    
    # Calculate metrics
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = df['GrossProfit'] / df['TotalSalesDollars']
    df['StockTurnovr'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    
    return df

if __name__ == '__main__':

    conn = sqlite3.connect('Inventory.db')

    logging.info('Creating Vendor Summary Table.........')
    summary_df = create_vendor_summary(conn)   # ✅ Corrected here
    logging.info(summary_df.head())

    logging.info('Cleaning Data........')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data..........')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Completed')




