import pandas as pd
from database.mysql_connection import get_mysql_connection
from utils.safe_get import safe_get

def upload_amazon_products_data(file_paths):
    if not file_paths:
        raise ValueError("No file provided to upload")
    
    connection = get_mysql_connection()
    cursor = connection.cursor()
    inserted = 0
    batch_size = 10000

    try:
        for file in file_paths:
            with open(file,newline='',encoding='utf-8') as f:  
                chunkFile_data = pd.read_csv(file,chunk_size=batch_size)
                for chunk in chunkFile_data:
                    chunk = chunk.rename(columns=lambda c:c.replace(' ','_'))
                    chunk_data=[]
                    for row in chunk.itertuples(index=False):
                        row_tuple = (
                        safe_get(row, 'ASIN'),                             
                        safe_get(row, 'Product_name'),
                        safe_get(row, 'price'),
                        safe_get(row, 'rating'),
                        safe_get(row, 'Number_of_ratings'),
                        safe_get(row, 'Brand'),
                        safe_get(row, 'Seller'),
                        safe_get(row, 'category'),
                        safe_get(row, 'subcategory'),
                        safe_get(row, 'sub_sub_category'),
                        safe_get(row, 'category_sub_sub_sub'),
                        safe_get(row, 'colour'),
                        safe_get(row, 'size_options'),
                        safe_get(row, 'description'),
                        safe_get(row, 'link'),
                        safe_get(row, 'Image_URLs'),
                        safe_get(row, 'About_the_items_bullet'),
                        safe_get(row, 'Product_details'),
                        safe_get(row, 'Additional_Details'),
                        safe_get(row, 'Manufacturer_Name'),
                        )
                        chunk_data.append(row_tuple)

                    # storing the valus in the database
                    upload_amazon_products_data_query = '''
                    INSERT INTO amazon_products (
                        ASIN, Product_name, price, rating, Number_of_ratings, Brand, Seller, category, subcategory, sub_sub_category, category_sub_sub_sub,colour,size_options,description,link,Image_URLs,About_the_items_bullet,Product_details,Additional_Details,Manufacturer_Name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                    cursor.executemany(upload_amazon_products_data_query,chunk_data)
                    connection.commit()
                    inserted+=len(chunk_data)
        return inserted
    finally:
        cursor.close()
        connection.close()
