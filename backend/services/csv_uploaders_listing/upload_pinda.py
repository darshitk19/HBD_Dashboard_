import pandas as pd
from database.mysql_connection import get_mysql_connection
from utils.safe_get import safe_get

def upload_pinda_data(file_paths):

    if not file_paths:
        raise ValueError("No file paths provided for upload.")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    inserted = 0
    batch_size = 10000

    try:
        for file in file_paths:
            with open(file,newline='',encoding='utf-8') as f:
                chunkFile_data = pd.read_csv(file,chunksize = batch_size)
                for chunk in chunkFile_data:
                    chunk_data = []
                    for row in chunk.itertuples(index=False):
                        row_tuple = (
                        safe_get(row, 'Name'),
                        safe_get(row, 'Url'),
                        safe_get(row, 'Address'),
                        safe_get(row, 'Phone'),
                        safe_get(row, 'Category'),
                        safe_get(row, 'Country'),
                        safe_get(row, 'City'),
                        )
                        chunk_data.append(row_tuple)


                    # execute batch insert
                    insert_query = '''
                        INSERT INTO pinda (
                            name, url, address, number, category, country, city
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                            url = VALUES(url),
                            number = VALUES(number),
                            category = VALUES(category),
                            country = VALUES(country),
                            city = VALUES(city);
                        '''
                    cursor.executemany(insert_query, chunk_data)
                    connection.commit()
                    inserted+=len(chunk_data)
        return inserted
    finally:
        cursor.close()
        connection.close()