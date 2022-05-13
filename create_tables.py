from mysql.connector import connect, Error

try:
    with connect(
        host="172.18.0.2",
        port="3306",
        user="spark",
        password="Ehj,jhjC",
        database="datawarehouse",
    ) as connection:

        create_sales_table_query = """
            CREATE TABLE IF NOT EXISTS sales (
                sale_id INT AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(40), 
                color VARCHAR(15), 
                material VARCHAR(15),
                department VARCHAR(40), 
                promo_code VARCHAR(35), 
                price DECIMAL(10,2),
                load_date DATE, 
                source_id INT,
                batch_id INT
            )   
        """

        with connection.cursor() as cursor:
            cursor.execute(create_sales_table_query)
            connection.commit()

        create_batch_statistics_table_query = """
            CREATE TABLE IF NOT EXISTS batch_stat (
                batch_id INT AUTO_INCREMENT PRIMARY KEY,
                batch_size INT, 
                load_time INT,
                max_price DECIMAL(10,2), 
                min_price DECIMAL(10,2),
                sources_count INT
            )
        """

        with connection.cursor() as cursor:
            cursor.execute(create_batch_statistics_table_query)
            connection.commit()

        create_features_table_query = """
                    CREATE TABLE IF NOT EXISTS features (
                        sale_id INT PRIMARY KEY,
                        is_metall INT, 
                        is_nature INT,
                        is_sint INT,
                        is_cloth INT,
                        length_name INT,
                        sum_promo_digit INT,
                        price DECIMAL(10,2)
                    )
                """

        with connection.cursor() as cursor:
            cursor.execute(create_features_table_query)
            connection.commit()
        
        
        with connection.cursor() as cursor:
            cursor.execute(create_batch_statistics_table_query)
            connection.commit()

        create_models_table_query = """
                    CREATE TABLE IF NOT EXISTS models (
                        model_id INT  AUTO_INCREMENT PRIMARY KEY, 
                        file_name VARCHAR(10),
                        num_rows INT,
                        rmse DECIMAL(10, 5),
                        mse DECIMAL(10, 5),
                        mae DECIMAL(10,5),
                        r2 DECIMAL(10, 5)
                    )
                """

        with connection.cursor() as cursor:
            cursor.execute(create_models_table_query)
            connection.commit()
            
        
        print("All tables are created")

except Error as e:
    print(e)
