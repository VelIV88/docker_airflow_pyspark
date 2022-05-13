from datetime import timedelta
from datetime import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

airflow_home = "/usr/local/airflow"

default_args = {
    "owner": "Data Engineer",
    "retries": 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'pipline',
    default_args=default_args,
    schedule_interval=timedelta(hours=2),
    start_date=days_ago(1),
    concurrency=1,
    max_active_runs=1,
)


def extract_data(path_to_sources):
    """
    генерируем список таск по извлечению данных из разных источников
    :param str
    :return: list
    """
    import requests
    import json
    from random import randint

    # Структура файла источников:
    # Разделитель - табуляция.
    # Первое значение - идинтификатор источника
    # Второе значение - URL источника
    with open(f"{path_to_sources}/sources.txt") as f:
        sources = [row.strip().split() for row in f]

    def extractor(source_id, url):
        """
        сохраняем результаты запроса к источнику в json файлы
        :param source_id: int
        :param url: str
        """
        param = {"size": randint(30, 90)}
        response = requests.get(url, params=param)
        results = list(map(lambda elem:
                           {**elem, **{"source_id": int(source_id)}},
                           response.json()))

        with open(f"{path_to_sources}/tmp_{source_id}.json", "w") as file:
            json.dump(results, file)

    return [
        PythonOperator(
            task_id=f"extract_source_{source_id}",
            python_callable=extractor,
            op_kwargs={"source_id": source_id, "url": url},
            dag=dag,)
        for source_id, url in sources
    ]


extract_data_tasks = extract_data(f"{airflow_home}/sources")


def batch_load():
    """
    Формируем dataframe из json файлов
    и грузим данные в таблицы MySQL
    """
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F

    airflow_home = "/usr/local/airflow"

    start_time = dt.now()

    spark = SparkSession\
        .builder \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.29.jar") \
        .master("local[1]") \
        .appName("Json to DF") \
        .getOrCreate()

    # Находим номер последнего батча
    batch_info = spark.read.format('jdbc').options(
        url='jdbc:mysql://172.18.0.2:3306/datawarehouse',
        driver='com.mysql.jdbc.Driver',
        dbtable='batch_stat',
        user='spark',
        password='Ehj,jhjC').load()

    last_batch_id = batch_info.select(F.max("batch_id")).collect()[0]["max(batch_id)"]
    curr_batch_id = 1 if last_batch_id is None else last_batch_id + 1

    # Формируем датафрейм из json файлов и складываем данные батча в бд
    df = spark.read.json(f"{airflow_home}/sources/*.json")
    df = df.withColumn("load_date", F.lit(dt.now().date()))\
        .withColumn("load_time", F.lit(dt.now().time().strftime("%H:%M:%S")))\
        .withColumn("batch_id", F.lit(curr_batch_id))

    df.select(
        "product_name", "color", "material",
        "department", "promo_code", "price",
        "load_date", "source_id", "batch_id")\
        .write.format('jdbc').options(
            url='jdbc:mysql://172.18.0.2:3306/datawarehouse',
            driver='com.mysql.jdbc.Driver',
            dbtable='sales',
            user='spark',
            password='Ehj,jhjC')\
        .mode('append')\
        .save()

    stop_time = dt.now()

    # Рассчитываем статистики по батчу и записываем их в бд
    batch_stat = spark.createDataFrame([{
        "batch_size": df.count(),
        "load_time": (stop_time - start_time).seconds,
        "max_price": df.select(F.max("price")).collect()[0]["max(price)"],
        "min_price": df.select(F.min("price")).collect()[0]["min(price)"],
        "sources_count": df.select("source_id").distinct().count()
    }])

    batch_stat.write.format('jdbc').options(
            url='jdbc:mysql://172.18.0.2:3306/datawarehouse',
            driver='com.mysql.jdbc.Driver',
            dbtable='batch_stat',
            user='spark',
            password='Ehj,jhjC')\
        .mode('append')\
        .save()

    spark.stop()

load_data_task = PythonOperator(
    python_callable=batch_load,
    task_id='load_batch',
    dag=dag,
)


def check_batch():
    """
    Сравниваем статистики последнего батча с заданными параметрами
    :return: id следующей таски
    """
    from mysql.connector import connect

    with connect(
            host="172.18.0.2",
            port="3306",
            user="spark",
            password="Ehj,jhjC",
            database="datawarehouse",
    ) as connection:

        last_batch_query = """
            SELECT *
            FROM batch_stat
            WHERE batch_id = (SELECT max(batch_id) FROM batch_stat)
        """

        with connection.cursor() as cursor:
            cursor.execute(last_batch_query)
            _, cnt_row, dur_load, _, _,  sour_num = cursor.fetchall()[0]

        return "alert" if cnt_row < 100 or dur_load > 120 or sour_num < 3 else "dummy_alert"


check_batch_stat_task = BranchPythonOperator(
    task_id="check_batch",
    python_callable=check_batch,
    dag=dag,
)

batch_alert = EmailOperator(
    task_id="alert",
    to="iapenkov@omgtu.tech",
    subject="Airflow alert!",
    html_content="Alarm! Last batch is wrong! Check batch statistics!",
    dag=dag,
)

dummy_alert = DummyOperator(
    task_id="dummy_alert",
    dag=dag,
    )


def check_cnt_row_features():
    """
    Проверяем число строк для которых еще не посчитаны фичи.
    При значении больше 200 запускаем джобу на расчёт фичей.
    :return: id следующего таска
    """
    from mysql.connector import connect

    with connect(
            host="172.18.0.2",
            port="3306",
            user="spark",
            password="Ehj,jhjC",
            database="datawarehouse",
    ) as connection:
        last_sale_query = """
                SELECT max(sale_id)
                FROM sales
        """

        with connection.cursor() as cursor:
            cursor.execute(last_sale_query)
            last_sale = cursor.fetchall()[0][0]
            last_sale = 0 if last_sale is None else last_sale

        last_feature_query = """
                        SELECT max(sale_id)
                        FROM features
        """

        with connection.cursor() as cursor:
            cursor.execute(last_feature_query)
            last_feature = cursor.fetchall()[0][0]
            last_feature = 0 if last_feature is None else last_feature

    return "load_feature" if last_sale - last_feature > 500 else "dummy_feature"


check_cnt_row_task = BranchPythonOperator(
    task_id="check_cnt_row",
    python_callable=check_cnt_row_features,
    trigger_rule='all_done',
    dag=dag,
    )

dummy_feature = DummyOperator(
    task_id="dummy_feature",
    dag=dag,
    )


def features_load():
    """
    Рассчитываем и грузим фичи в витрину.
    """
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F

    spark = SparkSession\
        .builder \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.29.jar") \
        .master("local[1]") \
        .appName("Json to DF") \
        .getOrCreate()

    # Находим id последней строки в таблице фичей
    feature_info = spark.read.format('jdbc').options(
        url='jdbc:mysql://172.18.0.2:3306/datawarehouse',
        driver='com.mysql.jdbc.Driver',
        dbtable='features',
        user='spark',
        password='Ehj,jhjC').load()

    last_feature = feature_info.select(F.max("sale_id")).collect()[0]["max(sale_id)"]
    last_feature = 1 if last_feature is None else last_feature + 1

    # Загружаем в датафрейм данные по батчам
    df = spark.read.format('jdbc').options(
        url='jdbc:mysql://172.18.0.2:3306/datawarehouse',
        driver='com.mysql.jdbc.Driver',
        dbtable='sales',
        user='spark',
        password='Ehj,jhjC').load()

    # Выбираем свежие данные и рассчитываем фичи
    df = df.filter(df["sale_id"] >= last_feature)


    def is_material(material, mat_lst):
        return 1 if material in mat_lst else 0


    df = df.withColumn("length_name", F.length(df["product_name"]))
    df = df.withColumn("promo_code", F.regexp_replace("promo_code", "[^0-9]", ""))

    promo_sum = F.udf(lambda str_num: sum(map(int, list(str_num))))
    df = df.withColumn("sum_promo_digit", promo_sum(F.col("promo_code")))

    is_metall = F.udf(lambda mater:
        is_material(mater, ["Bronze", "Copper", "Aluminum", "Steel", "Iron"])
                    )
    is_nature = F.udf(lambda mater:
        is_material(mater, ["Paper", "Leather", "Granite", "Wooden", "Marble"])
                    )
    is_cloth = F.udf(lambda mater:
        is_material(mater, ["Silk", "Cotton", "Wool", "Linen"])
                    )
    is_sint = F.udf(lambda mater:
        is_material(mater, ["Plastic", "Rubber", "Concrete"])
                    )

    df = df\
        .withColumn("is_metall", is_metall(F.col("material")))\
        .withColumn("is_nature", is_nature(F.col("material")))\
        .withColumn("is_cloth", is_cloth(F.col("material")))\
        .withColumn("is_sint", is_sint(F.col("material")))


    df.select(
        "sale_id", "is_metall", "is_nature",
        "is_sint", "is_cloth", "length_name",
        "sum_promo_digit", "price")\
        .write.format('jdbc').options(
            url='jdbc:mysql://172.18.0.2:3306/datawarehouse',
           driver='com.mysql.jdbc.Driver',
            dbtable='features',
            user='spark',
            password='Ehj,jhjC')\
        .mode('append')\
        .save()

    spark.stop()


load_feature_task = PythonOperator(
    python_callable=features_load,
    task_id="load_feature",
    dag=dag,
)


def check_new_feachures():
    """
    Проверяем есть ли новые фичи
    :return: id следующего таска
    """
    from mysql.connector import connect

    with connect(
            host="172.18.0.2",
            port="3306",
            user="spark",
            password="Ehj,jhjC",
            database="datawarehouse",
    ) as connection:
        last_fit_num_rows_query = """
                SELECT max(num_rows)
                FROM models
        """

        with connection.cursor() as cursor:
            cursor.execute(last_fit_num_rows_query)
            fit_num_row = cursor.fetchall()[0][0]
            fit_num_row = 0 if fit_num_row is None else fit_num_row

        current_num_rows_query = """
                        SELECT count(*)
                        FROM features
        """

        with connection.cursor() as cursor:
            cursor.execute(current_num_rows_query)
            current_num_row = cursor.fetchall()[0][0]
            current_num_row = 0 if current_num_row is None else current_num_row

    return "model_pipline" if current_num_row > fit_num_row else "dummy_model"


check_new_features_task = BranchPythonOperator(
    task_id="check_new_features",
    python_callable=check_new_feachures,
    trigger_rule='all_done',
    dag=dag,
    )

dummy_model = DummyOperator(
    task_id="dummy_model",
    dag=dag,

    )


def model_pipline():
    from pyspark.sql.session import SparkSession
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.regression import LinearRegression
    from mysql.connector import connect

    spark = SparkSession \
        .builder \
        .config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.29.jar") \
        .master("local[1]") \
        .appName("Json to DF") \
        .getOrCreate()

    dataset = spark.read.format('jdbc').options(
        url='jdbc:mysql://172.18.0.2:3306/datawarehouse',
        driver='com.mysql.jdbc.Driver',
        dbtable='features',
        user='spark',
        password='Ehj,jhjC').load()

    dataset = dataset.select("is_metall",
                             "is_nature",
                             "is_sint",
                             "is_cloth",
                             "length_name",
                             "sum_promo_digit",
                             "price")


    # Преобразуем данные к векторному виду
    numeric_cols = ["is_metall", "is_nature", "is_sint", "is_cloth", "length_name", "sum_promo_digit"]
    assembler = VectorAssembler(
        inputCols=numeric_cols,
        outputCol='Attributes')
    output = assembler.transform(dataset)
    finalized_data = output.select("Attributes", "price")

    # Обучаем модель на тренировочной выборке
    train_data, test_data = finalized_data.randomSplit([0.8, 0.2])
    regressor = LinearRegression(featuresCol='Attributes', labelCol='price')
    model = regressor.fit(train_data)

    # Сохраняем модель и записываем метрики в бд
    with connect(
            host="172.18.0.2",
            port="3306",
            user="spark",
            password="Ehj,jhjC",
            database="datawarehouse",
    ) as connection:
        last_model_id_query = """
                SELECT max(model_id)
                FROM models
        """

        with connection.cursor() as cursor:
            cursor.execute(last_model_id_query)
            last_model_id = cursor.fetchall()[0][0]
            current_model_id = 1 if last_model_id is None else last_model_id + 1

        curr_rows_num = dataset.count()

        pred = model.evaluate(test_data)

        #model.save(f"{airflow_home}/models/regression_{current_model_id}.model")

        eval = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
        rmse = eval.evaluate(pred.predictions)
        mse = eval.evaluate(pred.predictions, {eval.metricName: "mse"})
        mae = eval.evaluate(pred.predictions, {eval.metricName: "mae"})
        r2 = eval.evaluate(pred.predictions, {eval.metricName: "r2"})

        load_metrix = f"""
                        INSERT INTO models(
                            file_name,
                            num_rows,
                            rmse,
                            mse,
                            mae,
                            r2)
                        VALUES (
                            'model_{current_model_id}',
                            {curr_rows_num},
                            {rmse},
                            {mse},
                            {mae},
                            {r2}
                        )
                """

        with connection.cursor() as cursor:
            cursor.execute(load_metrix)
        connection.commit()

    spark.stop()


model_pipline_task = PythonOperator(
    task_id="model_pipline",
    python_callable=model_pipline,
    dag=dag,
)


drop_jsons = BashOperator(
    task_id="drop_jsons",
    bash_command=f"find -name '{airflow_home}/sources/*.json' -type f -delete",
    trigger_rule='all_done',
    dag=dag,
)

extract_data_tasks >> load_data_task >> check_batch_stat_task >> [dummy_alert, batch_alert] >> check_cnt_row_task >> \
    [load_feature_task, dummy_feature] >> check_new_features_task >> [model_pipline_task, dummy_model] >> drop_jsons
