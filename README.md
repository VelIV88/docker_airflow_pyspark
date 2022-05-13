# docker_airflow_pyspark

Разработка прототипа загрузки, обработки и анализа данных.  

Использовался API генератора данных:  
https://random-data-api.com/api/commerce/random_commerce  

Больше всего сил и времени отнял процесс развёртывания среды.  
В итоге стабильно получилось запустить:  
-- Pyspark 2.3.1  
-- Airflow 1.10.9  
В качестве хранилища данных использована база данных MysQL.  

Команда для запуска среды:  
sudo docker-compose -f docker-compose.yml up -d --build  

Среда запускается от 2 до 6 минут.  
После запуска среды необходимо выполнить команду  
для создания используемых таблиц в базе данных (airflow_spark_webserver_1 при необходимости заменить на название контейнера с вебсервером):  
sudo docker exec -ti airflow_spark_webserver_1 python create_tables.py  

Адрес WebUI:  
http://localhost:8080/  

Параметры для подключения к базе данных:  
host: 0.0.0.0  
port: 3307  
user: spark  
password: Ehj,jhjC  

В списке Dag нас интересует pipline.  
Реализация механизма состоит из следующих этапов:  
    • Получение данных через API генератора:  PythonOperator парсит текстовый файл sources.txt, в котором хранятся источники данных. Происходит динамическая генерация джоб, по выгрузки из каждого источника. В каждый файл дописывается id источника В случае необходимости (например если источником будет не только API) файл можно сконфигурировать (добавить параметр типа источника), a генератор можно расширить операторами, зависящими от типа источника. Файл использовался для простоты разработки и тестирования. Конечно лучше хранить информацию в БД и доставать источники и метаданные из нее.  
    • Загрузка батча данных в хранилище: pyspark читает файлы json и формирует датафрейм. Добавляются колонки с метаданными, и рассчитываются статистики по батчу. Сам батч и статистики складываются в хранилище данных. В качестве воркера используется PythonOperator, по хорошему, если есть кластер со spark, я бы использовал SparkSubmitOperator.  
    • Проверка статистик по загруженному батчу. Проверял количество записанных строк, время загрузки и число источников. Пороговые значения записаны непосредсвенно в код. Конечно по хорошему их нужно хранить в отдельной табличке, где их удобно редактировать, и считывать из неё.  
    • В случае отклонений в статистике, Сообщение с алертом уходит на почту (smpt не конфигурировал, поэтому естественно эта таска будет падать)  
    • Проверка Количества строк, записанных с момента последнего формирования витрины с фичами.  
    • Если число строк более 200 запускается джоба по формированию фичей из новых данных. Pyspark выгружает ещё не обработанные данные в dataframe. Формируются следующие признаки: принадлежность материала к одной из групп (4 бинарных признака), длина названия товара, сумма цифр из промокода, цена. Полученные фичи дописываются в витрину.  
    • Если в витрине появились новые данные, запускаем джобу по обучению модели машинного обучения.  Использовалась модель Линейной регрессии. Обучаем предсказывать цену по остальным признакам в витрине. Обучаем на тренировочных данных, проверяем на тестовых. Измеряем метрики и складываем в информацию в базу данных. К сожалению так и не получилось сохранить саму модель в файл.  
    • Удаляем из папки источников скачанные json.  

Есть ряд вопросов которые не были учтены в ходе разработки, или моменты, которые можно было улучшить:  
    • Не рассмотрены вопросы безопасности. Для простоты данные для авторизации передаются прямо в коде. Так конечно поступать не стоит.  
    • Как уже отмечалось, в качестве воркеров стоит применять средства Spark.  
    • Не использовались хуки, работу с базой данных можно было конечно упростить. Но для переносимости кода было принято решение работать с библиотеками python для подключения к бд.  
    • Не использовался Xcom, для обмена метаданными, приходилось выкручиваться.  
