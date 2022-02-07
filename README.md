# Building a Data Warehouse and ETL Foundations
`DWH` `ETL` `Data Vault` `Anchor Modeling` `MPP` `Data Quality` `Apache Airflow` `Python` `Docker` `PostgreSQL`

### Lesson 4
- MPP системы    
    - SE (Shared-Everything) - архитектура с разделяемыми памятью и дисками
    - SD (Shared-Disks) - архитектура с разделяемыми дисками
    - SN (Shared-Nothing) - архитектура без совместного использования ресурсов
    - Teradata – это параллельная реляционная СУБД.
    - Vertica - реляционная колончатая СУБД.
    - ClickHouse — колоночная аналитическая СУБД
- Проектирование хранилища
    - Data vault
    - Anchor modeling

**HW** <br>
Спроектировать логические схемы `Data Vault` и `Anchor Modeling` на примере базы `Northwind`.

### Lesson 5
- ETL процесс
    - понятия ETL
    - основные свойства и требования к ETL процессам
    - разворачивание СУБД в докере
    - написание ETL процесса с помощью psycopg2

**HW** <br>
1) Развернуть всю архитектуру у себя
2) Написать ETL процесс для загрузки всех таблиц из postgres-источника в postgres-приемник
