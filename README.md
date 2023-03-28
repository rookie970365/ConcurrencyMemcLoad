# Concurrency MemcLoad
Многопроцессная реализация загрузчика логов в memcached
***
Сĸрипт парсит и заливает в мемĸеш поминутную
выгрузĸу логов треĸера установленных приложений. Ключом является тип и
идентифиĸатор устройства через двоеточие, значением являет protobuf
сообщение
### Архитектура

Загрузчик реализован с помощью классов [Process](https://docs-python.ru/standart-library/paket-multiprocessing-python/funktsija-process-modulja-multiprocessing/) 
и [Queue](https://docs-python.ru/standart-library/paket-multiprocessing-python/klass-queue-modulja-multiprocessing/) 
из модуля [multiprocessing](https://docs-python.ru/standart-library/paket-multiprocessing-python/),
количество процессов задается по количеству ядер процессора с помощья функции [multiprocessing.cpu_count()]()
### Ссылĸи на tsv.gz лог-файлы:
https://cloud.mail.ru/public/2hZL/Ko9s8R9TA

https://cloud.mail.ru/public/DzSX/oj8RxGX1A

https://cloud.mail.ru/public/LoDo/SfsPEzoGc
### Пример строки лог-файла:
idfa&nbsp;&nbsp;&nbsp;&nbsp;5rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;5423,43,567,3,7,23

gaid&nbsp;&nbsp;&nbsp;&nbsp;6rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;6423,43,567,3,7,23
## Запуск
```sh
python concurrency_memcload.py [-t] [-l] [--dry] [--pattern] [--idfa] [--gaid] [--adid] [--dvid]
```


### Параметры
```
"-t", "--test" - тестовый запуск
"-l", "--log"  - лог-файл
"--dry"        - парсинг лог-файла без записи в memcached
"--pattern"    - паттерн пути к лог-файлу

"--idfa",      - строки подключения <host:port> к memcached
"--gaid",        для соответствующего типа устройства
"--adid", 
"--dvid"       
                 
```
## Тестирование 
```sh
 pytest  
 ```
