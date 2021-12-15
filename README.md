https://www.python.org/dev/peps/pep-0249/


### IoTDB Meta Tree 与 DB-API2 映射关系

![IoTDB Meta Tree ](http://lifengchuan.oss-cn-beijing.aliyuncs.com/uPic/20211213/image-20211213102145519.png)



# 使用方法：

## DB-API
```python

from pyiotdb.db import connect

ip = "ip"
port = 6668
username = 'root'
password = 'root'
conn = connect(host=ip, port=port, username=username, password=password)
cursor = conn.cursor()
cursor.execute('show  storage group')
print(cursor.rowcount)
```

## SQLAlchemy

```python
from sqlalchemy.engine import create_engine

engine = create_engine('iotdb://root:root@ip:6668/')
connection = engine.connect()

result = connection.execute("SHOW STORAGE GROUP")

for row in result.fetchall():
    print(row['storage group'])

```


# Requirements

python3.7 

```shell script

   pip3 install  'future'
   pip3 install  'python-dateutil'
   pip3 install   'apache-iotdb==0.12.2'
   pip3 install   'more-itertools==8.3.0'


```