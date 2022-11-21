# Sql Worker

---

### Usage

```python
from sql_worker import MySQLDatabase, PostgreSQLDatabase


md = MySQLDatabase('name', 'username', 'password')
pd = PostgreSQLDatabase('name', 'username', 'password')


with md:
    md.insert('table', name='Doka', age=10, is_old=False, info=None)
    md.select('table')
    md.get('table', 103)
    md.delete('table', 103)

pd.insert('table', name='Doka', age=10, is_old=False, info=None)
pd.select('table')
pd.get('table', 103)
pd.delete('table', 103)
```
