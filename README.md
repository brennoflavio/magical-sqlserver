# Magical SQL Server

A tool that integrates Microsoft SQL Server into applications like magic.

## What is this?

Do you have an application that needs to consume data stored on SQL Server Database, or needs to post data into this database? Magical SQL Server will handle that for you easily.

## Requirements

To use this module, you have to install python3 and pymssql:

```
pip3 install pymssql
```

Bulk insert method uses BCP to copy multiple rows efficiently. See [Microsoft Documentation](https://docs.microsoft.com/pt-br/sql/tools/bcp-utility?view=sql-server-2017) to install it on your machine.

## Usage

Let's say that you have a table called ```my_database.dbo.users```:

id | name | team
-- | ----- | ----
1 | example | example
2 | new_user | users

To retrieve all the data from the table:
```
from magical_sqlserver import SQLServer
sql = SQLServer (
    user,
    password,
    host,
    my_database,
    port=1433,
)

data = sql.select("users")
```

This will return:
```
[
  {
    "id": 1,
    "name": "example",
    "team": "example"
  },
  {
    "id": 2,
    "name": "new_user",
    "team": "users"
  }
]
```

You can filter columns and get specific columns:
```
data = sql.select("users", condition={"id":1}, columns=["name"])
```

With result ```{"name":"example"}```.

### With Context

You may use ```with``` statement then Magical SQL Server will open and close a connection for you as your statement ends:

```
with SQLServer(user, host, password, my_database) as sql:
    sql.select("users")
```

You can close your connection manually with ```sql.close()``` method too.

### As a decorator

You may want to decorate your funciton with this module. To to that, simply use the ```provide_session``` module. It will try replace sql argument if it exists in your funcion, or add a kwarg argument called ```sql```. This decorator opens an close an SQL Server connection for you. Example:

```
from magical_sqlserver import provide_session

@provide_session(user, host, password, my_database)
def awesome_function(sql=None):
  sql.select("users")
```

Or with kwargs:
```
@provide_session(user, host, password, my_database)
def awesome_function(**kwargs):
  sql = kwargs["sql"]
  sql.select("users")
```

## Writing data

### Single records

To create new record:

```
data = {"id":3, "name":"bar"}

sql.insert("users", data)
```

To update existing records:
```
sql.update("users", data={"name":"new_name"}, contition={"id":1})
```

You can delete records too:
```
sql.delete("users", {"id":2, "name":"new_user"})
```

It's important to say that all conditions are additive. For example, if your update has condition ```{"id": 1, "name": "new_user"}```, this module will build an sql query that has ```id = 1 and name = 'new_user'``` and will try this against the database. In the table above, no data will be updated.

### Bulk Insert
Simply pass a list of dicts to ```bulk_insert``` method. It will transform it to csv temporary files and copy it to table with BCP method. See requirements for more details.

```
data = [{"id":4, "name":"me"},{"id":5, "name":"you"}]

sql.bulk_insert("users", data)
```

## Other Stuff
You can run generic queries with the method query:
```
data = sql.query("select top(10) * from my_table join users on my_table.id = users.id")
```

And retrieve all tables from Sql Server Schema:
```
tables_list = sql.tables()
```
