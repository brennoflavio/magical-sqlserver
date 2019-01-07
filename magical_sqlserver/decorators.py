from magical_sqlserver.api import SQLServer
from functools import wraps


def provide_session(user, password, host, database, port=1433, *args, **kwargs):
    def decorator(func):
        @wraps(func)
        def wrapper():
            func_params = func.__code__.co_varnames
            with SQLServer(user, password, host, database, port) as sql:
                if "sql" in func_params:
                    return func(sql=sql, *args, **kwargs)
                else:
                    kwargs["sql"] = sql
                    return func(*args, **kwargs)
        return wrapper
    return decorator
