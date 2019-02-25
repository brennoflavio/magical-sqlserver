import pymssql
import subprocess
import csv
from tempfile import NamedTemporaryFile


class SQLServer:
    def __init__(self, user, password, host, database, port=1433):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self._connection = pymssql.connect(
            user=user,
            password=password,
            host=host,
            database=database,
            port=port,
            as_dict=True,
        )

    def _execute(self, sql):
        """
        Execute commited transaction against sql server with execute method
        """
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            self._connection.commit()
        except pymssql.ProgrammingError:
            self._connection.rollback()
            raise pymssql.ProgrammingError

    def _run_process(self, cmd):
        """
        Run externam system process. This will be used to bulk insert data with BCP
        """
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = proc.communicate()
        print(f"Output: {outs}")
        print(f"Stderr: {errs}")
        if proc.returncode != 0:
            raise Exception(f"Process failed with return code {proc.returncode}")

    def tables(self):
        """
        Return list of sql server tables
        """
        cursor = self._connection.cursor()
        sql = "SELECT concat(table_schema,'.',table_name) table_name FROM INFORMATION_SCHEMA.TABLES\
                WHERE table_type='BASE TABLE'"
        cursor.execute(sql)
        result = []
        for row in cursor.fetchall():
            result.append(row[0])
        return result

    def query(self, sql):
        """
        Execute uncommited transaction against sql server
        """
        cursor = self._connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    def close(self):
        """
        Closes connection
        """
        return self._connection.close()

    def select(self, table, columns="*", condition=None):
        """
        Select generic results from sql server table, returning them as dict
        """
        if condition is None:
            sql = (
                "select "
                + ",".join(columns)
                + f" from\
                {table} (nolock)"
            )
        else:
            where = []
            for key in condition:
                where.append(str(key) + "=" + "'" + str(condition[key]) + "'")
            sql = (
                "select "
                + ",".join(columns)
                + f" from {table} where "
                + " and ".join(where)
            )
        cursor = self._connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    def insert(self, table, data):
        """
        Insert single json data into table
        """
        columns = []
        values = []
        for key in data:
            columns.append(key)
            if data[key] is None:
                values.append("Null")
            else:
                values.append("'" + str(data[key]) + "'")
        sql = (
            f"insert into {table} ("
            + ",".join(columns)
            + ") values ("
            + ",".join(values)
            + ")"
        )
        self._execute(sql)

    def update(self, table, data, condition=None):
        """
        Update table data giving some column match condition
        """
        values = []
        where = []
        for key in data:
            if data[key] is None:
                values.append(str(key) + "= Null")
            else:
                values.append(str(key) + "=" + "'" + str(data[key]) + "'")
        if condition is None:
            sql = f"update {table} set " + ",".join(values)
        else:
            for key in condition:
                where.append(str(key) + "=" + "'" + str(condition[key]) + "'")
            sql = (
                f"update {table} set "
                + ",".join(values)
                + "where "
                + " and ".join(where)
            )
        self._execute(sql)

    def delete(self, table, condition=None):
        """
        Delete data giving some condition
        """
        where = []
        if condition is None:
            sql = f"delete from {table}"
        else:
            for key in condition:
                where.append(str(key) + "=" + "'" + str(condition[key]) + "'")
            sql = f"delete from {table} " + "where " + " and ".join(where)
        self._execute(sql)

    def _generate_format_file(self, table_name, format_file_path, delimiter="\t"):
        """
        Generate required file to be used with bcp
        """
        cmd = [
            "bcp",
            table_name,
            "format",
            "nul",
            "-c",
            "-f",
            format_file_path,
            f"-t{delimiter}",
            "-S",
            self.host,
            "-U",
            self.user,
            "-P",
            self.password,
            "-d",
            self.database,
        ]
        self._run_process(cmd)

    def bulk_insert(self, table, data):
        """
        Bulk insert data into table with BCP
        """
        with NamedTemporaryFile() as csv_data, NamedTemporaryFile() as format_file:
            fieldnames = []
            self._generate_format_file(table, format_file.name)
            format_file.flush()

            with open(format_file.name, "r") as opened_format_file:
                opened_format_file.readline()
                num_cols = int(opened_format_file.readline())
                for _ in range(num_cols):
                    tokens = opened_format_file.readline().split()
                    fieldnames.append(tokens[6])

            with open(csv_data.name, "w") as opened_csv_data:
                writer = csv.DictWriter(
                    opened_csv_data,
                    fieldnames=fieldnames,
                    delimiter="\t",
                    lineterminator="\n",
                )
                for row in data:
                    writer.writerow(row)
                csv_data.flush()

            cmd = [
                "bcp",
                table,
                "in",
                csv_data.name,
                "-f",
                format_file.name,
                "-m",
                "1",
                "-S",
                self.host,
                "-U",
                self.user,
                "-P",
                self.password,
                "-d",
                self.database,
            ]

            self._run_process(cmd)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return self.close()

    def select_distinct(self, table, columns="*", condition=None):
        """
        Select distinct generic results from sql server table, returning them as dict
        """
        if condition is None:
            sql = (
                "select distinct"
                + ",".join(columns)
                + f" from\
                {table} (nolock)"
            )
        else:
            where = []
            for key in condition:
                where.append(str(key) + "=" + "'" + str(condition[key]) + "'")
            sql = (
                "select "
                + ",".join(columns)
                + f" from {table} where "
                + " and ".join(where)
            )
        cursor = self._connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
