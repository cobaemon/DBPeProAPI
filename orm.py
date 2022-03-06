from sqlalchemy import create_engine, text


def _setup_engine(_json):
    db_list = {
        'PostgreSQL': ['postgresql', 'psycopg2'],
        'MySQL': ['mysql', 'mysqldb'],
        'Oracle': ['oracle', 'cx_oracle'],
        'Microsoft SQL Server': ['mssql', 'pymssql'],
    }
    db = db_list[_json['db_type']]

    engine = create_engine(
        f'{db[0]}+{db[1]}://{_json["user"]}:{_json["password"]}@{_json["host"]}:{_json["port"]}/{_json["database"]}'
    )

    return engine


def _select_db(_json):
    match _json['db_type']:
        case 'PostgreSQL':
            return Postgresql(_json=_json)
        case 'MySQL':
            return Mysql(_json=_json)
        case 'Oracle':
            return Oracle(_json=_json)
        case 'Microsoft SQL Server':
            return Mssql(_json=_json)


def _connection_check(_json):
    try:
        _engine = _setup_engine(_json)
        with _engine.connect() as conn:
            pass

        return 1, 'Successfully connected to the database.'
    except:
        return 2, 'Failed to connect to the database.'


def _check_authority(_json):
    try:
        _engine = _setup_engine(_json)
        db = _select_db(_json)

        with _engine.connect() as conn:
            row = conn.execute(
                db._get_check_authority()
            )

        user = {}
        for r in row:
            user[r[0]] = str(r[1])

        if _json['user'] in user:
            if user[_json['user']] == 'True' or user[_json['user']] == 'Y':
                return 1, 'OK'
            else:
                return 2, 'User is not superuser'
        else:
            return 2, 'User not found'
    except:
        return 2, 'User information cannot be retrieved.'


def _database_list(_json):
    try:
        _engine = _setup_engine(_json)
        db = _select_db(_json)

        with _engine.connect() as conn:
            row = conn.execute(
                db._get_database_list()
            )

        return 1, ','.join([r[0] for r in row])
    except:
        return 2, 'Failed to retrieve database list.'


def _target_user_list(_json):
    try:
        _engine = _setup_engine(_json)
        db = _select_db(_json)

        with _engine.connect() as conn:
            row = conn.execute(
                db._get_target_user_list()
            )

        return 1, ','.join(list(dict.fromkeys([r[0] for r in row])))
    except:
        return 2, 'Failed to retrieve target user list.'


def _table_list(_json):
    try:
        _engine = _setup_engine(_json)
        db = _select_db(_json)

        schema = []
        if _json['db_type'] == 'PostgreSQL':
            with _engine.connect() as conn:
                row = conn.execute(
                    db._get_schema_list()
                )

            for r in row:
                schema.append(r[0])

        table = []
        with _engine.connect() as conn:
            row = conn.execute(
                db._get_table_list()
            )

        for r in row:
            schema.append(r[0])
            if len(r) == 2:
                table.append(f'{r[0]}.{r[1]}')

        schema = list(dict.fromkeys(schema))
        schema.extend(table)

        if len(schema) > 0:
            return 1, ','.join([sc for sc in schema])
        else:
            return 2, 'No table'
    except:
        return 2, 'Failed to retrieve table list.'


def _authority_list(_json):
    _authority = [
        'ALL PRIVILEGES',
        'CREATE',
        'DELETE',
        'EXECUTE',
        'INSERT',
        'REFERENCES',
        'SELECT',
        'TRIGGER',
        'UPDATE',
        'USAGE',
    ]
    return 1, ','.join(_authority)


class Postgresql:
    def __init__(self, _json):
        self._json = _json
        self._engine = _setup_engine(_json)

    def _get_check_authority(self):
        return text(
            "select rolname, rolsuper from pg_roles"
        )

    def _get_database_list(self):
        return text(
            "select datname from pg_database where datname not like 'pg_%'"
        )

    def _get_target_user_list(self):
        return text(
            "select rolname from pg_roles where rolname not like 'pg_%'"
        )

    def _get_schema_list(self):
        return text(
            "select nspname from pg_namespace where nspname not like 'pg_%' and nspname not like 'information_%'")

    def _get_table_list(self):
        return text(
            "select schemaname, tablename from pg_tables where schemaname not like 'pg_%' and tablename not like 'pg_%' and schemaname not like 'information_%'"
        )

    def add(self, authority):
        if '.' in self._json["table"]:
            return text(f'GRANT {authority} ON {self._json["table"]} TO {self._json["target_user"]}')
        else:
            return text(f'GRANT {authority} ON SCHEMA {self._json["table"]} TO {self._json["target_user"]}')

    def remove(self, authority):
        if '.' in self._json["table"]:
            return text(f'REVOKE {authority} ON {self._json["table"]} FROM {self._json["target_user"]}')
        else:
            return text(f'REVOKE {authority} ON SCHEMA {self._json["table"]} FROM {self._json["target_user"]}')


class Mysql:
    def __init__(self, _json):
        self._json = _json
        self._engine = _setup_engine(_json)

    def _get_check_authority(self):
        return text(
            "select user, Super_priv from mysql.user"
        )

    def _get_database_list(self):
        return text(
            'show databases'
        )

    def _get_target_user_list(self):
        return text(
            "select user from mysql.user where user not like 'mysql.%'"
        )

    def _get_table_list(self):
        return text(
            'show tables'
        )

    def add(self, authority):
        return text(
            f'GRANT {authority} ON {self._json["database"]}.{self._json["table"]} TO {self._json["target_user"]}@{self._json["host"]}')

    def remove(self, authority):
        return text(
            f'REVOKE {authority} ON {self._json["database"]}.{self._json["table"]} FROM {self._json["target_user"]}@{self._json["host"]}')


class Oracle:
    def __init__(self, _json):
        self._json = _json
        self._engine = _setup_engine(_json)

    def _get_database_list(self):
        pass

    def _get_target_user_list(self):
        pass

    def _get_table_list(self):
        pass

    def add(self):
        return text(
            f'GRANT {self._json["authority"]} ON {self._json["user"]}.{self._json["table"]} TO {self._json["target_user"]}')

    def remove(self):
        return text(
            f'REVOKE {self._json["authority"]} ON {self._json["user"]}.{self._json["table"]} TO {self._json["target_user"]}')


class Mssql:
    def __init__(self, _json):
        self._json = _json
        self._engine = _setup_engine(_json)

    def _get_database_list(self):
        pass

    def _get_target_user_list(self):
        pass

    def _get_table_list(self):
        pass

    def add(self, authority):
        return text(
            f'GRANT {authority} ON OBJECT\:\:{self._json["database"]}.{self._json["table"]} TO {self._json["target_user"]}')

    def remove(self, authority):
        return text(
            f'REVOKE {authority} ON OBJECT \:\: {self._json["database"]}.{self._json["table"]} TO {self._json["target_user"]}')


def _add_authority(_json):
    try:
        _engine = _setup_engine(_json)
        db = _select_db(_json)

        with _engine.connect() as conn:
            if _json['db_type'] != 'Oracle':
                for authority in _json['authority'].split(','):
                    try:
                        conn.execute(
                            db.add(authority),
                        )
                    except:
                        return 2, 'Failed to execute the query.'
            else:
                try:
                    conn.execute(
                        db.add(),
                    )
                except:
                    return 2, 'Failed to execute the query.'
            return 1, 'Successful query and commit.'
    except:
        return 2, 'Failed to connect to the database.'


def _remove_authority(_json):
    try:
        _engine = _setup_engine(_json)
        db = _select_db(_json)

        with _engine.connect() as conn:
            if _json['db_type'] != 'Oracle':
                for authority in _json['authority'].split(','):
                    try:
                        conn.execute(
                            db.remove(authority),
                        )
                    except:
                        return 2, 'Failed to execute the query.'
            else:
                try:
                    conn.execute(
                        db.remove(),
                    )
                except:
                    return 2, 'Failed to execute the query.'
            return 1, 'Successful query and commit.'
    except:
        return 2, 'Failed to connect to the database.'
