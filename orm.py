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

        if _json['db_type'] != 'Oracle':
            user = {}
            for r in row:
                user[r[0]] = str(r[1])

            if user[_json['user']] == 'True' or user[_json['user']] == 'Y':
                return 1, 'OK'
            else:
                return 2, 'User is not superuser'
        else:
            for r in row:
                if r[0] == 'GRANT ANY OBJECT PRIVILEGE':
                    return 1, 'OK'
            return 2, 'User is not authorized'
    except:
        return 2, 'User information cannot be retrieved.'


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
        if _json['db_type'] in ('PostgreSQL', 'Microsoft SQL Server'):
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
    _authority = {
        'PostgreSQL': [
            [
                'ALL PRIVILEGES',
                'DELETE',
                'INSERT',
                'REFERENCES',
                'SELECT',
                'TRIGGER',
                'TRUNCATE',
                'UPDATE'
            ],
            [
                'ALL PRIVILEGES',
                'CREATE',
                'USAGE'
            ]
        ],
        'MySQL': [
            'ALL PRIVILEGES',
            'ALTER',
            'CREATE',
            'CREATE VIEW',
            'DELETE',
            'DROP',
            'GRANT OPTION',
            'INDEX',
            'INSERT',
            'REFERENCES',
            'SELECT',
            'SHOW VIEW',
            'TRIGGER',
            'UPDATE',
            'USAGE'
        ],
        'Oracle': [
            'ALL',
            'ALTER',
            'DEBUG',
            'DELETE',
            'INDEX',
            'INSERT',
            'REFERENCES',
            'SELECT',
            'UPDATE'
        ],
        'Microsoft SQL Server': [
            [
                'ALTER',
                'CONTROL',
                'DELETE',
                'EXECUTE',
                'INSERT',
                'RECEIVE',
                'REFERENCES',
                'SELECT',
                'TAKE OWNERSHIP',
                'UPDATE',
                'VIEW CHANGE TRACKING',
                'VIEW DEFINITION'
            ],
            [
                'ALTER',
                'CONTROL',
                'CREATE SEQUENCE',
                'DELETE',
                'EXECUTE',
                'INSERT',
                'REFERENCES',
                'SELECT',
                'TAKE OWNERSHIP',
                'UPDATE',
                'VIEW CHANGE TRACKING',
                'VIEW DEFINITION'
            ]
        ]
    }

    if _json['db_type'] in ('PostgreSQL', 'Microsoft SQL Server'):
        if '.' in _json["table"]:
            return 1, ','.join(_authority[_json['db_type']][0])
        else:
            return 1, ','.join(_authority[_json['db_type']][1])
    else:
        return 1, ','.join(_authority[_json['db_type']])


class Postgresql:
    def __init__(self, _json):
        self._json = _json

    def _get_check_authority(self):
        return text(
            "select rolname, rolsuper from pg_roles"
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

    def _get_check_authority(self):
        return text(
            "select user, Super_priv from mysql.user"
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

    def _get_check_authority(self):
        return text('SELECT PRIVILEGE FROM USER_SYS_PRIVS')

    def _get_target_user_list(self):
        return text('SELECT USERNAME FROM ALL_USERS')

    def _get_table_list(self):
        return text('SELECT TABLE_NAME FROM DBA_TABLES')

    def add(self):
        return text(
            f'GRANT {self._json["authority"]} ON {self._json["user"]}.{self._json["table"]} TO {self._json["target_user"]}')

    def remove(self):
        return text(
            f'REVOKE {self._json["authority"]} ON {self._json["user"]}.{self._json["table"]} TO {self._json["target_user"]}')


class Mssql:
    def __init__(self, _json):
        self._json = _json

    def _get_target_user_list(self):
        return text('SELECT name FROM sys.database_principals')

    def _get_schema_list(self):
        return text('SELECT name FROM sys.schemas')

    def _get_table_list(self):
        return text('SELECT name FROM sys.all_objects')

    def add(self, authority):
        if '.' in self._json["table"]:
            return text(
                f'GRANT {authority} ON OBJECT \:\: {self._json["table"]} TO {self._json["target_user"]}')
        else:
            return text(
                f'GRANT {authority} ON SCHEMA \:\: {self._json["table"]} TO {self._json["target_user"]}')

    def remove(self, authority):
        if '.' in self._json["table"]:
            return text(
                f'REVOKE {authority} ON OBJECT \:\: {self._json["table"]} FROM {self._json["target_user"]}')
        else:
            return text(
                f'REVOKE {authority} ON SCHEMA \:\: {self._json["table"]} FROM {self._json["target_user"]}')


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
