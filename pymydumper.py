# -*-coding=utf-8-*-
import sys
import os
import pymysql


def command_line_args(args):
    if len(args) == 0:
        return False
    argsDict = dict()
    if '--help' in args:
        argsDict['help'] = 1
        return argsDict
    for arg in args:
        params = arg.split('=')
        if len(params) != 2:
            return False

        key = params[0]
        if not key.startswith('--'):
            return False
        key = key[2:]
        argsDict[key] = params[1]
    return argsDict


def help():
    print '''help desc:
        --host=(mysql host)
        --user=(mysql database user)
        --password=(mysql database user)
        --port=(value or default 3306)
        --database=(databases name)
        --back-path=(backpath)'''


def get_tables(args):
    db = args['database']
    conn = pymysql.connect(host=args['host'],
                           port=args.has_key('port') and args['port'] or 3306,
                           user=args['user'],
                           passwd=args['password'],
                           db=db
                           )
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute('SELECT table_name FROM information_schema.tables ' +
                   "WHERE table_schema='" + db + "' and table_type='base table'")
    rets = cursor.fetchall()
    conn.close()
    return rets


def get_lock_tables_sql(tables):
    sql = 'LOCK TABLES '
    for val in tables:
        sql += val['table_name'] + ' READ,'
    sql = sql[:-1] + ';'
    return sql


def valid_back_path(args):
    if not args.has_key('back-path'):
        print 'back-path is required'
        sys.exit(1)

    if not os.path.isdir(args['back-path']):
        print 'back-path is not valid path'
        sys.exit(1)


def lock_tables_dump_data(args):
    valid_back_path(args)
    db = args['database']
    db_tables = get_tables(args)
    lock_sql = get_lock_tables_sql(db_tables)

    conn = pymysql.connect(host=args['host'],
                           port=args.has_key('port') and args['port'] or 3306,
                           user=args['user'],
                           passwd=args['password'],
                           db=db
                           )
    cursor = conn.cursor()
    cursor.execute(lock_sql)

    command_dump_mysql_db(args)

    cursor.execute('UNLOCK TABLES;')
    cursor.close()


def command_dump_mysql_db(args):
    cmd = '/usr/local/bin/mydumper -t 8 -u %s -p %s -h %s -P %s --long-query-guard 300 --no-locks -B %s -c -o %s' % (
        args['user'],
        args['password'],
        args['host'],
        args.has_key('port') and args['port'] or 3306,
        args['database'],
        args['back-path']
    )
    output = os.popen(cmd)
    print output.read()


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    if not args:
        print ("params is error")
        sys.exit(1)

    if args.has_key('help'):
        help()
        sys.exit(0)

    lock_tables_dump_data(args)
