#!/usr//bin/env/python3
# -*- coding:utf-8 -*-
__author__ = 'Hiram Zhang'

import os
from datetime import datetime

import pymssql


def get_backup_name(db: str, bak_type: str) -> str:
    db = db.replace('.', '_')
    nowtime = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
    return '_'.join([bak_type, nowtime, db]) + '.bak'


class ErrorBackupType(Exception):
    pass


class BakMssql:
    """Connect to sqlserver and backup specified database in specified type"""

    def __init__(self, user: str = None, password: str = None, host: str = 'localhost', port: int = 1433,
                 database: str = 'master'):
        """Connect to sqlserver"""
        self.conn = pymssql.connect(host=host, port=port, user=user, password=password, database=database)
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    def __del__(self):
        """Make sure the sqlserver connection closed"""
        try:
            self.cursor.close()
            self.conn.close()
        except AttributeError:
            pass

    def run(self, database: str, bak_type: str, filepath: str) -> str:
        """
        Backup database
        :param database:the database to backup
        :param bak_type:backup type,should be Full,Diff,Log
        :param filepath:remote server file path to save backup
        :return: backup file path in the remote server
        """
        if bak_type not in ('Full', 'Diff', 'Log'):
            raise ErrorBackupType
        filename = get_backup_name(database, bak_type)
        full_path = os.path.join(filepath, filename)
        if bak_type == 'Full':
            sql = r'''BACKUP DATABASE [%s] TO  
                      DISK = N'%s' WITH NOFORMAT, NOINIT,  
                      NAME = N'%s', SKIP, NOREWIND, NOUNLOAD,  STATS = 10''' % (
                database, full_path, 'Full Database Backup ' + filename)
        elif bak_type == 'Diff':
            sql = r'''BACKUP DATABASE [%s] TO  
                        DISK = N'%s' WITH DIFFERENTIAL,NOFORMAT, NOINIT,  
                        NAME = N'%s', SKIP, NOREWIND, NOUNLOAD,  STATS = 10''' % (
                database, full_path, 'Diff Database Backup ' + filename)
        elif bak_type == 'Log':
            sql = r'''BACKUP LOG [%s] TO  
                        DISK = N'%s' WITH NOFORMAT, NOINIT,  
                        NAME = N'%s', SKIP, NOREWIND, NOUNLOAD,  STATS = 10''' % (
                database, full_path, 'Transaction Log  Backup ' + filename)
        try:
            self.cursor.execute(sql)
            return full_path
        except Exception as e:
            return None


if __name__ == '__main__':
    bak = BakMssql(user='username', password='password', host='localhost')
    bak_file = bak.run('dbname', 'bak_type', 'filepath')
    print(bak_file)
