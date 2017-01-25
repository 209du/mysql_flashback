# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from datetime import datetime
import time
import codecs
import argparse
from fnmatch import fnmatch
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent


version = '1.0'


class Flashback():
    """
    根据Binlog生成回闪SQL
    """
    def __init__(self, host, port, user, password, server_id, outfile, reverse, start_time=None, end_time=None,
                 log_file=None, table_filter=None):
        self.mysql_settings = {'host': host, 'port': port, 'user': user, 'passwd': password}
        self.server_id = server_id
        self.outfile = outfile
        self.reverse = reverse
        self.start_timestamp = None if start_time is None else long(time.mktime(start_time.timetuple()))
        self.end_timestamp = None if end_time is None else long(time.mktime(end_time.timetuple()))
        self.log_file = log_file
        self.log_pos = 4 if log_file is not None else None
        self.table_filter = table_filter
        self.event_list = []
        self.binlogstream = None

    def start(self):
        self.binlogstream = BinLogStreamReader(
            connection_settings=self.mysql_settings,
            server_id=self.server_id,
            blocking=False,
            resume_stream=False,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            skip_to_timestamp=self.start_timestamp,
            log_file=self.log_file,
            log_pos=self.log_pos
        )

        for event in self.binlogstream:
            event.log_file = self.binlogstream.log_file
            event.log_pos = self.binlogstream.log_pos

            if self.end_timestamp and event.timestamp > self.end_timestamp:
                break

            if isinstance(event, QueryEvent) and event.query not in ('BEGIN', 'COMMIT'):
                self.event_list.append(event)
            elif type(event) in (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent) and self._match_filter(event):
                self.event_list.append(event)

        self.dump_sql()

    def _match_filter(self, event):
        """
        判断 行数据事件 是否满足 表过滤选项
        :param event:
        :return: True满足条件 False不满足
        """
        if self.table_filter is None:
            return True
        elif isinstance(self.table_filter, list):
            for filter in self.table_filter:
                if fnmatch('{0}.{1}'.format(event.schema, event.table), filter):
                    return True
            else:
                return False
        else:
            return fnmatch('{0}.{1}'.format(event.schema, event.table), self.table_filter)

    def dump_sql(self):
        with codecs.open(self.outfile, 'w', encoding='utf-8') as f:
            incr = -1 if self.reverse else 1

            for event in self.event_list[::incr]:
                f.write('-- ==## {0} {1}-{2} '.format(
                    datetime.fromtimestamp(event.timestamp), event.log_file, event.log_pos))

                event_map = {
                    QueryEvent: self._dump_query_event,
                    WriteRowsEvent: self._dump_write_rows_event,
                    UpdateRowsEvent: self._dump_update_rows_event,
                    DeleteRowsEvent: self._dump_delete_rows_event
                }

                dump_method = event_map.get(type(event), self._dump_not_implemented_event)
                dump_method(event, f, self.reverse)
                f.write('\n')

            f.write('\n-- ==## {0} completed OK!'.format(datetime.now()))

    def _dump_query_event(self, event, outfile, reverse=False):
        outfile.write('QueryEvent\n{0};\n\n'.format(event.query))

    def _dump_write_rows_event(self, event, outfile, reverse=False):
        outfile.write('WriteRowsEvent rows:{0}\n'.format(len(event.rows)))
        sql = self._generate_insert_sql(event) if reverse == False else self._generate_delete_sql(event)
        outfile.write(sql)

    def _dump_update_rows_event(self, event, outfile, reverse=False):
        outfile.write('UpdateRowsEvent rows:{0}\n'.format(len(event.rows)))
        outfile.write(self._generate_update_sql(event, reverse=reverse))

    def _dump_delete_rows_event(self, event, outfile, reverse=False):
        outfile.write('DeleteRowsEvent rows:{0}\n'.format(len(event.rows)))
        sql = self._generate_delete_sql(event) if reverse == False else self._generate_insert_sql(event)
        outfile.write(sql)

    def _dump_not_implemented_event(self, event, outfile, flashback=False):
        outfile.write('NotImplementedEvent\n')

    def _generate_insert_sql(self, event):
        columns = [column.name for column in event.columns]
        row_list = []
        sql = 'INSERT INTO `{0}`.`{1}`(`{2}`) VALUES\n'.format(event.schema, event.table, '`,`'.join(columns))
        for row in event.rows:
            row_value = [self._format_data(row['values'][c]) for c in columns]
            row_list.append('(' + ', '.join(row_value) + ')')
        return sql + '\n,'.join(row_list) + ';\n'

    def _generate_update_sql(self, event, reverse=False):
        columns = [column.name for column in event.columns]
        old_key = 'before_values' if reverse == False else 'after_values'
        new_key = 'after_values' if reverse == False else 'before_values'
        sql_list = []
        sql = 'UPDATE `{0}`.`{1}`\n'.format(event.schema, event.table)
        for row in event.rows:
            set_clause = self._generate_set_clause(columns, row, new_key)
            where_clause = self._generate_where_clause(columns, row, old_key)
            sql_list.append(sql + set_clause + where_clause)
        return ''.join(sql_list)

    def _generate_delete_sql(self, event):
        columns = [column.name for column in event.columns]
        sql_list = []
        sql = 'DELETE FROM `{0}`.`{1}`\n'.format(event.schema, event.table)
        for row in event.rows:
            where_clause = self._generate_where_clause(columns, row, 'values')
            sql_list.append(sql + where_clause)
        return ''.join(sql_list)

    def _format_data(self, value):
        t = type(value)
        if t in (int, long, float):
            return str(value)
        elif t == type(None):
            return 'Null'
        elif t == str: # MySQL中的binary之类类型
            if len(value) == 0:
                return "''"
            else:
                return '0x' + ''.join(format(ord(c), 'x') for c in value)
                # return '0x' + ''.join(hex(ord(c)).replace('0x','') for c in value)
        else:
            s = '{0}'.format(value).replace("'", "\\'")
            return "'{0}'".format(s)

    def _generate_where_clause(self, columns, row, key_name):
        clause = '\n  AND '.join('`{0}` {1} {2}'.format(
                        c,
                        'is' if type(row[key_name][c]) == type(None) else '=',
                        self._format_data(row[key_name][c])
                    )
                for c in columns)
        return 'WHERE ' + clause + '\nLIMIT 1;\n'

    def _generate_set_clause(self, columns, row, key_name):
        clause = '\n   ,'.join('`{0}` = {1}'.format(
                        c,
                        self._format_data(row[key_name][c])
                    )
                for c in columns)
        return 'SET ' + clause + '\n'


def parse_datetime(time_str):
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(version=version, description='Flashback for MySQL')

    parser.add_argument('-H', '--host', dest='host', action='store',
                        help='MySQL host ip or domain, default is localhost', default='localhost')
    parser.add_argument('-P', '--port', dest='port', action='store',
                        help='MySQL port, default is 3306', default=3306, type=int)
    parser.add_argument('-u', '--user', dest='user', action='store',
                        help='MySQL user', default='root')
    parser.add_argument('-p', '--password', dest='password', action='store',
                        help='MySQL password', default='')
    parser.add_argument('-i', '--server-id', dest='server_id', action='store',
                        help='Server id used to connect to the master server, default is 3', default=3, type=int)
    parser.add_argument('-s', '--start-time', dest='start_time', action='store',
                        help='Binlog start time like "2017-01-01 01:01:01"', default=None, type=parse_datetime)
    parser.add_argument('-e', '--end-time', dest='end_time', action='store',
                        help='Binlog end time like "2017-01-01 01:01:01"', default=None, type=parse_datetime)
    parser.add_argument('-f', '--log-file', dest='log_file', action='store',
                        help='Start from this log file', default=None)
    parser.add_argument('-o', '--outfile', dest='outfile', action='store',
                        help='Output file', required=True)
    parser.add_argument('-t', '--tables', dest='tables', action='append',
                        help='Only generate SQL for these tables, like test.*, this option can be specified multiple times', default=None)
    parser.add_argument('-r', '--reverse', dest='reverse', action='store_true',
                        help='Used to generate flashback SQL', default=False)

    args = parser.parse_args()
    print(args)

    flash = Flashback(host=args.host, port=args.port, user=args.user, password=args.password, server_id=args.server_id,
                      outfile=args.outfile, reverse=args.reverse, start_time=args.start_time, end_time=args.end_time,
                      log_file=args.log_file, table_filter=args.tables
                      )
    flash.start()


if __name__ == "__main__":
    main()
