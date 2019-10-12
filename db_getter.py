import calendar

from helpers import logger
from sql_scripts import *
from helpers import TABLE_NAMES, db_name, DBConnector


class Reporter(DBConnector):
    def __init__(self, table_names=TABLE_NAMES):
        DBConnector.__init__(self)
        self.table_names = table_names

    def record_counter(self):
        for table in self.table_names:
            counter = self.curs.execute(sql_row_count.format(table_name=table)).fetchone()[0]
            logger.info(str(counter) + ' rows in ' + table)

    def patients(self, table='patient'):
        logger.info('* PATIENTS *')
        grouping = self.curs.execute(sql_group_by.format(table_name=table, col_name='gender')).fetchall()
        for row in grouping:
            logger.info("Gender: " + row[1] + '; Number of Patients: ' + str(row[0]))

    def procedures(self, table='procedure'):
        grouping = self.curs.execute(sql_group_by_limit.format(placeholder='encounters', table_name=table,
                                                               col_name='type_code', order_by='encounters',
                                                               direction='desc', num_lines=10)).fetchall()
        logger.info('* TOP TEN PROCEDURES *')
        for row in grouping:
            logger.info("Procedure type: " + row[1] + '; Number of Encounters: ' + str(row[0]))

    def encounters(self, table='encounter', col_name='start_date'):
        grouping = self.curs.execute(f"""select strftime('%w', start_date), count(*)
          from {table}
          group by strftime('%w', {col_name})""").fetchall()

        logger.info('* DAYS OF ENCOUNTERS *')
        grouping_sorted = sorted(grouping, reverse=True, key=lambda x: x[1])
        logger.info("The most popular Week day: " + calendar.day_name[int(grouping_sorted[0][0])-1]
                    + '; Number of Encounters: ' + str(grouping_sorted[0][1]))
        logger.info("The least popular Week day: " + calendar.day_name[int(grouping_sorted[-1][0])-1]
                    + '; Number of Encounters: ' + str(grouping_sorted[-1][1]))


if __name__ == '__main__':
    logger.info('********************')
    logger.info('* REPORT ON TABLES *')
    logger.info('********************')
    db_getter = Reporter()
    db_getter.connect()
    db_getter.patients()
    db_getter.procedures()
    db_getter.encounters()
    db_getter.disconnect()
