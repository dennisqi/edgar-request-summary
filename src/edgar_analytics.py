import os
from datetime import datetime, timedelta


class EDGARAnalytics:

    def __init__(self, timeout_iput_file, output_file):
        self.timeout = None
        self.assert_file_exist(
            timeout_iput_file, 'Need a valid timeout input file (inactivity_period.txt).')
        with open(timeout_iput_file) as fin:
            try:
                self.timeout = int(fin.readline().strip())
            except Exception as e:
                raise e
        self.start_datetime = {}
        self.records = {}
        self.ip_latest_datetime = {}
        self.output_file = output_file

    def assert_file_exist(self, file, error_message):
        if not os.path.isfile(file):
            raise AssertionError(error_message)

    def read_lines(self, input_file, header):
        """
        Read each line of the input file (log.csv)
        """
        self.assert_file_exist(input_file, 'Need a valid input file (log.csv).')
        with open(input_file, 'r') as fin:
            for line in fin:
                if header:
                    header = False
                    continue
                yield line

    def split_line(self, line, sepration, pattern):
        """
        Given a line from read_lines, split the line
        and return a dict of wanted info (refill pattern with result)
        sepration:
            a split key, e.g. ','
        pattern:
            a dict of row_name and index, e.g. {'ip': 0}
        """
        elements = line.split(sepration)
        result = {}
        for key in pattern.keys():
            result[key] = elements[pattern[key]]
        return result

    def get_timeouted_datetimes(self, record_datetime):
        """
        Given the latest request, return a list of timeouted datetimes
        """
        timeouted_datetime = record_datetime - timedelta(seconds=self.timeout)
        return [record_datetime for record_datetime in self.records if record_datetime < timeouted_datetime]

    def get_timeouted_records(self, timeouted_datetime, sepration):
        """
        Given a timeouted_datetime, write all timeouted records
        into output file by order
        delete records in self.records
        """
        records = self.records[timeouted_datetime]
        for ip in sorted(records, key=lambda ip: self.start_datetime[ip]):
            record = records[ip]
            start_datetime = self.start_datetime[ip]
            end_datetime = timeouted_datetime
            line = ''
            line += ip
            line += ','
            line += datetime.strftime(start_datetime, '%Y-%m-%d %H:%M:%S')
            line += ','
            line += datetime.strftime(end_datetime, '%Y-%m-%d %H:%M:%S')
            line += ','
            duration = end_datetime - start_datetime
            line += str(int(duration.total_seconds()) + 1)
            line += ','
            line += str(record['count'])
            line += '\n'
            yield line
            del self.start_datetime[ip]
            del self.ip_latest_datetime[ip]
        del self.records[timeouted_datetime]

    def write_timeouted_record(self, output_file, line):
        with open(output_file, 'a') as fout:
            fout.write(line)

    def get_record_datetime(self, record):
        if 'date' not in record or 'time' not in record:
            raise ValueError('No field "data" or "time" in record')
        return datetime.strptime(record['date'] + ' ' + record['time'], '%Y-%m-%d %H:%M:%S')

    def add_record(self, record):
        ip = record['ip']
        record_datetime = self.get_record_datetime(record)
        if ip not in self.start_datetime:
            self.start_datetime[ip] = record_datetime
        if record_datetime not in self.records:
            self.records[record_datetime] = {}
        if ip not in self.records[record_datetime]:
            self.records[record_datetime][ip] = {'count': 1}

        last_record_datetime = None
        if ip in self.ip_latest_datetime:
            last_record_datetime = self.ip_latest_datetime[ip]

        if last_record_datetime:
            temp_count = self.records[last_record_datetime][ip]['count']
            self.records[record_datetime][ip]['count'] = temp_count + 1
            # self.records[record_datetime][ip]['end_datetime'] = record_datetime
            if record_datetime != last_record_datetime:
                del self.records[last_record_datetime][ip]
                if not self.records[last_record_datetime]:
                    del self.records[last_record_datetime]
            self.ip_latest_datetime[ip] = record_datetime

        else:
            self.ip_latest_datetime[ip] = record_datetime



if __name__ == '__main__':
    timeout_iput_file = '../input/inactivity_period.txt'
    input_file = '../input/log.csv'
    output_file = '../output/sessionization.txt'
    sepration = ','
    pattern = {
        'ip': 0,
        'date': 1,
        'time': 2,
        'cik': 4,
        'accession': 5,
        'extention': 6
    }
    edgar_analytics = EDGARAnalytics(timeout_iput_file, output_file)
    for line in edgar_analytics.read_lines(input_file, header=True):
        record = edgar_analytics.split_line(line, sepration, pattern)
        record_datetime = edgar_analytics.get_record_datetime(record)
        print(edgar_analytics.records)
        # for k, v in edgar_analytics.records.items():
        #     print(k, v)
        # for k, v in edgar_analytics.start_datetime.items():
        #     print(k, v)
        for timeouted_datetime in edgar_analytics.get_timeouted_datetimes(record_datetime):
            # print(record_datetime, timeouted_datetime)
            for line1 in edgar_analytics.get_timeouted_records(timeouted_datetime, sepration):
                edgar_analytics.write_timeouted_record(output_file, line1)
        edgar_analytics.add_record(record)

    lines = []
    for timeouted_datetime in edgar_analytics.get_timeouted_datetimes(datetime.utcnow()):
        # print(record_datetime, timeouted_datetime)
        lines += [line for line in edgar_analytics.get_timeouted_records(timeouted_datetime, sepration)]
    for line in sorted(lines, key=lambda line: line.split(sepration)[1]):
        edgar_analytics.write_timeouted_record(output_file, line)
    edgar_analytics.start_datetime = {}
    edgar_analytics.records = {}
    edgar_analytics.ip_latest_datetime = {}
