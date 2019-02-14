import os
from datetime import datetime, timedelta


class EDGARAnalytics:

    def __init__(self, timeout_iput_file):
        self.timeout = None
        self.assert_file_exist(
            timeout_iput_file, 'Need a valid input file (inactivity_period.txt).')
        with open(timeout_iput_file) as fin:
            try:
                self.timeout = int(fin.readline().strip())
            except Exception as e:
                raise e
        self.start_time = {}
        self.records = {}

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
        return [record_datetime for record_datetime in self.records if record_datetime <= timeouted_datetime]

    def get_timeouted_records(self, timeouted_datetime, output_file):
        """
        Given a timeouted_datetime, write all timeouted records
        into output file by order
        delete records in self.records
        """
        records = self.records[timeouted_datetime]
        for ip in sorted(records, key=lambda ip: self.start_time[ip]):
            record = records[ip]
            start_time = start_time[ip]
            end_time = record['end_time']
            line = ''
            line += ip
            line += ','
            line += strftime(start_time, '%Y-%m-%d %H:%M:%S')
            line += ','
            line += strftime(end_time, '%Y-%m-%d %H:%M:%S')
            line += ','
            line += record['count']
            line += ','
            duration = end_time - start_time
            line += str(int(duration.dtdf.total_seconds()))
            line += '\n'
            yield line
        del self.records[timeouted_datetime]

    def write_timeouted_record(self, output_file, line):
        with open(output_file, 'a') as fout:
            fout.write(line)

    def get_record_datetime(record):
        if 'date' not in record or 'time' not in record:
            raise ValueError('No field "data" or "time" in record')
        return strptime(record['date'] + ' ' + record['time'], '%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    timeout_iput_file = '../input/inactivity_period.txt'
    input_file = '../input/log.csv'
    sepration = ','
    pattern = {
        'ip': 0,
        'date': 1,
        'time': 2,
        'cik': 4,
        'accession': 5,
        'extention': 6
    }
    edgar_analytics = EDGARAnalytics(timeout_iput_file)
    for line in edgar_analytics.read_lines(input_file, header=True):
        print(edgar_analytics.split_line(line, sepration, pattern))
