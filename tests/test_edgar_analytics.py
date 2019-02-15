import unittest
from datetime import datetime
from context import EDGARAnalytics

TIMEOUT_INPUT_FILE = '../input/inactivity_period.txt'
OUTPUT_FILE = '../output/sessionization.txt'


class TestEDGARAnalytics(unittest.TestCase):

    def test_timeout_iput_file(self):
        edgar_analytics = EDGARAnalytics(TIMEOUT_INPUT_FILE, OUTPUT_FILE)
        self.assertEqual(edgar_analytics.timeout, 2)

    def test_read_lines(self):
        edgar_analytics = EDGARAnalytics(TIMEOUT_INPUT_FILE, OUTPUT_FILE)

        first_line = '101.81.133.jja,2017-06-30,00:00:00,0.0,1608552.0,0001047469-17-004337,-index.htm,200.0,80251.0,1.0,0.0,0.0,9.0,0.0,\n'
        input_file = '../input/log.csv'
        read_lines_first_line = next(edgar_analytics.read_lines(input_file, header=True))
        self.assertEqual(read_lines_first_line, first_line)

        second_line = '107.23.85.jfd,2017-06-30,00:00:00,0.0,1027281.0,0000898430-02-001167,-index.htm,200.0,2825.0,1.0,0.0,0.0,10.0,0.0,\n'
        read_lines_second_line = next(edgar_analytics.read_lines(input_file, header=True))
        self.assertEqual(read_lines_second_line, first_line)

    def test_split_line(self):
        edgar_analytics = EDGARAnalytics(TIMEOUT_INPUT_FILE, OUTPUT_FILE)
        sample_line = '107.23.85.jfd,2017-06-30,00:00:00,0.0,1027281.0,0000898430-02-001167,-index.htm,200.0,2825.0,1.0,0.0,0.0,10.0,0.0,\n'
        pattern = {
            'ip': 0,
            'date': 1,
            'time': 2,
            'cik': 4,
            'accession': 5,
            'extention': 6
        }
        splited_line = edgar_analytics.split_line(sample_line, ',', pattern)
        expected_splited_line = {
            'ip': '107.23.85.jfd',
            'date': '2017-06-30',
            'time': '00:00:00',
            'cik': '1027281.0',
            'accession': '0000898430-02-001167',
            'extention': '-index.htm'
        }
        self.assertEqual(splited_line, expected_splited_line)

    def test_get_timeouted_datetimes(self):
        edgar_analytics = EDGARAnalytics(TIMEOUT_INPUT_FILE, OUTPUT_FILE)
        record_datetime = datetime(2019, 1, 1, 0, 0, 1)
        timeouted_datetimes = edgar_analytics.get_timeouted_datetimes(record_datetime)
        self.assertEqual(timeouted_datetimes, [])

        # TODO: insert record into self.records for further tests



if __name__ == '__main__':
    unittest.main()
