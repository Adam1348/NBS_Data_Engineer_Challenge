import io
import os
import unittest
from unittest.mock import patch
from my_challenge import download_file


class DownloadTest(unittest.TestCase):

    def test_pre_delta(self):
        self.assertEqual(tuple(download_file.pre_delta(1, 3, 1)), (1, 2, 3))
        self.assertEqual(tuple(download_file.pre_delta(1, 7, 2)), (1, 3, 5, 7))

    def test_url_generator(self):
        self.assertEqual(download_file.url_generator(['2012/1/1-0', '2012/1/1-0']), ['http://dumps.wikimedia.org/other/'
                                                                                     'pagecounts-raw/2012/2012-01/'
                                                                                     'pagecounts-20120101-000000.gz'])
        self.assertEqual(download_file.url_generator(['2012/1/1-0', '2012/1/1-1']), ['http://dumps.wikimedia.org/other/'
                                                                                     'pagecounts-raw/2012/2012-01/'
                                                                                     'pagecounts-20120101-000000.gz',
                                                                                     'http://dumps.wikimedia.org/other/'
                                                                                     'pagecounts-raw/2012/2012-01/'
                                                                                     'pagecounts-20120101-010000.gz',
                                                                                     ])

    def test_file_reader(self):
        test_file1 = io.StringIO('test')
        self.assertEqual(list(download_file.file_reader(test_file1)), ['test'])
        test_file2 = io.StringIO('test1\ntest2')
        self.assertEqual(list(download_file.file_reader(test_file2)), ['test1\n', 'test2'])

    def test_clean_hepler(self):
        self.assertEqual(download_file.clean_helper('aa.b MediaWiki:Exif-scenecapturetype-2 1 5379', '2012010100'),
                         ['aa', 'MediaWiki:Exif-scenecapturetype-2', '1', '5379', '2012/01/01-00'])
        self.assertIsNone(download_file.clean_helper('aa.d Special:AllPages 1 5336', '2012010100'))
        self.assertIsNone(download_file.clean_helper('aa.d Special:AllPages 1', '2012010100'))

    def test_download_helper(self):
        with patch('my_challenge.download_file.requests.get') as mocked_get:
            mocked_get.return_value.ok = True
            mocked_get.return_value.text = 'Success'
            response = download_file.download_helper('http://dumps.wikimedia.org/other/pagecounts-raw/'
                                                     '2012/2012-01/pagecounts-20120101-000000.gz')
            mocked_get.assert_called_with('http://dumps.wikimedia.org/other/pagecounts-raw/2012/2012-01/pagecounts-'
                                          '20120101-000000.gz')

            self.assertEqual(response.text, 'Success')

            mocked_get.return_value.ok = False
            response = download_file.download_helper('http://dumps.wikimedia.org/other/pagecounts-raw/'
                                                     '2012/201-01/pagecounts-20120101-000000.gz')
            mocked_get.assert_called_with('http://dumps.wikimedia.org/other/pagecounts-raw/2012/201-01/pagecounts-'
                                          '20120101-000000.gz')
            self.assertIsNone(response)


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + \
                                                   '\\Pandora-d2891344cd44.json'
    unittest.main()
