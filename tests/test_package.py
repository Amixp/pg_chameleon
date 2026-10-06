"""Packaging contract; actual wheel contents are separately verified at release."""
import runpy
import unittest
from unittest import mock


class PackagingTest(unittest.TestCase):
    def test_downstream_version_feature_license_and_compat_dependency(self):
        with mock.patch('setuptools.setup') as setup:
            runpy.run_path('setup.py')
        options = setup.call_args.kwargs
        self.assertEqual(options['version'], '2.0.21+betark.1')
        self.assertIn('sql/betark_transactional.sql', options['package_data']['pg_chameleon'])
        self.assertIn('LICENSE.txt', options['license_files'])
        self.assertIn('setuptools<81', options['install_requires'])


if __name__ == '__main__':
    unittest.main()
