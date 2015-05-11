from glob import glob
import pep8


class TestCodeFormat:
    def __init__(self):
        # set max line length to something more accommodating.
        pep8.MAX_LINE_LENGTH = 100
        self.pep8style = pep8.StyleGuide()

    def _test_conformance_in_files(self, filenames):
        assert len(filenames) != 0
        for filename in filenames:
            result = self.pep8style.check_files([filename])
            assert result.total_errors == 0, "Found code style errors (and warnings)."

    def test_1_main_pep8_conformance(self):
        """Test that all tests conforms to PEP8. (Bar E501)"""
        filenames = glob('../*.py')
        self._test_conformance_in_files(filenames)

    def test_2_st_worker_files_pep8_conformance(self):
        """Test that all tests conforms to PEP8. (Bar E501)"""
        filenames = glob('../st_worker_files/*.py')
        filenames.remove('../st_worker_files/st_worker_settings.tpl.py')  # template file: remove.
        self._test_conformance_in_files(filenames)

    def test_3_tests_pep8_conformance(self):
        """Test that all tests conforms to PEP8. (Bar E501)"""
        filenames = glob('pep8_tests/*.py')
        self._test_conformance_in_files(filenames)
