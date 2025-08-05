import shutil
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

from lsst.daf.butler import Butler, DatasetType
from lsst.pipe.base.script import register_instrument

TEST_DIR = Path(__file__).parent


class TestZip(unittest.TestCase):
    def setUp(self):
        """
        Performs the setup necessary to run
        all tests
        """
        self.source_butler = Butler(TEST_DIR / "data" / "from_butler")
        self.temp_dir = Path(tempfile.mkdtemp())
        Butler.makeRepo(self.temp_dir)
        self.dest_butler = Butler(self.temp_dir, writeable=True)
        register_instrument(self.temp_dir, ["lsst.obs.lsst.LsstCam"])
        self.dest_butler.registry.registerDatasetType(
            DatasetType(
                "raw",
                ["exposure", "instrument", "detector"],
                "Exposure",
                universe=self.dest_butler.dimensions,
            )
        )

    def tearDown(self):
        """
        Removes all test files created by tests
        """
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_zip_calib_both(self):
        result = subprocess.run(
            [
                "python",
                TEST_DIR.parent / "src" / "transfer_raw_zip.py",
                "--window",
                "30min",
                "--now",
                "2025-04-16T00:40",
                "--dest_uri_prefix",
                self.temp_dir / "raw",
                "--config_file",
                TEST_DIR.parent / "src" / "config_raw.yaml",
                TEST_DIR / "data" / "from_butler",
                self.temp_dir,
            ],
            capture_output=True,
        )
        assert b"Handling exposure: MC_O_20250415_000052" in result.stderr
        assert b"Handling exposure: MC_O_20250415_000053" in result.stderr
        zip_file = (
            self.temp_dir / "raw" / "LSSTCam" / "20250415" / "MC_O_20250415_000052.zip"
        )
        assert zip_file.exists()
        assert (
            zipfile.Path(zip_file)
            / "raw_LSSTCam_i_39_MC_O_20250415_000052_R22_S11_LSSTCam_raw_all.fits"
        ).exists()
        assert (
            zipfile.Path(zip_file)
            / "raw_LSSTCam_i_39_MC_O_20250415_000052_R22_S11_LSSTCam_raw_all.json"
        ).exists()

    def test_zip_calib_one(self):
        result = subprocess.run(
            [
                "python",
                TEST_DIR.parent / "src" / "transfer_raw_zip.py",
                "--window",
                "8min",
                "--now",
                "2025-04-16T00:40",
                "--dest_uri_prefix",
                self.temp_dir / "raw",
                "--config_file",
                TEST_DIR.parent / "src" / "config_raw.yaml",
                TEST_DIR / "data" / "from_butler",
                self.temp_dir,
            ],
            capture_output=True,
        )
        assert b"Handling exposure: MC_O_20250415_000052" not in result.stderr
        assert b"Handling exposure: MC_O_20250415_000053" in result.stderr
        assert not (
            self.temp_dir / "raw" / "LSSTCam" / "20250415" / "MC_O_20250415_000052.zip"
        ).exists()
        assert (
            self.temp_dir / "raw" / "LSSTCam" / "20250415" / "MC_O_20250415_000053.zip"
        ).exists()

    def test_zip_on_sky(self):
        result = subprocess.run(
            [
                "python",
                TEST_DIR.parent / "src" / "transfer_raw_zip.py",
                "--window",
                "10min",
                "--now",
                "2025-05-16T00:42",
                "--dest_uri_prefix",
                self.temp_dir / "raw",
                "--config_file",
                TEST_DIR.parent / "src" / "config_raw.yaml",
                TEST_DIR / "data" / "from_butler",
                self.temp_dir,
            ],
            capture_output=True,
        )
        print("stdout", result.stdout)
        print("stderr", result.stderr)
        assert b"Handling exposure: MC_O_20250415_000054" in result.stderr
        assert b"Handling exposure: MC_O_20250415_000055" in result.stderr


if __name__ == "__main__":
    unittest.main()
