set -e
set -x

rm -rf data/from_butler
butler create data/from_butler
butler register-instrument data/from_butler lsst.obs.lsst.LsstCam
butler register-dataset-type data/from_butler raw Exposure exposure instrument detector
sqlite3 data/from_butler/gen3.sqlite3 "INSERT INTO skymap VALUES ('lsst_cells_v1', '4nNuQSRN8Au9uTrsrArkstGd2pE=', 18938, 10, 10);"
butler transfer-datasets embargo data/from_butler -d raw --collections LSSTCam/raw/all -t copy --where "instrument='LSSTCam' AND day_obs=20250415 AND exposure.seq_num IN (52, 53, 54, 55) AND detector=94"
for i in 52 53 54 55; do 
  head -c 11520 data/from_butler/LSSTCam/raw/all/raw/20250415/MC_O_20250415_0000${i}/raw_LSSTCam_i_39_MC_O_20250415_0000${i}_R22_S11_LSSTCam_raw_all.fits > _header.fits
  mv _header.fits data/from_butler/LSSTCam/raw/all/raw/20250415/MC_O_20250415_0000${i}/raw_LSSTCam_i_39_MC_O_20250415_0000${i}_R22_S11_LSSTCam_raw_all.fits
  touch data/from_butler/LSSTCam/raw/all/raw/20250415/MC_O_20250415_0000${i}/raw_LSSTCam_i_39_MC_O_20250415_0000${i}_R22_S11_LSSTCam_raw_all.json
done
