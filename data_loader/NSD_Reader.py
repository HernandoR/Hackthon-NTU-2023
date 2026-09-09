from data_loader.link_s3 import AWS_S3_Loader
import h5py
import sys
import os
import logging
import nibabel as nib


class nsd_DS:

    A73k_PATH = 'nsddata_stimuli/stimuli/nsd/nsd_stimuli.hdf5'

    def __init__(self, data_path, NSD_bucket: AWS_S3_Loader) -> None:
        self.data_path = data_path
        self.NSD_bucket = NSD_bucket
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)


        self.Img_73k_path = os.path.join(self.data_path, self.A73k_PATH)

    def get_fmri(self, subject_id=1, run_id=1, sess_id=1, time_step=0):
        """
        Get the fmri data for a given subject, run, session and time step
        
        """
        TS_PATH = f'nsddata_timeseries\ppdata\subj{subject_id:02d}\\func1pt8mm\\timeseries'
        if type(sess_id) == int:
            sess_id = f'session{sess_id:02d}'

        fmri_id = f'timeseries_{sess_id}_run{run_id:02d}.nii.gz'
        fmri_path = os.path.join(self.data_path, TS_PATH, fmri_id)
        # fmri_file = os.path.join(fmri_path,f'{fmri_id}.nii.gz')
        if not os.path.exists(fmri_path):
            logging.info("Downloading fmri data")
            self.NSD_bucket.download_file(
                os.path.join(TS_PATH, fmri_id), fmri_path)

        fmri = nib.load(fmri_path)
        return fmri.get_fdata()[:, :, :, time_step]

    def get_imgid(self, subject_id=1, run_id=1, sess_id=1, time_step=0):
        TS_PATH = f'nsddata_timeseries\ppdata\subj{subject_id:02d}\\func1pt8mm\\design'
        if type(sess_id) == int:
            sess_id = f'session{sess_id:02d}'

        fmri_id = f'design_{sess_id}_run{run_id:02d}.tsv'
        fmri_path = os.path.join(self.data_path, TS_PATH, fmri_id)
        # fmri_file = os.path.join(fmri_path,f'{fmri_id}.nii.gz')
        if not os.path.exists(fmri_path):
            logging.info("Downloading fmri data")
            self.NSD_bucket.download_dir(
                os.path.join(TS_PATH, fmri_id), fmri_path)

        # the id is the time_step line
        with open(fmri_path, 'r') as f:
            for i, line in enumerate(f):
                if i == time_step:
                    return int(line.strip())

    def get_73k(self,img_id = 1):
        if not os.path.exists(self.Img_73k_path):
            logging.info("Downloading 73k dataset")
            self.NSD_bucket.download_file(
                self.A73k_PATH, self.Img_73k_path)
            
        with h5py.File(self.Img_73k_path, 'r') as f:
            return f['imgBrick'][img_id-1]
