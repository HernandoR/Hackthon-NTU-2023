from logger import *
from data_loader.link_s3 import AWS_S3_Loader
import numpy as np
from utils import prepare_device


SEED = 123
np.random.seed(SEED)


def main(config):
    logger = config.get_logger('train')

    # setup data_loader instances
    NSD_BUCKET=AWS_S3_Loader('natural-scenes-dataset')

    
    




