import argparse
from config_parser import config_dict
parser = argparse.ArgumentParser()
parser.add_argument('--level', type=str, help='配置日志级别')
args = parser.parse_args()
log_level = args.level

__all__ = ['log_level', 'config_dict']
