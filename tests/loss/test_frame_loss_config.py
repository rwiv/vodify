from pyutils import path_join, find_project_root

from vtask.service.loss import read_fl_config


def test_frame_loss_config():
    conf_path = path_join(find_project_root(), "dev", "fl_conf.yaml")
    conf = read_fl_config(conf_path)
    print()
    print(conf.key_frame)
    print(conf.all)
