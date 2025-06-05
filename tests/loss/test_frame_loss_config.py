from pyutils import path_join, find_project_root

from vtask.service.loss import read_loss_config


def test_frame_loss_config():
    conf_path = path_join(find_project_root(), "dev", "configs", "loss_conf_test.yaml")
    conf = read_loss_config(conf_path)
    print()
    print(conf.size)
