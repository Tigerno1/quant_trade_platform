
# import sys, os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from settings.default_platform_conf import CONFIG, ConfigGenerator


def merge(default, override):
    config = {}
    for key, value in default.items():
        if key in override:
            if isinstance(value, dict):
                config[key] = merge(value, override[key])
            else:
                config[key] = override[key]
        else:
            config[key] = value

    return config

def toDict(conf):
    conf_dict = ConfigGenerator()
    for key, value in conf.items():
        conf_dict[key] = toDict(value) if isinstance(value, dict) else value
    return conf_dict


try:
    from settings import override_conf
    configs = merge(CONFIG, override_conf.CONFIG)
    CONFIG = toDict(configs)
except ImportError as e:
    print(e)




